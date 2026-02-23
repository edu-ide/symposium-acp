//! ACP/HTTP agent transport for SACP.
//!
//! This module provides [`AcpHttpAgent`], a transport that connects to
//! agents serving ACP over HTTP (JSON-RPC POST + SSE streaming).
//! speaks native ACP over HTTP — no protocol conversion needed.
//!
//! ```text
//! ┌──────────┐     ┌──────────────┐     ┌────────────────────┐
//! │ Conductor │────▶│ AcpHttpAgent │────▶│ Agent HTTP Server  │
//! │  (ACP)   │◀────│  (native)    │◀────│  /acp (POST+SSE)   │
//! └──────────┘ ACP └──────────────┘ ACP └────────────────────┘
//! ```

use futures::future::BoxFuture;
use sacp::{Channel, Client, ConnectTo};
use tracing::{debug, error, warn};

/// ACP/HTTP transport — connects to agents via HTTP POST + SSE.
///
/// Each message TO the agent is sent as HTTP POST to `{endpoint}`.
/// Messages FROM the agent arrive via SSE on `{endpoint}/stream`.
pub struct AcpHttpAgent {
    endpoint: String,
}

impl std::fmt::Debug for AcpHttpAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcpHttpAgent")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl AcpHttpAgent {
    /// Create a new ACP/HTTP agent targeting the given endpoint.
    ///
    /// Example: `AcpHttpAgent::new("http://localhost:41242/acp")`
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl ConnectTo<Client> for AcpHttpAgent {
    async fn connect_to(
        self,
        client: impl ConnectTo<<Client as sacp::Role>::Counterpart>,
    ) -> Result<(), sacp::Error> {
        let (channel, serve_self) = ConnectTo::<Client>::into_channel_and_future(self);
        match futures::future::select(Box::pin(client.connect_to(channel)), serve_self).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right((result, _)) => result,
        }
    }

    fn into_channel_and_future(self) -> (Channel, BoxFuture<'static, Result<(), sacp::Error>>) {
        use futures::StreamExt;

        let (channel_for_caller, channel_for_bridge) = Channel::duplex();
        let endpoint = self.endpoint;

        let server_future = Box::pin(async move {
            let Channel { mut rx, tx } = channel_for_bridge;
            let http_client = reqwest::Client::new();

            debug!(endpoint = %endpoint, "AcpHttpAgent bridge started");

            // Start SSE listener in background
            let tx_sse = tx.clone();
            let endpoint_sse = endpoint.clone();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let sse_handle = tokio::spawn(async move {
                if let Err(e) = listen_sse(&endpoint_sse, &tx_sse, Some(ready_tx)).await {
                    warn!("AcpHttpAgent SSE listener error: {e}");
                }
            });

            // Wait for SSE connection to be fully established before starting POSTs
            // This prevents POSTing `initialize` before the server can broadcast the result back over SSE.
            if let Err(e) = ready_rx.await {
                warn!("AcpHttpAgent SSE ready wait error (channel dropped): {e}");
            }

            // Forward outgoing ACP messages as HTTP POST
            while let Some(msg_result) = rx.next().await {
                let msg = match msg_result {
                    Ok(msg) => msg,
                    Err(e) => {
                        warn!("AcpHttpAgent channel error: {e}");
                        continue;
                    }
                };

                let json = serde_json::to_value(&msg)
                    .map_err(sacp::Error::into_internal_error)?;

                debug!(?json, "AcpHttpAgent → POST");

                let mut retries = 0;
                let max_retries = 10;
                let mut backoff_ms = 500;

                loop {
                    let resp = http_client
                        .post(&endpoint)
                        .json(&json)
                        .send()
                        .await;

                    match resp {
                        Ok(r) if !r.status().is_success() => {
                            warn!("AcpHttpAgent POST error: {}", r.status());
                            break;
                        }
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            if retries >= max_retries {
                                error!("AcpHttpAgent POST failed after {} retries: {e}", max_retries);
                                break;
                            }
                            warn!("AcpHttpAgent POST connect failed, retrying in {}ms: {e}", backoff_ms);
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            retries += 1;
                            backoff_ms = std::cmp::min(backoff_ms * 2, 5000_u64);
                        }
                    }
                }
            }

            sse_handle.abort();
            debug!("AcpHttpAgent bridge shut down");
            Ok(())
        });

        (channel_for_caller, server_future)
    }
}

/// Listen to SSE stream from the ACP/HTTP server and forward parsed
/// JSON-RPC messages back into the channel.
///
/// Retries indefinitely on connection failure with exponential backoff,
/// so the proxy can start even when the agent HTTP server isn't ready yet.
async fn listen_sse(
    endpoint: &str,
    tx: &futures::channel::mpsc::UnboundedSender<Result<sacp::jsonrpcmsg::Message, sacp::Error>>,
    mut ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sse_url = format!("{}/stream", endpoint.trim_end_matches('/'));
    let mut backoff_ms: u64 = 100;
    const MAX_BACKOFF_MS: u64 = 500;

    loop {
        debug!(url = %sse_url, "AcpHttpAgent SSE connecting");

        let resp = match reqwest::get(&sse_url).await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                warn!(
                    "AcpHttpAgent SSE connect failed ({}), retrying in {}ms",
                    r.status(),
                    backoff_ms
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms + 100).min(MAX_BACKOFF_MS);
                continue;
            }
            Err(e) => {
                warn!(
                    "AcpHttpAgent SSE connect error ({}), retrying in {}ms",
                    e, backoff_ms
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms + 100).min(MAX_BACKOFF_MS);
                continue;
            }
        };

        // Connected — reset backoff
        backoff_ms = 100;
        debug!(url = %sse_url, "AcpHttpAgent SSE connected");
        if let Some(rtx) = ready_tx.take() {
            let _ = rtx.send(());
        }

        let mut buffer = String::new();
        let mut resp = resp;

        // Use chunk-based reading for reqwest response body
        loop {
            match resp.chunk().await {
                Ok(Some(chunk)) => {
                    buffer.push_str(&String::from_utf8_lossy(&chunk));

                    // Parse SSE events: "data: {...}\n\n"
                    while let Some(end) = buffer.find("\n\n") {
                        let event_block = buffer[..end].to_string();
                        buffer = buffer[end + 2..].to_string();

                        for line in event_block.lines() {
                            if let Some(json_str) = line.strip_prefix("data: ") {
                                let json_str = json_str.trim();
                                if json_str.is_empty() {
                                    continue;
                                }
                                match serde_json::from_str::<sacp::jsonrpcmsg::Message>(json_str) {
                                    Ok(msg) => {
                                        debug!("AcpHttpAgent ← SSE message");
                                        if tx.unbounded_send(Ok(msg)).is_err() {
                                            // Channel closed — exit
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        debug!("AcpHttpAgent SSE parse error: {e} — {json_str}");
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    // Stream ended — reconnect
                    warn!("AcpHttpAgent SSE stream ended, reconnecting in {}ms", backoff_ms);
                    break;
                }
                Err(e) => {
                    warn!("AcpHttpAgent SSE read error: {e}, reconnecting in {}ms", backoff_ms);
                    break;
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms + 100).min(MAX_BACKOFF_MS);
    }
}
