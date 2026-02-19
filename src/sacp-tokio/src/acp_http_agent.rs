//! ACP/HTTP agent transport for SACP.
//!
//! This module provides [`AcpHttpAgent`], a transport that connects to
//! agents serving ACP over HTTP (JSON-RPC POST + SSE streaming).
//!
//! Unlike [`A2AAgent`] which bridges A2A→ACP protocols, `AcpHttpAgent`
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
            let sse_handle = tokio::spawn(async move {
                if let Err(e) = listen_sse(&endpoint_sse, &tx_sse).await {
                    warn!("AcpHttpAgent SSE listener error: {e}");
                }
            });

            // Forward outgoing ACP messages as HTTP POST
            while let Some(msg_result) = rx.next().await {
                let msg = match msg_result {
                    Ok(msg) => msg,
                    Err(e) => {
                        warn!("AcpHttpAgent channel error: {e}");
                        continue;
                    }
                };

                // Serialize jsonrpcmsg::Message to JSON
                let json = serde_json::to_value(&msg)
                    .map_err(sacp::Error::into_internal_error)?;

                debug!(?json, "AcpHttpAgent → POST");

                let resp = http_client
                    .post(&endpoint)
                    .json(&json)
                    .send()
                    .await;

                match resp {
                    Ok(r) if !r.status().is_success() => {
                        warn!("AcpHttpAgent POST error: {}", r.status());
                    }
                    Err(e) => {
                        error!("AcpHttpAgent POST failed: {e}");
                    }
                    _ => {}
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
async fn listen_sse(
    endpoint: &str,
    tx: &futures::channel::mpsc::UnboundedSender<Result<sacp::jsonrpcmsg::Message, sacp::Error>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {


    let sse_url = format!("{}/stream", endpoint.trim_end_matches('/'));
    debug!(url = %sse_url, "AcpHttpAgent SSE connecting");

    let mut resp = reqwest::get(&sse_url).await?;
    if !resp.status().is_success() {
        return Err(format!("SSE connect failed: {}", resp.status()).into());
    }

    let mut buffer = String::new();

    // Use chunk-based reading for reqwest response body
    while let Some(chunk) = resp.chunk().await? {
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
                            tx.unbounded_send(Ok(msg))
                                .map_err(|e| format!("channel closed: {e}"))?;
                        }
                        Err(e) => {
                            debug!("AcpHttpAgent SSE parse error: {e} — {json_str}");
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
