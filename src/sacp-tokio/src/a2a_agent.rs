//! A2A agent transport for SACP.
//!
//! This module provides [`A2AAgent`], a transport adapter that connects to
//! A2A (Agent-to-Agent) protocol endpoints over HTTP, allowing them to
//! participate in SACP conductor chains alongside stdio-based [`AcpAgent`]s.
//!
//! [`AcpAgent`]: crate::AcpAgent

use a2a_rs::client::{A2AClient, StreamEvent};
use a2a_rs::types::{Message, Part, Role, SendMessageRequest, SendMessageResponse, Task};
use futures::future::BoxFuture;
use sacp::{Channel, Client, ConnectTo};
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::{debug, error, warn};

/// A transport adapter that bridges ACP (Agent Client Protocol) messages
/// to an A2A (Agent-to-Agent) endpoint over HTTP.
///
/// `A2AAgent` implements [`ConnectTo<Client>`], making it usable as a
/// drop-in replacement for [`AcpAgent`] in conductor chains. While `AcpAgent`
/// communicates with agents via stdio (spawning a child process), `A2AAgent`
/// communicates over HTTP using the A2A protocol.
///
/// # Architecture
///
/// ```text
/// ┌──────────┐     ┌──────────────┐     ┌─────────────────┐
/// │ Conductor │────▶│  A2AAgent    │────▶│  Remote A2A     │
/// │  (ACP)   │◀────│ (bridge)     │◀────│  Server (HTTP)  │
/// └──────────┘ ACP └──────────────┘ A2A └─────────────────┘
/// ```
///
/// The bridge translates:
/// - ACP JSON-RPC requests → A2A `SendMessage` HTTP requests
/// - A2A SSE stream events → ACP JSON-RPC notifications
///
/// [`AcpAgent`]: crate::AcpAgent
pub struct A2AAgent {
    /// The A2A endpoint URL
    endpoint: String,
}

impl std::fmt::Debug for A2AAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("A2AAgent")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl A2AAgent {
    /// Create a new A2A agent transport targeting the given endpoint URL.
    ///
    /// The endpoint should be the full URL of the A2A server's JSON-RPC endpoint
    /// (e.g., `http://localhost:41241`).
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    /// Returns the endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl ConnectTo<Client> for A2AAgent {
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
            let mut context_by_session: HashMap<String, String> = HashMap::new();
            let mut in_flight: HashMap<String, (serde_json::Value, tokio::task::JoinHandle<()>)> =
                HashMap::new();
            let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

            debug!(endpoint = %endpoint, "A2AAgent bridge started");

            loop {
                tokio::select! {
                        maybe_done_sid = done_rx.recv() => {
                            if let Some(done_sid) = maybe_done_sid {
                                in_flight.remove(&done_sid);
                            }
                        }
                        maybe_msg = rx.next() => {
                            let Some(msg_result) = maybe_msg else {
                                break;
                            };
                    let msg = match msg_result {
                        Ok(msg) => msg,
                        Err(e) => {
                            warn!("A2A bridge received error from channel: {e}");
                            continue;
                        }
                    };

                    // Parse the incoming JSON-RPC message
                    let raw_json =
                        serde_json::to_value(&msg).map_err(sacp::Error::into_internal_error)?;

                    debug!(?raw_json, "A2A bridge received ACP message");

                    // Extract method and check if it's a request (has 'id')
                    let method = raw_json.get("method").and_then(|m| m.as_str());
                    let msg_id = raw_json.get("id").cloned();
                    let params = raw_json
                        .get("params")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);

                match method {
                    Some("initialize") => {
                            debug!("A2A bridge handling initialize request");
                            if let Some(id) = msg_id {
                                let response = serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "result": {
                                        "protocolVersion": "0.1",
                                        "capabilities": {
                                            "prompts": {},
                                            "resources": {}
                                        },
                                        "serverInfo": {
                                            "name": "a2a-bridge",
                                            "version": "0.1.0"
                                        }
                                    }
                                });
                            send_json_response(&tx, response)?;
                        }
                    }
                    Some("session/new") => {
                        if let Some(id) = msg_id {
                            let response = serde_json::json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "sessionId": uuid::Uuid::new_v4().to_string()
                                }
                            });
                            send_json_response(&tx, response)?;
                        }
                    }
                    Some("session/load") => {
                        if let Some(id) = msg_id {
                            let session_id = extract_session_id(&params)
                                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                            let response = serde_json::json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "sessionId": session_id
                                }
                            });
                            send_json_response(&tx, response)?;
                        }
                    }
                    Some("session/cancel") => {
                            if let Some(session_id) = extract_session_id(&params) {
                                if let Some((request_id, handle)) = in_flight.remove(&session_id) {
                                    handle.abort();
                                    let _ = send_json_response(
                                        &tx,
                                        serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "id": request_id,
                                            "result": {
                                                "stopReason": "cancelled"
                                            }
                                        }),
                                    );
                                }
                            }
                        }

                        Some(method_name)
                            if method_name.contains("prompt")
                                || method_name.contains("message") =>
                        {
                            // Extract text from params to forward to A2A
                            let text = extract_prompt_text(&params);
                            if text.is_empty() {
                                warn!("A2A bridge: empty prompt text, skipping");
                                if let Some(id) = msg_id {
                                    send_json_response(
                                        &tx,
                                        serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "id": id,
                                            "error": { "code": -32000, "message": "Empty prompt text" }
                                        }),
                                    )?;
                                }
                                continue;
                            }

                            let Some(request_id) = msg_id else {
                                continue;
                            };

                            let session_id = extract_session_id(&params)
                                .unwrap_or_else(|| "__default_session__".to_string());

                            if let Some((previous_request_id, previous_handle)) =
                                in_flight.remove(&session_id)
                            {
                                previous_handle.abort();
                                let _ = send_json_response(
                                    &tx,
                                    serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "id": previous_request_id,
                                        "result": {
                                            "stopReason": "cancelled"
                                        }
                                    }),
                                );
                            }

                            debug!(text = %text, "A2A bridge forwarding prompt to A2A agent");

                            let ctx = context_by_session
                                .get(&session_id)
                                .cloned()
                                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                            context_by_session.insert(session_id.clone(), ctx.clone());

                            let endpoint_clone = endpoint.clone();
                            let tx_clone = tx.clone();
                            let done_tx_clone = done_tx.clone();
                            let session_id_clone = session_id.clone();
                            let request_id_clone = request_id.clone();
                            let text_clone = text.clone();

                            let handle = tokio::spawn(async move {
                                let client = A2AClient::new(&endpoint_clone);
                                let request = SendMessageRequest {
                                    message: Message {
                                        message_id: uuid::Uuid::new_v4().to_string(),
                                        context_id: Some(ctx),
                                        task_id: None,
                                        role: Role::User,
                                        parts: vec![Part::text(&text_clone)],
                                        metadata: None,
                                        extensions: vec![],
                                        reference_task_ids: None,
                                    },
                                    configuration: None,
                                    metadata: None,
                                };

                                // Prefer streaming, but always preserve structured payload for downstream consumers.
                                let response = collect_a2a_response(&client, request).await;
                                let response_text = response.text;
                                let response_structured = response.structured;

                                let _ = send_json_response(
                                    &tx_clone,
                                    serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "id": request_id_clone,
                                        "result": {
                                            "stopReason": "end_turn",
                                            "_meta": {
                                                "a2a_text": response_text,
                                                "a2a_structured": response_structured
                                            }
                                        }
                                    }),
                                );
                                let _ = done_tx_clone.send(session_id_clone);
                            });

                            in_flight.insert(session_id, (request_id, handle));
                        }

                        Some(method_name) => {
                            debug!(method = %method_name, "A2A bridge: unhandled method, sending empty response");
                            if let Some(id) = msg_id {
                                send_json_response(
                                    &tx,
                                    serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {}
                                    }),
                                )?;
                            }
                        }

                        None => {
                            debug!(?raw_json, "A2A bridge: ignoring notification");
                        }
                    }
                }
                    }
            }

            for (_, (_, handle)) in in_flight {
                handle.abort();
            }

            debug!("A2A bridge channel closed, shutting down");
            Ok(())
        });

        (channel_for_caller, server_future)
    }
}

/// Extract prompt/message text from ACP JSON-RPC params.
fn extract_prompt_text(params: &serde_json::Value) -> String {
    // ACP prompt format: { prompt: [{ type: "text", text: "..." }, ...] }
    if let Some(prompt_blocks) = params.get("prompt").and_then(|p| p.as_array()) {
        let mut texts = Vec::new();
        for block in prompt_blocks {
            let block_type = block.get("type").and_then(|v| v.as_str());
            if block_type == Some("text") {
                if let Some(text) = block.get("text").and_then(|v| v.as_str()) {
                    // Widget context is internal metadata and should not be forwarded as user text.
                    if !text.starts_with("__MCP_WIDGET_CTX__:") {
                        texts.push(text.to_string());
                    }
                }
            }
        }
        if !texts.is_empty() {
            return texts.join("\n");
        }
    }

    // Try ACP prompt format: { messages: [{ content: { type: "text", text: "..." } }] }
    if let Some(messages) = params.get("messages").and_then(|m| m.as_array()) {
        let mut texts = Vec::new();
        for msg in messages {
            if let Some(content) = msg.get("content") {
                if let Some(text) = content.get("text").and_then(|t| t.as_str()) {
                    texts.push(text.to_string());
                } else if let Some(parts) = content.as_array() {
                    for part in parts {
                        if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                            texts.push(text.to_string());
                        }
                    }
                }
            }
            if let Some(text) = msg.get("text").and_then(|t| t.as_str()) {
                texts.push(text.to_string());
            }
        }
        if !texts.is_empty() {
            return texts.join("\n");
        }
    }

    // Try simple text / prompt / content fields
    for key in &["text", "prompt", "content"] {
        if let Some(text) = params.get(*key).and_then(|t| t.as_str()) {
            return text.to_string();
        }
    }

    // Fallback: serialize params as text
    if !params.is_null() {
        return serde_json::to_string(params).unwrap_or_default();
    }

    String::new()
}

fn extract_session_id(params: &serde_json::Value) -> Option<String> {
    params
        .get("sessionId")
        .or_else(|| params.get("session_id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Extract text parts from `Part` list.
fn extract_text_from_parts(parts: &[Part]) -> String {
    parts
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join("")
}

fn normalize_text(text: &str) -> String {
    text.replace("\r\n", "\n").trim().to_string()
}

fn clean_a2a_output(text: String) -> String {
    let mut out = normalize_text(&text);
    if out.is_empty() {
        return out;
    }

    let lower = out.to_ascii_lowercase();
    if lower.starts_with("starting team mission:") {
        if let Some(idx) = out.find('[') {
            out = out[idx..].trim().to_string();
        }
    }

    if let Some(idx) = out.to_ascii_lowercase().rfind("mission completed:") {
        if idx > 0 {
            out = out[..idx].trim().to_string();
        }
    }

    out
}

fn should_retry_with_blocking_send(stream_text: &str) -> bool {
    let t = stream_text.trim();
    if t.is_empty() {
        return true;
    }
    let lower = t.to_ascii_lowercase();
    lower.starts_with("error:")
        || lower.contains("error decoding response body")
        || lower.contains("invalid content-type")
}

fn is_noise_status_text(text: &str) -> bool {
    let lower = text.trim().to_ascii_lowercase();
    lower.starts_with("starting team mission:")
        || lower.starts_with("mission completed:")
        || lower.starts_with("phase:")
        || lower.starts_with("progress:")
}

fn collect_agent_history_text(task: &Task) -> String {
    let mut lines: Vec<String> = Vec::new();
    for msg in &task.history {
        if msg.role != Role::Agent {
            continue;
        }
        let text = normalize_text(&extract_text_from_parts(&msg.parts));
        if text.is_empty() || is_noise_status_text(&text) {
            continue;
        }
        lines.push(text);
    }
    lines.join("\n\n")
}

fn collect_artifact_text(task: &Task) -> String {
    task.artifacts
        .iter()
        .flat_map(|a| a.parts.iter())
        .filter_map(|p| p.text.as_deref())
        .map(normalize_text)
        .filter(|t| !t.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn extract_task_text(task: &Task) -> String {
    let history_text = collect_agent_history_text(task);
    if !history_text.is_empty() {
        return clean_a2a_output(history_text);
    }

    let artifact_text = collect_artifact_text(task);
    if !artifact_text.is_empty() {
        return clean_a2a_output(artifact_text);
    }

    if let Some(msg) = &task.status.message {
        return clean_a2a_output(extract_text_from_parts(&msg.parts));
    }

    String::new()
}

#[derive(Debug, Clone)]
struct CollectedA2AResponse {
    text: String,
    structured: Value,
}

#[derive(Debug, Clone)]
struct StreamCollection {
    text: String,
    events: Vec<Value>,
    role_sections: Vec<Value>,
    terminal: Option<Value>,
}

fn normalize_team_role_label(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "leader" => Some("Leader"),
        "researcher" => Some("Researcher"),
        "verifier" => Some("Verifier"),
        "creator" => Some("Creator"),
        _ => None,
    }
}

fn extract_role_from_metadata_value(meta: &Value) -> Option<String> {
    let obj = meta.as_object()?;
    for key in [
        "team_role",
        "role",
        "agent_role",
        "source_role",
        "name",
        "agent",
    ] {
        let Some(raw) = obj.get(key).and_then(|v| v.as_str()) else {
            continue;
        };
        if let Some(norm) = normalize_team_role_label(raw) {
            return Some(norm.to_string());
        }
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn extract_role_from_metadata(meta: Option<&Value>) -> Option<String> {
    meta.and_then(extract_role_from_metadata_value)
}

fn push_role_section(sections: &mut Vec<Value>, role: &str, content: &str) {
    let role = role.trim();
    let content = content.trim();
    if role.is_empty() || content.is_empty() {
        return;
    }
    if sections.iter().any(|entry| {
        entry.get("role").and_then(|v| v.as_str()) == Some(role)
            && entry.get("content").and_then(|v| v.as_str()) == Some(content)
    }) {
        return;
    }
    sections.push(json!({
        "role": role,
        "content": content
    }));
}

fn collect_role_sections_from_value(value: &Value, sections: &mut Vec<Value>, depth: usize) {
    if depth > 8 {
        return;
    }

    match value {
        Value::Array(items) => {
            for item in items {
                collect_role_sections_from_value(item, sections, depth + 1);
            }
        }
        Value::Object(map) => {
            let role = map
                .get("team_role")
                .or_else(|| map.get("role"))
                .or_else(|| map.get("agent_role"))
                .and_then(|v| v.as_str())
                .and_then(|v| {
                    normalize_team_role_label(v)
                        .map(|s| s.to_string())
                        .or_else(|| {
                            let t = v.trim();
                            if t.is_empty() {
                                None
                            } else {
                                Some(t.to_string())
                            }
                        })
                });

            let content = map
                .get("content")
                .or_else(|| map.get("text"))
                .or_else(|| map.get("body"))
                .or_else(|| map.get("message"))
                .and_then(|v| v.as_str())
                .map(normalize_text);

            if let (Some(role), Some(content)) = (role, content) {
                push_role_section(sections, &role, &content);
            }

            for nested in map.values() {
                collect_role_sections_from_value(nested, sections, depth + 1);
            }
        }
        _ => {}
    }
}

fn collect_role_sections_from_message(message: &Message, sections: &mut Vec<Value>) {
    if let Some(role) = extract_role_from_metadata(message.metadata.as_ref()) {
        let text = normalize_text(&extract_text_from_parts(&message.parts));
        if !text.is_empty() {
            push_role_section(sections, &role, &text);
        }
    }

    if let Some(meta) = &message.metadata {
        collect_role_sections_from_value(meta, sections, 0);
    }
    for part in &message.parts {
        if let Some(data) = &part.data {
            collect_role_sections_from_value(data, sections, 0);
        }
        if let Some(meta) = &part.metadata {
            collect_role_sections_from_value(meta, sections, 0);
        }
    }
}

fn extract_role_sections_from_task(task: &Task) -> Vec<Value> {
    let mut sections: Vec<Value> = Vec::new();
    if let Some(meta) = &task.metadata {
        collect_role_sections_from_value(meta, &mut sections, 0);
    }

    for message in &task.history {
        if message.role == Role::Agent {
            collect_role_sections_from_message(message, &mut sections);
        }
    }

    for artifact in &task.artifacts {
        let artifact_text = normalize_text(
            &artifact
                .parts
                .iter()
                .filter_map(|p| p.text.as_deref())
                .collect::<Vec<_>>()
                .join("\n"),
        );
        if !artifact_text.is_empty() {
            if let Some(role) = extract_role_from_metadata(artifact.metadata.as_ref()) {
                push_role_section(&mut sections, &role, &artifact_text);
            }
        }
        if let Some(meta) = &artifact.metadata {
            collect_role_sections_from_value(meta, &mut sections, 0);
        }
        for part in &artifact.parts {
            if let Some(data) = &part.data {
                collect_role_sections_from_value(data, &mut sections, 0);
            }
            if let Some(meta) = &part.metadata {
                collect_role_sections_from_value(meta, &mut sections, 0);
            }
        }
    }

    sections
}

fn push_event_record(events: &mut Vec<Value>, source_role: &str, message: &str, event_type: &str) {
    let source_role = source_role.trim();
    let message = message.trim();
    let event_type = event_type.trim();
    if source_role.is_empty() || message.is_empty() || event_type.is_empty() {
        return;
    }
    if events.iter().any(|entry| {
        entry.get("source_role").and_then(|v| v.as_str()) == Some(source_role)
            && entry.get("message").and_then(|v| v.as_str()) == Some(message)
            && entry.get("event_type").and_then(|v| v.as_str()) == Some(event_type)
    }) {
        return;
    }
    events.push(json!({
        "event_type": event_type,
        "source_role": source_role,
        "message": message
    }));
}

fn collect_events_from_value(
    value: &Value,
    events: &mut Vec<Value>,
    default_event_type: &str,
    depth: usize,
) {
    if depth > 8 {
        return;
    }

    match value {
        Value::Array(items) => {
            for item in items {
                collect_events_from_value(item, events, default_event_type, depth + 1);
            }
        }
        Value::Object(map) => {
            let source_role = map
                .get("source_role")
                .or_else(|| map.get("role"))
                .or_else(|| map.get("agent_role"))
                .or_else(|| map.get("agent"))
                .or_else(|| map.get("from"))
                .and_then(|v| v.as_str())
                .map(|v| {
                    normalize_team_role_label(v)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| v.trim().to_string())
                });
            let message = map
                .get("message")
                .or_else(|| map.get("content"))
                .or_else(|| map.get("text"))
                .or_else(|| map.get("body"))
                .and_then(|v| v.as_str())
                .map(normalize_text);
            let event_type = map
                .get("event_type")
                .or_else(|| map.get("type"))
                .and_then(|v| v.as_str())
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| default_event_type.to_string());

            if let (Some(source_role), Some(message)) = (source_role, message) {
                push_event_record(events, &source_role, &message, &event_type);
            }

            for nested in map.values() {
                collect_events_from_value(nested, events, default_event_type, depth + 1);
            }
        }
        _ => {}
    }
}

fn collect_events_from_message(
    message: &Message,
    events: &mut Vec<Value>,
    default_event_type: &str,
) {
    if let Some(meta) = &message.metadata {
        collect_events_from_value(meta, events, default_event_type, 0);
    }
    for part in &message.parts {
        if let Some(data) = &part.data {
            collect_events_from_value(data, events, default_event_type, 0);
        }
        if let Some(meta) = &part.metadata {
            collect_events_from_value(meta, events, default_event_type, 0);
        }
    }
}

fn extract_events_from_task(task: &Task) -> Vec<Value> {
    let mut events = Vec::new();
    if let Some(meta) = &task.metadata {
        collect_events_from_value(meta, &mut events, "task", 0);
    }
    for message in &task.history {
        if message.role == Role::Agent {
            collect_events_from_message(message, &mut events, "message");
        }
    }
    for artifact in &task.artifacts {
        if let Some(meta) = &artifact.metadata {
            collect_events_from_value(meta, &mut events, "artifactUpdate", 0);
        }
        for part in &artifact.parts {
            if let Some(data) = &part.data {
                collect_events_from_value(data, &mut events, "artifactUpdate", 0);
            }
            if let Some(meta) = &part.metadata {
                collect_events_from_value(meta, &mut events, "artifactUpdate", 0);
            }
        }
    }
    events
}

/// Extract text from a `SendMessageResponse`.
fn extract_send_message_text(resp: &SendMessageResponse) -> String {
    match resp {
        SendMessageResponse::Task(task) => extract_task_text(task),
        SendMessageResponse::Message(msg) => clean_a2a_output(extract_text_from_parts(&msg.parts)),
    }
}

fn collect_response_from_send(resp: &SendMessageResponse) -> CollectedA2AResponse {
    let text = extract_send_message_text(resp);
    let mut role_sections = Vec::new();
    let mut events = Vec::new();

    match resp {
        SendMessageResponse::Task(task) => {
            role_sections.extend(extract_role_sections_from_task(task));
            events.extend(extract_events_from_task(task));
            if let Some(msg) = &task.status.message {
                let status_text = normalize_text(&extract_text_from_parts(&msg.parts));
                if !status_text.is_empty() {
                    let src = extract_role_from_metadata(msg.metadata.as_ref())
                        .unwrap_or_else(|| "Agent".to_string());
                    push_event_record(&mut events, &src, &status_text, "statusUpdate");
                }
                collect_events_from_message(msg, &mut events, "statusUpdate");
            }
        }
        SendMessageResponse::Message(msg) => {
            collect_role_sections_from_message(msg, &mut role_sections);
            collect_events_from_message(msg, &mut events, "message");
            let msg_text = normalize_text(&extract_text_from_parts(&msg.parts));
            if !msg_text.is_empty() {
                let src = extract_role_from_metadata(msg.metadata.as_ref())
                    .unwrap_or_else(|| "Agent".to_string());
                push_event_record(&mut events, &src, &msg_text, "message");
            }
        }
    }

    CollectedA2AResponse {
        text,
        structured: json!({
            "kind": "sendMessageResponse",
            "payload": serde_json::to_value(resp).unwrap_or(Value::Null),
            "role_sections": role_sections,
            "events": events
        }),
    }
}

/// Collect response text + structured event payload from an A2A SSE stream.
async fn collect_stream_response(
    rx: &mut tokio::sync::mpsc::Receiver<Result<StreamEvent, a2a_rs::client::ClientError>>,
) -> StreamCollection {
    let mut parts = Vec::new();
    let mut non_noise_parts = Vec::new();
    let mut terminal_text: Option<String> = None;
    let mut events: Vec<Value> = Vec::new();
    let mut role_sections: Vec<Value> = Vec::new();
    let mut terminal: Option<Value> = None;

    while let Some(event) = rx.recv().await {
        match event {
            Ok(StreamEvent::StatusUpdate(su)) => {
                if let Some(msg) = &su.status.message {
                    let text = normalize_text(&extract_text_from_parts(&msg.parts));
                    if !text.is_empty() {
                        let keep = text.clone();
                        if !is_noise_status_text(&text) {
                            non_noise_parts.push(keep.clone());
                        }
                        parts.push(keep.clone());
                        let src = extract_role_from_metadata(su.metadata.as_ref())
                            .or_else(|| extract_role_from_metadata(msg.metadata.as_ref()))
                            .unwrap_or_else(|| "Agent".to_string());
                        push_event_record(&mut events, &src, &keep, "statusUpdate");
                    }
                    collect_role_sections_from_message(msg, &mut role_sections);
                    collect_events_from_message(msg, &mut events, "statusUpdate");
                }
                if let Some(meta) = &su.metadata {
                    collect_events_from_value(meta, &mut events, "statusUpdate", 0);
                }
            }
            Ok(StreamEvent::Task(task)) => {
                let text = extract_task_text(&task);
                if !text.is_empty() {
                    terminal_text = Some(text);
                }
                terminal = Some(serde_json::to_value(&task).unwrap_or(Value::Null));
                role_sections.extend(extract_role_sections_from_task(&task));
                break; // Task is terminal
            }
            Ok(StreamEvent::Message(msg)) => {
                let text = normalize_text(&extract_text_from_parts(&msg.parts));
                if !text.is_empty() {
                    let keep = text.clone();
                    if !is_noise_status_text(&text) {
                        non_noise_parts.push(keep.clone());
                    }
                    parts.push(keep.clone());
                    terminal_text = Some(text);
                    let src = extract_role_from_metadata(msg.metadata.as_ref())
                        .unwrap_or_else(|| "Agent".to_string());
                    push_event_record(&mut events, &src, &keep, "message");
                }
                collect_role_sections_from_message(&msg, &mut role_sections);
                collect_events_from_message(&msg, &mut events, "message");
                terminal = Some(serde_json::to_value(&msg).unwrap_or(Value::Null));
                break; // Message is terminal
            }
            Ok(StreamEvent::ArtifactUpdate(artifact)) => {
                let text = normalize_text(
                    &artifact
                        .artifact
                        .parts
                        .iter()
                        .filter_map(|p| p.text.as_deref())
                        .collect::<Vec<_>>()
                        .join("\n"),
                );
                if !text.is_empty() {
                    non_noise_parts.push(text.clone());
                    parts.push(text.clone());
                    let src = extract_role_from_metadata(artifact.metadata.as_ref())
                        .or_else(|| extract_role_from_metadata(artifact.artifact.metadata.as_ref()))
                        .unwrap_or_else(|| "Agent".to_string());
                    push_event_record(&mut events, &src, &text, "artifactUpdate");
                }
                if let Some(meta) = &artifact.artifact.metadata {
                    collect_role_sections_from_value(meta, &mut role_sections, 0);
                    collect_events_from_value(meta, &mut events, "artifactUpdate", 0);
                }
                for part in &artifact.artifact.parts {
                    if let Some(data) = &part.data {
                        collect_role_sections_from_value(data, &mut role_sections, 0);
                        collect_events_from_value(data, &mut events, "artifactUpdate", 0);
                    }
                    if let Some(meta) = &part.metadata {
                        collect_role_sections_from_value(meta, &mut role_sections, 0);
                        collect_events_from_value(meta, &mut events, "artifactUpdate", 0);
                    }
                }
                if let Some(meta) = &artifact.metadata {
                    collect_events_from_value(meta, &mut events, "artifactUpdate", 0);
                }
            }
            Err(e) => {
                error!("A2A stream error: {e}");
                if parts.is_empty() {
                    return StreamCollection {
                        text: format!("Error: {e}"),
                        events,
                        role_sections,
                        terminal,
                    };
                }
                break;
            }
        }
    }

    let text = if let Some(text) = terminal_text {
        clean_a2a_output(text)
    } else if !non_noise_parts.is_empty() {
        clean_a2a_output(non_noise_parts.join("\n\n"))
    } else {
        clean_a2a_output(parts.join("\n"))
    };

    StreamCollection {
        text,
        events,
        role_sections,
        terminal,
    }
}

async fn collect_a2a_response(
    client: &A2AClient,
    request: SendMessageRequest,
) -> CollectedA2AResponse {
    match client.send_message_stream(request.clone()).await {
        Ok(mut stream_rx) => {
            let stream = collect_stream_response(&mut stream_rx).await;
            if should_retry_with_blocking_send(&stream.text) {
                debug!(
                    "A2A streaming produced non-usable output ({}), trying blocking send",
                    stream.text
                );
                match client.send_message(request).await {
                    Ok(resp) => {
                        let mut collected = collect_response_from_send(&resp);
                        if let Some(obj) = collected.structured.as_object_mut() {
                            obj.insert("fallback_from_stream".to_string(), json!(true));
                            obj.insert(
                                "stream_attempt".to_string(),
                                json!({
                                    "text": stream.text,
                                    "events": stream.events,
                                    "role_sections": stream.role_sections,
                                    "terminal": stream.terminal,
                                }),
                            );
                        }
                        collected
                    }
                    Err(send_err) => {
                        error!("A2A send failed after stream fallback: {send_err}");
                        CollectedA2AResponse {
                            text: stream.text,
                            structured: json!({
                                "kind": "stream",
                                "events": stream.events,
                                "role_sections": stream.role_sections,
                                "terminal": stream.terminal,
                                "fallback_error": send_err.to_string()
                            }),
                        }
                    }
                }
            } else {
                CollectedA2AResponse {
                    text: stream.text,
                    structured: json!({
                        "kind": "stream",
                        "events": stream.events,
                        "role_sections": stream.role_sections,
                        "terminal": stream.terminal
                    }),
                }
            }
        }
        Err(stream_err) => {
            debug!("A2A streaming failed ({stream_err}), trying blocking send");
            match client.send_message(request).await {
                Ok(resp) => {
                    let mut collected = collect_response_from_send(&resp);
                    if let Some(obj) = collected.structured.as_object_mut() {
                        obj.insert("stream_error".to_string(), json!(stream_err.to_string()));
                    }
                    collected
                }
                Err(send_err) => {
                    error!("A2A send failed: stream={stream_err}, send={send_err}");
                    CollectedA2AResponse {
                        text: format!("Error: stream={stream_err}, send={send_err}"),
                        structured: json!({
                            "kind": "error",
                            "stream_error": stream_err.to_string(),
                            "send_error": send_err.to_string()
                        }),
                    }
                }
            }
        }
    }
}

/// Send a JSON value as a JSON-RPC message through the channel.
fn send_json_response(
    tx: &futures::channel::mpsc::UnboundedSender<Result<sacp::jsonrpcmsg::Message, sacp::Error>>,
    value: serde_json::Value,
) -> Result<(), sacp::Error> {
    let rpc_msg: sacp::jsonrpcmsg::Message =
        serde_json::from_value(value).map_err(sacp::Error::into_internal_error)?;
    tx.unbounded_send(Ok(rpc_msg))
        .map_err(sacp::util::internal_error)
}
