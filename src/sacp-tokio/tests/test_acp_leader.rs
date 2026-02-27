// Headless integration test for ACP leader connection
// Run with: RUST_LOG=debug cargo test --test test_acp_leader -- --nocapture

use std::str::FromStr;
use sacp_tokio::AcpAgent;

const GEMINI_CLI_ROOT: &str =
    "/mnt/nvme0n1p2/workspace/monorepo/services/ilhae-agent/gemini-cli";
const LEADER_WORKSPACE: &str = "/home/tripleyoung/.ilhae/team-workspaces/leader";

/// Build the same leader command as context_proxy.rs
fn leader_cmd() -> String {
    format!(
        "bash -c 'cd {} && GEMINI_CLI_HOME={} GEMINI_YOLO_MODE=true GEMINI_FOLDER_TRUST=true USE_CCPA=1 CODER_AGENT_NAME=leader node packages/cli/dist/index.js --experimental-acp'",
        GEMINI_CLI_ROOT,
        LEADER_WORKSPACE,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leader_acp_handshake_timeout() {
    // Test: Can we even connect + initialize within 30s?
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let cmd = leader_cmd();
    eprintln!("Testing ACP leader: {}", cmd);

    let agent = AcpAgent::from_str(&cmd).expect("Failed to parse agent command");
    let cwd = std::path::Path::new(GEMINI_CLI_ROOT);

    eprintln!("Calling AcpAgentSession::connect_with_timeout (30s)...");
    let start = std::time::Instant::now();

    let result = sacp_tokio::AcpAgentSession::connect_with_timeout(
        agent,
        cwd,
        std::time::Duration::from_secs(30),
    )
    .await;

    let elapsed = start.elapsed();
    eprintln!("Elapsed: {:.1}s", elapsed.as_secs_f64());

    match &result {
        Ok(session) => {
            eprintln!("✅ SUCCESS! Session ID: {}", session.session_id());
        }
        Err(e) => {
            eprintln!("❌ FAILED: {:?}", e);
        }
    }

    assert!(result.is_ok(), "ACP handshake failed: {:?}", result.err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leader_acp_send_prompt() {
    // Test: Full round-trip — connect + send a simple prompt
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();

    let cmd = leader_cmd();
    let agent = AcpAgent::from_str(&cmd).expect("Failed to parse agent command");
    let cwd = std::path::Path::new(GEMINI_CLI_ROOT);

    eprintln!("Connecting to leader agent...");

    let mut session = sacp_tokio::AcpAgentSession::connect_with_timeout(
        agent,
        cwd,
        std::time::Duration::from_secs(30),
    )
    .await
    .expect("Connection failed");

    eprintln!("✅ Connected! Session: {}", session.session_id());
    eprintln!("Sending prompt: 'Say hello in one word.'");

    session.send_prompt("Say hello in one word.").expect("send_prompt failed");

    // Read response with a 60s timeout
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        session.read_to_string(),
    )
    .await;

    match response {
        Ok(Ok(text)) => {
            eprintln!("✅ Got response: {:?}", text);
            assert!(!text.is_empty(), "Response was empty");
        }
        Ok(Err(e)) => {
            panic!("read_to_string failed: {:?}", e);
        }
        Err(_) => {
            panic!("Timed out waiting for response after 60s");
        }
    }
}
