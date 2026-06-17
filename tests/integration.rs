use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures::StreamExt;
use mcp_server::config::{Config, McpEntry, ServerSection};
use mcp_server::routes;
use mcp_server::state::AppState;

fn echo_command() -> String {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/echo_fixture/target/debug/echo_fixture");
    #[cfg(windows)]
    let p = p.with_extension("exe");
    p.to_string_lossy().into_owned()
}

#[tokio::test]
async fn sse_round_trip_with_echo_server() {
    // 1. Build an AppState with the echo fixture as "mcp-echo".
    let mut mcp = HashMap::new();
    mcp.insert("mcp-echo".to_string(), McpEntry {
        command: echo_command(),
        args: vec![],
        env: HashMap::new(),
        cwd: None,
        is_http: false,
    });
    let cfg = Config { server: ServerSection::default(), mcp };
    let state = AppState::new(cfg);

    // 2. Spawn the axum server on a random port.
    let app = routes::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // 3. Open the SSE stream and read the endpoint event.
    let client = reqwest::Client::new();
    let resp = client.get(format!("http://{addr}/mcp-echo/sse"))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let mut events = resp.bytes_stream().eventsource();

    let endpoint_evt = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await.expect("timeout waiting for endpoint event")
        .expect("stream ended").expect("event err");
    assert_eq!(endpoint_evt.event, "endpoint");
    let endpoint_path = endpoint_evt.data;
    assert!(endpoint_path.starts_with("/mcp-echo/messages?sessionId="), "got {endpoint_path}");

    // 4. POST a JSON-RPC request to the endpoint URL.
    let post_url = format!("http://{addr}{endpoint_path}");
    let post_resp = client.post(&post_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "ping",
            "params": { "hello": "integration" }
        }))
        .send().await.unwrap();
    assert_eq!(post_resp.status(), 202);

    // 5. Receive the message event from the SSE stream.
    let msg_evt = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await.expect("timeout waiting for message event")
        .expect("stream ended").expect("event err");
    assert_eq!(msg_evt.event, "message");
    let payload: serde_json::Value = serde_json::from_str(&msg_evt.data).unwrap();
    assert_eq!(payload["id"], serde_json::json!(99));
    assert_eq!(payload["result"]["echoed"]["hello"], serde_json::json!("integration"));
}
