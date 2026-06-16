# mcp-server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local HTTP gateway that spawns multiple stdio-based MCP servers as child processes and exposes each one over the HTTP+SSE transport at `http://localhost:9000/<id>/sse`, multiplexing many SSE sessions onto a single shared child per server.

**Architecture:** A single Rust binary (`mcp-server`) reads a TOML config that lists named stdio MCP servers (e.g. `mcp-brave`, `mcp-filesystem`). For each entry, it lazily spawns the configured `command` on first connection, wires the child's `stdin`/`stdout` to a JSON-RPC message bus, and routes traffic to/from SSE clients identified by `sessionId`. JSON-RPC IDs are rewritten transparently so multiple HTTP clients can share one underlying child. Built on `tokio` + `axum`; no external MCP SDK is required — we treat the wire payloads as opaque JSON and only inspect/rewrite the `id` field.

**Tech Stack:** Rust 2021, `tokio` (async runtime + child processes), `axum` 0.8 (HTTP + SSE), `tower-http` (tracing middleware), `serde` / `serde_json` (JSON), `toml` (config), `tracing` + `tracing-subscriber` (logging), `uuid` (session IDs), `thiserror` (errors), `clap` (CLI flags). Test deps: `reqwest`, `eventsource-stream`, `tempfile`, `assert_cmd`.

---

## Design Decisions

- **Project layout:** Standalone Cargo crate at `mcp-server/`. Not a workspace member of `callmodel` for now — keeps it independently buildable. Workspace integration can be a later refactor.
- **Transport:** Legacy SSE transport (two-endpoint model). Client `GET /<id>/sse` opens an event stream that begins with an `endpoint` event whose `data` field is the path to `POST` JSON-RPC messages, including a freshly-minted `sessionId` query parameter. Streamable-HTTP is out of scope for v1.
- **Process model:** One child process per configured `id`, shared across all sessions for that id. Lazy spawn on first SSE connection. Automatic respawn if the child exits while sessions are still attached.
- **ID multiplexing:** Each session keeps its own `id` namespace. The proxy assigns a globally-unique `u64` outbound id per request, remembers `(session, original_id)`, and rewrites the response id before forwarding via SSE. JSON-RPC notifications (no `id`) and server-initiated requests are broadcast to every attached session of that child.
- **Identifier format:** Any URL-safe slug — `^[a-zA-Z0-9][a-zA-Z0-9_-]*$`. The slug is both the TOML table key and the URL segment.
- **Config reload:** Not in v1. Restart the binary to pick up changes.
- **Auth:** None in v1. Server binds to `127.0.0.1` by default.

## File Structure

```
mcp-server/
├── PLAN.md                  (this file)
├── AGENTS.md                (already written alongside this plan)
├── README.md                (created in Task 12)
├── Cargo.toml
├── config.example.toml
├── src/
│   ├── main.rs              CLI entrypoint + tokio runtime
│   ├── lib.rs               re-exports for integration tests
│   ├── error.rs             ProxyError enum, Result alias
│   ├── config.rs            TOML config types + loader + slug validation
│   ├── jsonrpc.rs           Minimal JSON-RPC envelope, id-rewriter
│   ├── child.rs             Stdio child wrapper: spawn, line-reader, writer
│   ├── session.rs           SseSession + SessionRegistry (per child)
│   ├── proxy.rs             ChildProxy: owns child + registry + multiplexer
│   ├── state.rs             AppState: HashMap<slug, Arc<ChildProxy>> + lazy spawn
│   ├── routes.rs            Axum router: GET /:id/sse, POST /:id/messages, GET /, GET /health
│   └── shutdown.rs          Ctrl+C handler that kills all live children
└── tests/
    ├── echo_fixture/        Small Rust binary that echoes JSON-RPC for tests
    │   ├── Cargo.toml
    │   └── src/main.rs
    └── integration.rs       End-to-end SSE round-trip test
```

Each source file has one responsibility. The `proxy.rs` module is the integration point: it owns a `child::Child`, a `session::SessionRegistry`, and a `jsonrpc::IdMux`, and it exposes two methods — `send_from_session(session_id, payload)` and `attach(session_id) -> Receiver`.

---

## Task 1: Cargo scaffold

**Files:**
- Create: `mcp-server/Cargo.toml`
- Create: `mcp-server/src/main.rs`
- Create: `mcp-server/src/lib.rs`
- Create: `mcp-server/.gitignore`

- [ ] **Step 1: Create the Cargo manifest**

`mcp-server/Cargo.toml`:

```toml
[package]
name = "mcp-server"
version = "0.1.0"
edition = "2021"
description = "Local HTTP+SSE gateway that proxies multiple stdio MCP servers"
license = "MIT OR Apache-2.0"

[dependencies]
tokio = { version = "1", features = ["full"] }
axum = { version = "0.8", features = ["macros"] }
tower-http = { version = "0.6", features = ["trace", "cors"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
uuid = { version = "1", features = ["v4"] }
thiserror = "1"
anyhow = "1"
clap = { version = "4", features = ["derive"] }
futures = "0.3"
bytes = "1"
tokio-util = { version = "0.7", features = ["codec"] }
async-stream = "0.3"

[dev-dependencies]
reqwest = { version = "0.12", features = ["json", "stream"] }
eventsource-stream = "0.2"
tempfile = "3"
tokio = { version = "1", features = ["test-util"] }

[[bin]]
name = "mcp-server"
path = "src/main.rs"
```

- [ ] **Step 2: Create the stub main + lib**

`mcp-server/src/lib.rs`:

```rust
pub mod error;
```

`mcp-server/src/main.rs`:

```rust
fn main() {
    println!("mcp-server scaffold");
}
```

`mcp-server/src/error.rs`:

```rust
// populated in Task 2
```

`mcp-server/.gitignore`:

```
/target
```

- [ ] **Step 3: Verify scaffold builds**

Run: `cd mcp-server && cargo check`
Expected: clean build, no warnings except the empty `error.rs`.

- [ ] **Step 4: Verify it runs**

Run: `cargo run --bin mcp-server`
Expected output: `mcp-server scaffold`

- [ ] **Step 5: Commit**

```bash
git add mcp-server/Cargo.toml mcp-server/Cargo.lock mcp-server/src mcp-server/.gitignore
git commit -m "mcp-server: scaffold Cargo crate"
```

---

## Task 2: Error type

**Files:**
- Modify: `mcp-server/src/error.rs`
- Modify: `mcp-server/src/lib.rs`

- [ ] **Step 1: Write a failing unit test**

Append to `mcp-server/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_server_carries_id() {
        let e = ProxyError::UnknownServer("mcp-brave".into());
        assert!(e.to_string().contains("mcp-brave"));
    }

    #[test]
    fn io_error_converts() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "x");
        let e: ProxyError = io.into();
        assert!(matches!(e, ProxyError::Io(_)));
    }
}
```

- [ ] **Step 2: Run the test, expect failure**

Run: `cd mcp-server && cargo test error::`
Expected: compile error — `ProxyError` is undefined.

- [ ] **Step 3: Implement `ProxyError`**

Replace `mcp-server/src/error.rs` with:

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("unknown server: {0}")]
    UnknownServer(String),

    #[error("invalid server id: {0}")]
    InvalidServerId(String),

    #[error("child process for {id} exited unexpectedly: {reason}")]
    ChildExited { id: String, reason: String },

    #[error("session {0} not found")]
    UnknownSession(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),
}

pub type Result<T> = std::result::Result<T, ProxyError>;
```

The test block from Step 1 stays at the bottom of the file.

- [ ] **Step 4: Run the test, expect pass**

Run: `cargo test error::`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add mcp-server/src/error.rs mcp-server/src/lib.rs
git commit -m "mcp-server: ProxyError enum"
```

---

## Task 3: Config module

**Files:**
- Create: `mcp-server/src/config.rs`
- Create: `mcp-server/config.example.toml`
- Modify: `mcp-server/src/lib.rs`

- [ ] **Step 1: Write failing unit tests**

Create `mcp-server/src/config.rs` with only the test module to start:

```rust
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

use crate::error::{ProxyError, Result};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub mcp: HashMap<String, McpEntry>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerSection {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self { host: default_host(), port: default_port() }
    }
}

fn default_host() -> String { "127.0.0.1".into() }
fn default_port() -> u16 { 9000 }

#[derive(Debug, Deserialize, Clone)]
pub struct McpEntry {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub cwd: Option<String>,
}

pub fn load_from_path(path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path)?;
    let cfg: Config = toml::from_str(&raw)?;
    for id in cfg.mcp.keys() {
        validate_slug(id)?;
    }
    Ok(cfg)
}

pub fn validate_slug(s: &str) -> Result<()> {
    let mut chars = s.chars();
    let first = chars.next().ok_or_else(|| ProxyError::InvalidServerId(s.into()))?;
    if !first.is_ascii_alphanumeric() {
        return Err(ProxyError::InvalidServerId(s.into()));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            return Err(ProxyError::InvalidServerId(s.into()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn defaults_apply_when_server_section_missing() {
        let cfg: Config = toml::from_str("").unwrap();
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 9000);
        assert!(cfg.mcp.is_empty());
    }

    #[test]
    fn parses_full_example() {
        let toml = r#"
            [server]
            host = "0.0.0.0"
            port = 9100

            [mcp.mcp-brave]
            command = "npx"
            args = ["-y", "@modelcontextprotocol/server-brave-search"]
            env = { BRAVE_API_KEY = "abc" }
        "#;
        let cfg: Config = toml::from_str(toml).unwrap();
        assert_eq!(cfg.server.port, 9100);
        let brave = &cfg.mcp["mcp-brave"];
        assert_eq!(brave.command, "npx");
        assert_eq!(brave.args.len(), 2);
        assert_eq!(brave.env["BRAVE_API_KEY"], "abc");
    }

    #[test]
    fn rejects_invalid_slug() {
        assert!(validate_slug("ok-id_1").is_ok());
        assert!(validate_slug("").is_err());
        assert!(validate_slug("-bad").is_err());
        assert!(validate_slug("with space").is_err());
        assert!(validate_slug("with/slash").is_err());
    }

    #[test]
    fn load_from_path_round_trip() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, r#"
            [mcp.mcp-echo]
            command = "echo"
        "#).unwrap();
        let cfg = load_from_path(f.path()).unwrap();
        assert_eq!(cfg.mcp["mcp-echo"].command, "echo");
    }
}
```

Add `pub mod config;` to `mcp-server/src/lib.rs`.

- [ ] **Step 2: Run the tests**

Run: `cargo test config::`
Expected: 4 passed.

- [ ] **Step 3: Add `tempfile` to `[dev-dependencies]` if not already**

Already added in Task 1.

- [ ] **Step 4: Write the example config**

`mcp-server/config.example.toml`:

```toml
# mcp-server example configuration
# Each [mcp.<slug>] entry becomes an SSE endpoint at /<slug>/sse

[server]
host = "127.0.0.1"
port = 9000

[mcp.mcp-brave]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-brave-search"]
env = { BRAVE_API_KEY = "REPLACE_ME" }

[mcp.mcp-filesystem]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "C:/some/path"]
```

- [ ] **Step 5: Commit**

```bash
git add mcp-server/src/config.rs mcp-server/src/lib.rs mcp-server/config.example.toml
git commit -m "mcp-server: TOML config types and loader"
```

---

## Task 4: JSON-RPC envelope + ID multiplexer

**Files:**
- Create: `mcp-server/src/jsonrpc.rs`
- Modify: `mcp-server/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Create `mcp-server/src/jsonrpc.rs`:

```rust
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// One direction of a JSON-RPC message classified by what kind of routing it needs.
#[derive(Debug)]
pub enum Inbound {
    /// Request from a session that has an `id` — needs id remapping.
    Request { original_id: Value, outbound_id: u64, payload: Value },
    /// Notification or response from a session — passed through with no remap.
    Passthrough(Value),
}

/// One direction of a JSON-RPC message coming back from the child.
#[derive(Debug)]
pub enum Outbound {
    /// Response with an outbound id we remember — rewrite to original.
    RoutedResponse { session_id: String, payload: Value },
    /// Response with an id we don't know — drop or log.
    UnknownResponse(Value),
    /// Notification (no id) — fanout to all sessions of this child.
    Broadcast(Value),
}

pub struct IdMux {
    next: AtomicU64,
    pending: parking_lot::Mutex<HashMap<u64, (String, Value)>>,
}

impl IdMux {
    pub fn new() -> Self {
        Self { next: AtomicU64::new(1), pending: parking_lot::Mutex::new(HashMap::new()) }
    }

    /// Process a payload received from a session. If it's a request, allocate
    /// an outbound id, store the mapping, and return a rewritten payload.
    pub fn rewrite_inbound(&self, session_id: &str, mut payload: Value) -> Inbound {
        let has_method = payload.get("method").is_some();
        let id = payload.get("id").cloned();
        match (has_method, id) {
            (true, Some(original_id)) if !original_id.is_null() => {
                let outbound_id = self.next.fetch_add(1, Ordering::Relaxed);
                self.pending.lock().insert(outbound_id, (session_id.to_string(), original_id.clone()));
                payload["id"] = Value::from(outbound_id);
                Inbound::Request { original_id, outbound_id, payload }
            }
            _ => Inbound::Passthrough(payload),
        }
    }

    /// Process a payload received from the child's stdout.
    pub fn classify_outbound(&self, mut payload: Value) -> Outbound {
        let has_method = payload.get("method").is_some();
        let id = payload.get("id").cloned();
        if has_method {
            // server-initiated notification or request — broadcast
            return Outbound::Broadcast(payload);
        }
        match id {
            Some(Value::Number(n)) if n.is_u64() => {
                let outbound_id = n.as_u64().unwrap();
                let mapping = self.pending.lock().remove(&outbound_id);
                match mapping {
                    Some((session_id, original_id)) => {
                        payload["id"] = original_id;
                        Outbound::RoutedResponse { session_id, payload }
                    }
                    None => Outbound::UnknownResponse(payload),
                }
            }
            _ => Outbound::UnknownResponse(payload),
        }
    }

    pub fn pending_count(&self) -> usize { self.pending.lock().len() }
}

impl Default for IdMux { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_gets_rewritten_id_and_pending_entry() {
        let mux = IdMux::new();
        let inbound = mux.rewrite_inbound("sess-A", json!({
            "jsonrpc": "2.0", "id": 7, "method": "tools/list"
        }));
        match inbound {
            Inbound::Request { original_id, outbound_id, payload } => {
                assert_eq!(original_id, json!(7));
                assert_eq!(payload["id"], json!(outbound_id));
                assert_eq!(mux.pending_count(), 1);
            }
            _ => panic!("expected Request"),
        }
    }

    #[test]
    fn notification_passes_through() {
        let mux = IdMux::new();
        let n = mux.rewrite_inbound("sess-A", json!({
            "jsonrpc": "2.0", "method": "notifications/initialized"
        }));
        assert!(matches!(n, Inbound::Passthrough(_)));
        assert_eq!(mux.pending_count(), 0);
    }

    #[test]
    fn response_routes_back_to_session() {
        let mux = IdMux::new();
        let inbound = mux.rewrite_inbound("sess-B", json!({
            "jsonrpc": "2.0", "id": 42, "method": "ping"
        }));
        let outbound_id = match inbound {
            Inbound::Request { outbound_id, .. } => outbound_id,
            _ => unreachable!(),
        };
        let outbound = mux.classify_outbound(json!({
            "jsonrpc": "2.0", "id": outbound_id, "result": {"ok": true}
        }));
        match outbound {
            Outbound::RoutedResponse { session_id, payload } => {
                assert_eq!(session_id, "sess-B");
                assert_eq!(payload["id"], json!(42));
                assert_eq!(payload["result"]["ok"], json!(true));
            }
            _ => panic!("expected RoutedResponse"),
        }
        assert_eq!(mux.pending_count(), 0);
    }

    #[test]
    fn server_initiated_notification_is_broadcast() {
        let mux = IdMux::new();
        let out = mux.classify_outbound(json!({
            "jsonrpc": "2.0", "method": "notifications/tools/list_changed"
        }));
        assert!(matches!(out, Outbound::Broadcast(_)));
    }
}
```

Append to `Cargo.toml` `[dependencies]`:

```toml
parking_lot = "0.12"
```

Add `pub mod jsonrpc;` to `mcp-server/src/lib.rs`.

- [ ] **Step 2: Run the tests**

Run: `cargo test jsonrpc::`
Expected: 4 passed.

- [ ] **Step 3: Commit**

```bash
git add mcp-server/src/jsonrpc.rs mcp-server/src/lib.rs mcp-server/Cargo.toml mcp-server/Cargo.lock
git commit -m "mcp-server: JSON-RPC envelope and id multiplexer"
```

---

## Task 5: Stdio child wrapper

**Files:**
- Create: `mcp-server/src/child.rs`
- Create: `mcp-server/tests/echo_fixture/Cargo.toml`
- Create: `mcp-server/tests/echo_fixture/src/main.rs`
- Modify: `mcp-server/src/lib.rs`

- [ ] **Step 1: Build the echo fixture binary**

`mcp-server/tests/echo_fixture/Cargo.toml`:

```toml
[package]
name = "echo_fixture"
version = "0.0.0"
edition = "2021"
publish = false

[[bin]]
name = "echo_fixture"
path = "src/main.rs"
```

`mcp-server/tests/echo_fixture/src/main.rs`:

```rust
// Reads newline-delimited JSON-RPC requests from stdin and replies on stdout
// with a result that echoes the params. Used in unit + integration tests.
use std::io::{BufRead, Write};

fn main() {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let Ok(msg) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
        if let Some(id) = msg.get("id").cloned() {
            let resp = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "echoed": msg.get("params").cloned().unwrap_or(serde_json::json!(null)) }
            });
            writeln!(out, "{}", resp).ok();
            out.flush().ok();
        }
    }
}

// minimal Cargo dep declared at the workspace level isn't possible since this
// is a standalone crate — we add serde_json here.
```

Append to that crate's `Cargo.toml`:

```toml
[dependencies]
serde_json = "1"
```

- [ ] **Step 2: Verify the fixture builds**

Run: `cd mcp-server/tests/echo_fixture && cargo build`
Expected: clean build. Note the binary path: `mcp-server/tests/echo_fixture/target/debug/echo_fixture(.exe)`.

- [ ] **Step 3: Write failing tests for the child wrapper**

Create `mcp-server/src/child.rs`:

```rust
use std::collections::HashMap;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child as TokioChild, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;

use crate::error::{ProxyError, Result};

pub struct StdioChild {
    pub id: String,
    process: TokioChild,
    stdin_tx: mpsc::Sender<String>,
    pub stdout_rx: mpsc::Receiver<String>,
}

impl StdioChild {
    pub async fn spawn(
        id: &str,
        program: &str,
        args: &[String],
        env: &HashMap<String, String>,
        cwd: Option<&str>,
    ) -> Result<Self> {
        let mut cmd = Command::new(program);
        cmd.args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(dir) = cwd { cmd.current_dir(dir); }
        for (k, v) in env { cmd.env(k, v); }

        let mut process = cmd.spawn().map_err(ProxyError::Io)?;
        let stdin = process.stdin.take().expect("piped");
        let stdout = process.stdout.take().expect("piped");
        let stderr = process.stderr.take().expect("piped");

        let (stdin_tx, stdin_rx) = mpsc::channel::<String>(64);
        let (stdout_tx, stdout_rx) = mpsc::channel::<String>(64);

        spawn_writer(stdin, stdin_rx, id.to_string());
        spawn_reader(stdout, stdout_tx, id.to_string());
        spawn_stderr_logger(stderr, id.to_string());

        Ok(Self { id: id.to_string(), process, stdin_tx, stdout_rx })
    }

    pub async fn send(&self, line: String) -> Result<()> {
        self.stdin_tx.send(line).await
            .map_err(|_| ProxyError::ChildExited {
                id: self.id.clone(),
                reason: "stdin channel closed".into(),
            })
    }

    pub async fn kill(mut self) {
        let _ = self.process.kill().await;
    }
}

fn spawn_writer(mut stdin: ChildStdin, mut rx: mpsc::Receiver<String>, id: String) {
    tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            if let Err(e) = stdin.write_all(line.as_bytes()).await {
                tracing::warn!(server = %id, error = %e, "stdin write failed");
                break;
            }
            if !line.ends_with('\n') {
                let _ = stdin.write_all(b"\n").await;
            }
            if let Err(e) = stdin.flush().await {
                tracing::warn!(server = %id, error = %e, "stdin flush failed");
                break;
            }
        }
    });
}

fn spawn_reader(stdout: ChildStdout, tx: mpsc::Sender<String>, id: String) {
    tokio::spawn(async move {
        let mut reader = BufReader::new(stdout).lines();
        loop {
            match reader.next_line().await {
                Ok(Some(line)) => {
                    if tx.send(line).await.is_err() { break; }
                }
                Ok(None) => {
                    tracing::info!(server = %id, "stdout EOF");
                    break;
                }
                Err(e) => {
                    tracing::warn!(server = %id, error = %e, "stdout read error");
                    break;
                }
            }
        }
    });
}

fn spawn_stderr_logger(stderr: tokio::process::ChildStderr, id: String) {
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            tracing::debug!(server = %id, stderr = %line);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn echo_path() -> PathBuf {
        // built by Task 5 Step 2
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("tests");
        p.push("echo_fixture");
        p.push("target");
        p.push("debug");
        #[cfg(windows)]
        p.push("echo_fixture.exe");
        #[cfg(not(windows))]
        p.push("echo_fixture");
        p
    }

    #[tokio::test]
    async fn echo_round_trip() {
        let path = echo_path();
        assert!(path.exists(), "build the echo fixture first: cd tests/echo_fixture && cargo build");

        let mut child = StdioChild::spawn(
            "echo",
            path.to_str().unwrap(),
            &[],
            &HashMap::new(),
            None,
        ).await.expect("spawn");

        child.send(r#"{"jsonrpc":"2.0","id":1,"method":"ping","params":{"hello":"world"}}"#.into())
            .await.unwrap();

        let line = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            child.stdout_rx.recv(),
        ).await.expect("timeout").expect("eof");

        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["id"], serde_json::json!(1));
        assert_eq!(v["result"]["echoed"]["hello"], serde_json::json!("world"));
    }
}
```

Add `pub mod child;` to `mcp-server/src/lib.rs`.

- [ ] **Step 4: Run the test**

Run: `cd mcp-server/tests/echo_fixture && cargo build && cd ../.. && cargo test child::`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add mcp-server/src/child.rs mcp-server/src/lib.rs mcp-server/tests/echo_fixture
git commit -m "mcp-server: stdio child wrapper + echo test fixture"
```

---

## Task 6: SSE session registry

**Files:**
- Create: `mcp-server/src/session.rs`
- Modify: `mcp-server/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Create `mcp-server/src/session.rs`:

```rust
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Each connected SSE client gets one of these. The `tx` end is held by the
/// session registry; the `rx` end is owned by the route handler that drains
/// it into the SSE response stream.
pub struct SseSession {
    pub id: String,
    pub tx: mpsc::Sender<String>,
}

#[derive(Default)]
pub struct SessionRegistry {
    inner: Mutex<HashMap<String, mpsc::Sender<String>>>,
}

impl SessionRegistry {
    pub fn new() -> Arc<Self> { Arc::new(Self::default()) }

    pub fn register(&self, id: String, tx: mpsc::Sender<String>) {
        self.inner.lock().insert(id, tx);
    }

    pub fn remove(&self, id: &str) {
        self.inner.lock().remove(id);
    }

    pub fn get(&self, id: &str) -> Option<mpsc::Sender<String>> {
        self.inner.lock().get(id).cloned()
    }

    pub fn all(&self) -> Vec<mpsc::Sender<String>> {
        self.inner.lock().values().cloned().collect()
    }

    pub fn len(&self) -> usize { self.inner.lock().len() }

    pub fn is_empty(&self) -> bool { self.inner.lock().is_empty() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_get_remove() {
        let reg = SessionRegistry::new();
        let (tx, mut rx) = mpsc::channel(4);
        reg.register("s1".into(), tx);
        assert_eq!(reg.len(), 1);

        let sender = reg.get("s1").unwrap();
        sender.send("hello".into()).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), "hello");

        reg.remove("s1");
        assert!(reg.is_empty());
        assert!(reg.get("s1").is_none());
    }

    #[tokio::test]
    async fn all_returns_every_sender() {
        let reg = SessionRegistry::new();
        let (tx1, _r1) = mpsc::channel(1);
        let (tx2, _r2) = mpsc::channel(1);
        reg.register("a".into(), tx1);
        reg.register("b".into(), tx2);
        assert_eq!(reg.all().len(), 2);
    }
}
```

Add `pub mod session;` to `mcp-server/src/lib.rs`.

- [ ] **Step 2: Run the tests**

Run: `cargo test session::`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add mcp-server/src/session.rs mcp-server/src/lib.rs
git commit -m "mcp-server: SSE session registry"
```

---

## Task 7: ChildProxy integration layer

**Files:**
- Create: `mcp-server/src/proxy.rs`
- Modify: `mcp-server/src/lib.rs`

- [ ] **Step 1: Write a failing integration test**

Create `mcp-server/src/proxy.rs`:

```rust
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::child::StdioChild;
use crate::config::McpEntry;
use crate::error::Result;
use crate::jsonrpc::{IdMux, Inbound, Outbound};
use crate::session::SessionRegistry;

pub struct ChildProxy {
    pub id: String,
    sessions: Arc<SessionRegistry>,
    mux: Arc<IdMux>,
    stdin_tx: mpsc::Sender<String>,
}

impl ChildProxy {
    pub async fn spawn(id: &str, entry: &McpEntry) -> Result<Arc<Self>> {
        let child = StdioChild::spawn(id, &entry.command, &entry.args, &entry.env, entry.cwd.as_deref()).await?;
        let sessions = SessionRegistry::new();
        let mux = Arc::new(IdMux::new());

        let (in_tx, mut in_rx) = mpsc::channel::<String>(64);

        // forward inbound channel -> child stdin
        let child_send_tx = child.stdin_tx_clone();
        tokio::spawn(async move {
            while let Some(line) = in_rx.recv().await {
                if child_send_tx.send(line).await.is_err() { break; }
            }
        });

        // drain child stdout -> classify + route
        let sessions_for_reader = sessions.clone();
        let mux_for_reader = mux.clone();
        let mut stdout_rx = child.stdout_rx;
        tokio::spawn(async move {
            while let Some(line) = stdout_rx.recv().await {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                    tracing::warn!("non-json line from child: {}", line);
                    continue;
                };
                match mux_for_reader.classify_outbound(v) {
                    Outbound::RoutedResponse { session_id, payload } => {
                        if let Some(tx) = sessions_for_reader.get(&session_id) {
                            let _ = tx.send(payload.to_string()).await;
                        }
                    }
                    Outbound::Broadcast(payload) => {
                        let s = payload.to_string();
                        for tx in sessions_for_reader.all() {
                            let _ = tx.send(s.clone()).await;
                        }
                    }
                    Outbound::UnknownResponse(v) => {
                        tracing::warn!("dropping unmapped response: {}", v);
                    }
                }
            }
        });

        Ok(Arc::new(Self {
            id: id.to_string(),
            sessions,
            mux,
            stdin_tx: in_tx,
        }))
    }

    pub fn register_session(&self, session_id: String, tx: mpsc::Sender<String>) {
        self.sessions.register(session_id, tx);
    }

    pub fn drop_session(&self, session_id: &str) {
        self.sessions.remove(session_id);
    }

    pub async fn send_from_session(&self, session_id: &str, payload: serde_json::Value) -> Result<()> {
        let inbound = self.mux.rewrite_inbound(session_id, payload);
        let line = match inbound {
            Inbound::Request { payload, .. } => payload.to_string(),
            Inbound::Passthrough(payload) => payload.to_string(),
        };
        let _ = self.stdin_tx.send(line).await;
        Ok(())
    }

    pub fn session_count(&self) -> usize { self.sessions.len() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn echo_entry() -> McpEntry {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("tests/echo_fixture/target/debug/echo_fixture");
        #[cfg(windows)]
        let p = p.with_extension("exe");
        McpEntry {
            command: p.to_string_lossy().into_owned(),
            args: vec![],
            env: HashMap::new(),
            cwd: None,
        }
    }

    #[tokio::test]
    async fn proxy_routes_response_back_to_originating_session() {
        let proxy = ChildProxy::spawn("echo", &echo_entry()).await.unwrap();

        let (tx_a, mut rx_a) = mpsc::channel::<String>(4);
        let (tx_b, mut rx_b) = mpsc::channel::<String>(4);
        proxy.register_session("sess-a".into(), tx_a);
        proxy.register_session("sess-b".into(), tx_b);

        proxy.send_from_session("sess-a", serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "ping", "params": {"who": "a"}
        })).await.unwrap();

        let line = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rx_a.recv(),
        ).await.expect("timeout").expect("closed");

        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["id"], serde_json::json!(1));
        assert_eq!(v["result"]["echoed"]["who"], serde_json::json!("a"));

        // session B should not have received anything
        assert!(rx_b.try_recv().is_err());
    }
}
```

Update `mcp-server/src/child.rs` — expose a `stdin_tx_clone` method:

```rust
impl StdioChild {
    // ... existing code ...

    pub fn stdin_tx_clone(&self) -> mpsc::Sender<String> {
        self.stdin_tx.clone()
    }
}
```

Add `pub mod proxy;` to `mcp-server/src/lib.rs`.

- [ ] **Step 2: Run the test**

Run: `cargo test proxy::`
Expected: 1 passed (assumes echo fixture is built from Task 5).

- [ ] **Step 3: Commit**

```bash
git add mcp-server/src/proxy.rs mcp-server/src/child.rs mcp-server/src/lib.rs
git commit -m "mcp-server: ChildProxy integration layer"
```

---

## Task 8: Application state with lazy spawn

**Files:**
- Create: `mcp-server/src/state.rs`
- Modify: `mcp-server/src/lib.rs`

- [ ] **Step 1: Write failing test**

Create `mcp-server/src/state.rs`:

```rust
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::proxy::ChildProxy;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    proxies: Arc<Mutex<HashMap<String, Arc<ChildProxy>>>>,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        Self {
            config: Arc::new(config),
            proxies: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self.config.mcp.keys().cloned().collect();
        v.sort();
        v
    }

    /// Returns an existing proxy or spawns it.
    pub async fn get_or_spawn(&self, id: &str) -> Result<Arc<ChildProxy>> {
        if let Some(p) = self.proxies.lock().get(id).cloned() {
            return Ok(p);
        }
        let entry = self.config.mcp.get(id)
            .ok_or_else(|| ProxyError::UnknownServer(id.to_string()))?
            .clone();
        let proxy = ChildProxy::spawn(id, &entry).await?;
        self.proxies.lock().insert(id.to_string(), proxy.clone());
        Ok(proxy)
    }

    pub fn live_ids(&self) -> Vec<String> {
        self.proxies.lock().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::McpEntry;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn echo_config() -> Config {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("tests/echo_fixture/target/debug/echo_fixture");
        #[cfg(windows)]
        let p = p.with_extension("exe");
        let mut mcp = HashMap::new();
        mcp.insert("mcp-echo".to_string(), McpEntry {
            command: p.to_string_lossy().into_owned(),
            args: vec![],
            env: HashMap::new(),
            cwd: None,
        });
        Config {
            server: Default::default(),
            mcp,
        }
    }

    #[tokio::test]
    async fn unknown_server_returns_error() {
        let state = AppState::new(echo_config());
        let err = state.get_or_spawn("nope").await.unwrap_err();
        assert!(matches!(err, ProxyError::UnknownServer(_)));
    }

    #[tokio::test]
    async fn second_get_returns_same_proxy() {
        let state = AppState::new(echo_config());
        let p1 = state.get_or_spawn("mcp-echo").await.unwrap();
        let p2 = state.get_or_spawn("mcp-echo").await.unwrap();
        assert!(Arc::ptr_eq(&p1, &p2));
    }
}
```

Add `pub mod state;` to `mcp-server/src/lib.rs`.

- [ ] **Step 2: Run tests**

Run: `cargo test state::`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add mcp-server/src/state.rs mcp-server/src/lib.rs
git commit -m "mcp-server: AppState with lazy child spawning"
```

---

## Task 9: HTTP routes (SSE + messages + listing)

**Files:**
- Create: `mcp-server/src/routes.rs`
- Modify: `mcp-server/src/lib.rs`

- [ ] **Step 1: Write failing handler tests with an in-process router**

Create `mcp-server/src/routes.rs`:

```rust
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::Stream;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(list_servers))
        .route("/health", get(health))
        .route("/:id/sse", get(open_sse))
        .route("/:id/messages", post(post_message))
        .with_state(state)
}

async fn health() -> &'static str { "ok" }

async fn list_servers(State(state): State<AppState>) -> Json<serde_json::Value> {
    let ids = state.ids();
    let live = state.live_ids();
    Json(json!({
        "servers": ids.iter().map(|id| json!({
            "id": id,
            "sse": format!("/{}/sse", id),
            "messages": format!("/{}/messages", id),
            "live": live.contains(id),
        })).collect::<Vec<_>>()
    }))
}

#[derive(Debug, Deserialize)]
struct MessagesQuery { #[serde(rename = "sessionId")] session_id: String }

async fn open_sse(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, std::convert::Infallible>>>, (StatusCode, String)> {
    let proxy = state.get_or_spawn(&id).await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    let session_id = uuid::Uuid::new_v4().to_string();

    let (tx, rx) = mpsc::channel::<String>(64);
    proxy.register_session(session_id.clone(), tx);

    let endpoint_url = format!("/{}/messages?sessionId={}", id, session_id);
    let endpoint_event = Event::default().event("endpoint").data(endpoint_url);

    let proxy_for_cleanup = proxy.clone();
    let session_id_for_cleanup = session_id.clone();

    let stream = async_stream::stream! {
        yield Ok(endpoint_event);
        let mut rx = ReceiverStream::new(rx);
        while let Some(line) = rx.next().await {
            yield Ok(Event::default().event("message").data(line));
        }
        proxy_for_cleanup.drop_session(&session_id_for_cleanup);
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

async fn post_message(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<MessagesQuery>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let proxy = match state.get_or_spawn(&id).await {
        Ok(p) => p,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };
    if let Err(e) = proxy.send_from_session(&q.session_id, body).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    StatusCode::ACCEPTED.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, McpEntry, ServerSection};
    use axum::body::Body;
    use axum::http::Request;
    use std::collections::HashMap;
    use tower::ServiceExt;

    fn empty_state() -> AppState {
        AppState::new(Config {
            server: ServerSection::default(),
            mcp: HashMap::new(),
        })
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let app = router(empty_state());
        let resp = app.oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_returns_configured_ids() {
        let mut mcp = HashMap::new();
        mcp.insert("mcp-foo".into(), McpEntry {
            command: "true".into(), args: vec![], env: HashMap::new(), cwd: None,
        });
        let state = AppState::new(Config { server: ServerSection::default(), mcp });
        let app = router(state);

        let resp = app.oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["servers"][0]["id"], serde_json::json!("mcp-foo"));
        assert_eq!(v["servers"][0]["sse"], serde_json::json!("/mcp-foo/sse"));
    }

    #[tokio::test]
    async fn unknown_id_returns_404_on_sse() {
        let app = router(empty_state());
        let resp = app.oneshot(Request::builder().uri("/nope/sse").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
```

Append to `Cargo.toml` `[dependencies]`:

```toml
tokio-stream = "0.1"
tower = "0.5"
```

Add `pub mod routes;` to `mcp-server/src/lib.rs`.

- [ ] **Step 2: Run the tests**

Run: `cargo test routes::`
Expected: 3 passed.

- [ ] **Step 3: Commit**

```bash
git add mcp-server/src/routes.rs mcp-server/src/lib.rs mcp-server/Cargo.toml mcp-server/Cargo.lock
git commit -m "mcp-server: axum routes for SSE, messages, and listing"
```

---

## Task 10: CLI + main bootstrap

**Files:**
- Modify: `mcp-server/src/main.rs`
- Create: `mcp-server/src/shutdown.rs`
- Modify: `mcp-server/src/lib.rs`

- [ ] **Step 1: Write shutdown helper**

Create `mcp-server/src/shutdown.rs`:

```rust
use tokio::signal;

pub async fn ctrl_c() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("install ctrl-c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received");
}
```

Add `pub mod shutdown;` to `mcp-server/src/lib.rs`.

- [ ] **Step 2: Write main**

Replace `mcp-server/src/main.rs`:

```rust
use std::path::PathBuf;

use clap::Parser;
use mcp_server::{config, routes, shutdown, state::AppState};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "mcp-server", about = "Local HTTP+SSE proxy for stdio MCP servers")]
struct Cli {
    /// Path to the TOML config file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Override the bind host from config
    #[arg(long)]
    host: Option<String>,

    /// Override the bind port from config
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,mcp_server=debug")))
        .init();

    let cli = Cli::parse();
    let mut cfg = config::load_from_path(&cli.config)?;
    if let Some(h) = cli.host { cfg.server.host = h; }
    if let Some(p) = cli.port { cfg.server.port = p; }

    let bind_addr = format!("{}:{}", cfg.server.host, cfg.server.port);
    tracing::info!("loaded {} server(s) from {}", cfg.mcp.len(), cli.config.display());
    for id in cfg.mcp.keys() {
        tracing::info!("  /{id}/sse → command `{}`", cfg.mcp[id].command);
    }

    let state = AppState::new(cfg);
    let app = routes::router(state).layer(tower_http::trace::TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("mcp-server listening on http://{}", bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::ctrl_c())
        .await?;

    Ok(())
}
```

- [ ] **Step 3: Verify build**

Run: `cargo build --bin mcp-server`
Expected: clean build.

- [ ] **Step 4: Smoke test the binary**

Create a temp config: `mcp-server/test-config.toml`:

```toml
[server]
port = 9123

[mcp.mcp-echo]
command = "true"
```

Run in one shell: `cargo run --bin mcp-server -- --config test-config.toml`
In another shell: `curl http://127.0.0.1:9123/health` → `ok`
And: `curl http://127.0.0.1:9123/` → JSON listing with `mcp-echo`.

Stop the server with Ctrl+C — confirm it exits cleanly.

Delete `mcp-server/test-config.toml`.

- [ ] **Step 5: Commit**

```bash
git add mcp-server/src/main.rs mcp-server/src/shutdown.rs mcp-server/src/lib.rs
git commit -m "mcp-server: CLI + main + graceful shutdown"
```

---

## Task 11: End-to-end integration test

**Files:**
- Create: `mcp-server/tests/integration.rs`

- [ ] **Step 1: Write the failing test**

Create `mcp-server/tests/integration.rs`:

```rust
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
```

- [ ] **Step 2: Run the test**

Run: `cd mcp-server/tests/echo_fixture && cargo build && cd ../.. && cargo test --test integration`
Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add mcp-server/tests/integration.rs
git commit -m "mcp-server: end-to-end SSE round-trip integration test"
```

---

## Task 12: README, config example refresh, top-level wiring

**Files:**
- Create: `mcp-server/README.md`
- Modify: `mcp-server/config.example.toml` (only if it diverged from current Config struct)
- Modify: top-level `README.md` (add link to mcp-server)

- [ ] **Step 1: Write the README**

`mcp-server/README.md`:

````markdown
# mcp-server

Local HTTP gateway that runs multiple **stdio**-based MCP servers as child
processes and exposes each one over the **HTTP+SSE** transport.

```
                 ┌──────────────────────────────────────────┐
   HTTP+SSE      │              mcp-server                  │      stdio
   clients   <─> │  /mcp-brave/sse        ┌──────────────┐  │ <──> mcp-server-brave
                 │  /mcp-filesystem/sse   │  multiplexer │  │ <──> mcp-server-filesystem
                 │  /<id>/messages?...    └──────────────┘  │ <──> ...
                 └──────────────────────────────────────────┘
```

## Quick start

1. Build: `cargo build --release`
2. Copy `config.example.toml` to `config.toml` and edit.
3. Run: `./target/release/mcp-server --config config.toml`
4. Connect a client to e.g. `http://localhost:9000/mcp-brave/sse`.

## Configuration

```toml
[server]
host = "127.0.0.1"
port = 9000

[mcp.mcp-brave]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-brave-search"]
env = { BRAVE_API_KEY = "REPLACE_ME" }
```

The TOML key (`mcp-brave`) becomes the URL segment. Slugs must match
`^[A-Za-z0-9][A-Za-z0-9_-]*$`.

## Endpoints

| Method | Path                              | Purpose                                      |
| ------ | --------------------------------- | -------------------------------------------- |
| GET    | `/`                               | List configured servers                      |
| GET    | `/health`                         | Liveness probe                               |
| GET    | `/<id>/sse`                       | Open SSE stream; first event is `endpoint`   |
| POST   | `/<id>/messages?sessionId=<uuid>` | Submit a JSON-RPC message for that session   |

## How it works

* On first connection to `/<id>/sse`, the child process for `<id>` is spawned.
* Each SSE client gets a UUID session id and an independent JSON-RPC `id`
  namespace. The proxy rewrites ids before forwarding to the child and again
  on the way back.
* Notifications (no `id`) and server-initiated requests are broadcast to all
  attached sessions of that child.

## CLI

```
mcp-server [--config <path>] [--host <bind>] [--port <port>]
```

Set `RUST_LOG=mcp_server=debug,info` to see per-line trace output.
````

- [ ] **Step 2: Link from top-level README**

In `E:/develop/callmodel/README.md`, append a section:

```markdown
## Related

- [`mcp-server/`](./mcp-server/README.md) — Local HTTP+SSE proxy for stdio MCP servers.
```

- [ ] **Step 3: Final full-suite run**

Run: `cd mcp-server && cargo test`
Expected: all unit tests + integration test pass.

Run: `cargo clippy --all-targets -- -D warnings`
Expected: no warnings.

Run: `cargo fmt --all -- --check`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add mcp-server/README.md README.md
git commit -m "mcp-server: README + top-level link"
```

---

## Self-Review Checklist (post-write)

- [x] Spec coverage: every requirement maps to a task
  - "run multiple stdio MCP tools": Tasks 3, 5, 7, 8
  - "proxied to SSE server": Tasks 6, 7, 9
  - "URI as identifier (`mcp-brave`)": Task 3 (slug validation), Task 9 (routes)
  - "endpoint pattern `/<id>/sse`": Task 9 + integration test in Task 11
- [x] No placeholders or TBDs in code blocks
- [x] Type/identifier consistency across tasks:
  - `ProxyError` (Task 2) used in Tasks 3, 5, 7, 8
  - `Config`/`McpEntry` (Task 3) used in Tasks 7, 8
  - `IdMux`/`Inbound`/`Outbound` (Task 4) used in Task 7
  - `StdioChild::stdin_tx_clone` (Task 5) consumed in Task 7
  - `ChildProxy::{spawn,register_session,drop_session,send_from_session}` (Task 7) consumed in Tasks 8, 9
  - `AppState::{get_or_spawn,ids,live_ids}` (Task 8) consumed in Task 9
