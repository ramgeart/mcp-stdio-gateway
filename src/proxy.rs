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
    _child: StdioChild,  // keeps the child process alive via kill_on_drop
}

impl ChildProxy {
    pub async fn spawn(id: &str, entry: &McpEntry) -> Result<Arc<Self>> {
        let mut child = StdioChild::spawn(id, &entry.command, &entry.args, &entry.env, entry.cwd.as_deref()).await?;
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
        let mut stdout_rx = child.take_stdout_rx();
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
            _child: child,
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
