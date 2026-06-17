use std::collections::{HashMap, HashSet};
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
    pending_requests: Arc<parking_lot::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<serde_json::Value>>>>,
    active_sessions: Arc<parking_lot::Mutex<HashSet<String>>>,
    _child: Option<StdioChild>,  // keeps the stdio child process alive via kill_on_drop (if any)
    pub is_http: bool,
}

impl std::fmt::Debug for ChildProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildProxy").field("id", &self.id).finish_non_exhaustive()
    }
}

impl ChildProxy {
    pub async fn spawn(id: &str, entry: &McpEntry) -> Result<Arc<Self>> {
        let sessions = SessionRegistry::new();
        let mux = Arc::new(IdMux::new());
        let pending_requests: Arc<parking_lot::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<serde_json::Value>>>> = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let active_sessions = Arc::new(parking_lot::Mutex::new(HashSet::new()));

        let (in_tx, mut in_rx) = mpsc::channel::<String>(64);

        if entry.is_http {
            let client = ::reqwest::Client::new();
            let http_url = entry.command.clone();

            // forward inbound channel to HTTP POST
            let pending_requests_for_post = pending_requests.clone();
            let mux_for_post = mux.clone();
            let http_url_for_post = http_url.clone();
            tokio::spawn(async move {
                while let Some(line) = in_rx.recv().await {
                    let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                        continue;
                    };
                    let outbound_id_opt = v.get("id").and_then(|id| id.as_u64());
                    
                    let client_clone = client.clone();
                    let url_clone = http_url_for_post.clone();
                    let pending_requests_clone = pending_requests_for_post.clone();
                    let mux_clone = mux_for_post.clone();
                    
                    tokio::spawn(async move {
                        let res_post: std::result::Result<::reqwest::Response, ::reqwest::Error> = client_clone.post(&url_clone)
                            .header("Accept", "application/json")
                            .header("Content-Type", "application/json")
                            .json(&v)
                            .send().await;

                        if let Ok(resp) = res_post {
                            if resp.status().is_success() {
                                if let Ok(resp_json) = resp.json::<serde_json::Value>().await {
                                    if let Some(outbound_id) = outbound_id_opt {
                                        let oneshot_tx = {
                                            let mut pending = pending_requests_clone.lock();
                                            pending.remove(&outbound_id)
                                        };
                                        if let Some(tx) = oneshot_tx {
                                            if let Outbound::RoutedResponse { payload, .. } = mux_clone.classify_outbound(resp_json) {
                                                let _ = tx.send(payload);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            });

            return Ok(Arc::new(Self {
                id: id.to_string(),
                sessions,
                mux,
                stdin_tx: in_tx,
                pending_requests,
                active_sessions,
                _child: None,
                is_http: true,
            }));
        }

        let mut child = StdioChild::spawn(id, &entry.command, &entry.args, &entry.env, entry.cwd.as_deref()).await?;

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
        let pending_requests_for_reader = pending_requests.clone();
        let mut stdout_rx = child.take_stdout_rx();
        tokio::spawn(async move {
            while let Some(line) = stdout_rx.recv().await {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
                    tracing::warn!("non-json line from child: {}", line);
                    continue;
                };
                let id_opt = v.get("id").and_then(|id| id.as_u64());
                if let Some(outbound_id) = id_opt {
                    let oneshot_tx = {
                        let mut pending = pending_requests_for_reader.lock();
                        pending.remove(&outbound_id)
                    };
                    if let Some(tx) = oneshot_tx {
                        if let Outbound::RoutedResponse { payload, .. } = mux_for_reader.classify_outbound(v) {
                            let _ = tx.send(payload);
                        }
                        continue;
                    }
                }
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
            pending_requests,
            active_sessions,
            _child: Some(child),
            is_http: false,
        }))
    }

    pub fn register_session(&self, session_id: String, tx: mpsc::Sender<String>) {
        self.add_active_session(session_id.clone());
        self.sessions.register(session_id, tx);
    }

    pub fn drop_session(&self, session_id: &str) {
        self.sessions.remove(session_id);
    }

    pub fn add_active_session(&self, session_id: String) {
        let mut active = self.active_sessions.lock();
        active.insert(session_id);
    }

    pub fn drop_active_session(&self, session_id: &str) {
        let mut active = self.active_sessions.lock();
        active.remove(session_id);
    }

    pub fn has_active_session(&self, session_id: &str) -> bool {
        let active = self.active_sessions.lock();
        active.contains(session_id) || self.sessions.get(session_id).is_some()
    }

    pub async fn send_request_from_session(&self, session_id: &str, payload: serde_json::Value) -> Result<serde_json::Value> {
        let inbound = self.mux.rewrite_inbound(session_id, payload);
        match inbound {
            Inbound::Request { outbound_id, payload, .. } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                {
                    let mut pending = self.pending_requests.lock();
                    pending.insert(outbound_id, tx);
                }
                let line = payload.to_string();
                if self.stdin_tx.send(line).await.is_err() {
                    let mut pending = self.pending_requests.lock();
                    pending.remove(&outbound_id);
                    return Err(crate::error::ProxyError::ChildExited {
                        id: self.id.clone(),
                        reason: "stdin channel closed".into(),
                    });
                }
                let response = rx.await.map_err(|_| crate::error::ProxyError::ChildExited {
                    id: self.id.clone(),
                    reason: "oneshot receiver hung up (child exited?)".into(),
                })?;
                Ok(response)
            }
            Inbound::Passthrough(payload) => {
                let line = payload.to_string();
                let _ = self.stdin_tx.send(line).await;
                Ok(serde_json::Value::Null)
            }
        }
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
            is_http: false,
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
