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
