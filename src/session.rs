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
