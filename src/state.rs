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
