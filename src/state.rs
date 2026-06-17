use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::proxy::ChildProxy;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Mutex<Config>>,
    proxies: Arc<Mutex<HashMap<String, Arc<ChildProxy>>>>,
    pub config_path: Option<std::path::PathBuf>,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        Self {
            config: Arc::new(Mutex::new(config)),
            proxies: Arc::new(Mutex::new(HashMap::new())),
            config_path: None,
        }
    }

    pub fn with_path(mut self, path: std::path::PathBuf) -> Self {
        self.config_path = Some(path);
        self
    }

    pub fn ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self.config.lock().mcp.keys().cloned().collect();
        v.sort();
        v
    }

    /// Returns an existing proxy or spawns it.
    pub async fn get_or_spawn(&self, id: &str) -> Result<Arc<ChildProxy>> {
        if let Some(p) = self.proxies.lock().get(id).cloned() {
            return Ok(p);
        }
        let entry = self.config.lock().mcp.get(id)
            .ok_or_else(|| ProxyError::UnknownServer(id.to_string()))?
            .clone();
        let proxy = ChildProxy::spawn(id, &entry).await?;
        self.proxies.lock().insert(id.to_string(), proxy.clone());
        Ok(proxy)
    }

    pub fn live_ids(&self) -> Vec<String> {
        self.proxies.lock().keys().cloned().collect()
    }

    pub fn add_server(&self, id: String, entry: crate::config::McpEntry) -> Result<()> {
        self.config.lock().mcp.insert(id, entry);
        self.save_to_disk()
    }

    pub async fn disable_server(&self, id: &str) -> bool {
        let removed_config = self.config.lock().mcp.remove(id).is_some();
        let removed_proxy = self.proxies.lock().remove(id).is_some();
        if removed_config {
            let _ = self.save_to_disk();
        }
        removed_config || removed_proxy
    }

    pub fn save_to_disk(&self) -> Result<()> {
        if let Some(ref path) = self.config_path {
            let config_guard = self.config.lock();
            crate::config::save_to_path(path, &config_guard)?;
        }
        Ok(())
    }

    pub async fn reload_from_disk(&self) -> Result<()> {
        if let Some(ref path) = self.config_path {
            let next_config = crate::config::load_from_path(path)?;
            
            let mut current_config = self.config.lock();
            let mut current_proxies = self.proxies.lock();
            
            current_config.server = next_config.server;
            
            let mut keys_to_remove = Vec::new();
            for server_id in current_proxies.keys() {
                if !next_config.mcp.contains_key(server_id) {
                    keys_to_remove.push(server_id.clone());
                } else {
                    let next_entry = &next_config.mcp[server_id];
                    let current_entry = current_config.mcp.get(server_id);
                    if let Some(curr) = current_entry {
                        if curr.command != next_entry.command || curr.args != next_entry.args || curr.env != next_entry.env || curr.cwd != next_entry.cwd {
                            keys_to_remove.push(server_id.clone());
                        }
                    }
                }
            }
            
            for key in keys_to_remove {
                current_proxies.remove(&key);
            }
            
            current_config.mcp = next_config.mcp;
            Ok(())
        } else {
            Err(ProxyError::Config("No configuration path tracking available to reload".to_string()))
        }
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
            is_http: false,
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
