use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::error::{ProxyError, Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub mcp: HashMap<String, McpEntry>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct McpEntry {
    pub command: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_http: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

pub fn load_from_path(path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path)?;
    let cfg: Config = toml::from_str(&raw)?;
    for id in cfg.mcp.keys() {
        validate_slug(id)?;
    }
    Ok(cfg)
}

pub fn save_to_path(path: &Path, config: &Config) -> Result<()> {
    let raw = toml::to_string_pretty(config)?;
    std::fs::write(path, raw)?;
    Ok(())
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
