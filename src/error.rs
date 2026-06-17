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

    #[error("toml serialization error: {0}")]
    TomlSer(#[from] toml::ser::Error),
}

pub type Result<T> = std::result::Result<T, ProxyError>;

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
