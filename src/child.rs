use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child as TokioChild, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;

use crate::error::{ProxyError, Result};

pub struct StdioChild {
    pub id: String,
    process: TokioChild,
    stdin_tx: mpsc::Sender<String>,
    stdout_rx: Option<mpsc::Receiver<String>>,
}

impl StdioChild {
    pub async fn spawn(
        id: &str,
        program: &str,
        args: &[String],
        env: &HashMap<String, String>,
        cwd: Option<&str>,
    ) -> Result<Self> {
        let mut resolved_program = program.to_string();
        #[cfg(windows)]
        if let Some(resolved) = find_windows_program(program) {
            resolved_program = resolved;
        }

        let mut cmd = Command::new(&resolved_program);
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

        Ok(Self { id: id.to_string(), process, stdin_tx, stdout_rx: Some(stdout_rx) })
    }

    pub async fn send(&self, line: String) -> Result<()> {
        self.stdin_tx.send(line).await
            .map_err(|_| ProxyError::ChildExited {
                id: self.id.clone(),
                reason: "stdin channel closed".into(),
            })
    }

    pub fn stdin_tx_clone(&self) -> mpsc::Sender<String> {
        self.stdin_tx.clone()
    }

    pub fn take_stdout_rx(&mut self) -> mpsc::Receiver<String> {
        self.stdout_rx.take().expect("stdout_rx already taken")
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

#[cfg(windows)]
fn find_windows_program(program: &str) -> Option<String> {
    let path = Path::new(program);
    if path.extension().is_some() {
        return None;
    }

    let extensions = ["exe", "cmd", "bat", "com"];

    if program.contains('\\') || program.contains('/') {
        for ext in &extensions {
            let candidate = path.with_extension(*ext);
            if candidate.exists() {
                return Some(candidate.to_string_lossy().into_owned());
            }
        }
        return None;
    }

    if let Some(path_var) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_var) {
            for ext in &extensions {
                let candidate = dir.join(format!("{}.{}", program, ext));
                if candidate.exists() {
                    return Some(candidate.to_string_lossy().into_owned());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn echo_path() -> PathBuf {
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

        let mut stdout_rx = child.take_stdout_rx();
        let line = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stdout_rx.recv(),
        ).await.expect("timeout").expect("eof");

        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["id"], serde_json::json!(1));
        assert_eq!(v["result"]["echoed"]["hello"], serde_json::json!("world"));
    }

    #[test]
    #[cfg(windows)]
    fn test_find_windows_program() {
        let cmd_resolved = find_windows_program("cmd");
        assert!(cmd_resolved.is_some());
        let path = cmd_resolved.unwrap();
        assert!(path.to_lowercase().ends_with("cmd.exe"));
    }
}
