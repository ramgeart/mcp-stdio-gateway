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
