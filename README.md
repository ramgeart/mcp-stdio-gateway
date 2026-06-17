# mcp-server

Local HTTP gateway that runs multiple **stdio**-based MCP servers as child
processes and exposes each one over the **HTTP+SSE** transport.

```
                 ┌──────────────────────────────────────────┐
   HTTP+SSE      │              mcp-server                  │      stdio
   clients   <─> │  /mcp-brave/sse        ┌──────────────┐  │ <──> mcp-server-brave
                 │  /mcp-filesystem/sse   │  multiplexer │  │ <──> mcp-server-filesystem
                 │  /<id>/messages?...    └──────────────┘  │ <──> ...
                 └──────────────────────────────────────────┘
```

## Quick start

1. Build: `cargo build --release`
2. Copy `config.example.toml` to `config.toml` and edit.
3. Run: `./target/release/mcp-server --config config.toml`
4. Connect a client to e.g. `http://localhost:9000/mcp-brave/sse`.

## Configuration

```toml
[server]
host = "127.0.0.1"
port = 9000

[mcp.mcp-brave]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-brave-search"]
env = { BRAVE_API_KEY = "REPLACE_ME" }
```

The TOML key (`mcp-brave`) becomes the URL segment. Slugs must match
`^[A-Za-z0-9][A-Za-z0-9_-]*$`.

## Endpoints

| Method | Path                              | Purpose                                      |
| ------ | --------------------------------- | -------------------------------------------- |
| GET    | `/`                               | List configured servers                      |
| GET    | `/health`                         | Liveness probe                               |
| GET    | `/<id>/sse`                       | Open SSE stream; first event is `endpoint`   |
| POST   | `/<id>/messages?sessionId=<uuid>` | Submit a JSON-RPC message for that session   |

## How it works

* On first connection to `/<id>/sse`, the child process for `<id>` is spawned.
* Each SSE client gets a UUID session id and an independent JSON-RPC `id`
  namespace. The proxy rewrites ids before forwarding to the child and again
  on the way back.
* Notifications (no `id`) and server-initiated requests are broadcast to all
  attached sessions of that child.

## CLI

```
mcp-server [--config <path>] [--host <bind>] [--port <port>]
```

Set `RUST_LOG=mcp_server=debug,info` to see per-line trace output.
