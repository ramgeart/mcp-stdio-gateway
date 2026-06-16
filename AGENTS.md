# AGENTS.md — mcp-server

> Guidance for AI coding agents (Claude Code, Copilot CLI, Cursor, etc.) working in the `mcp-server/` crate. See [`PLAN.md`](./PLAN.md) for the full task-by-task build order.

## What this crate is

A small Rust binary that acts as an HTTP gateway in front of one or more
**stdio**-based MCP (Model Context Protocol) servers. For every TOML entry
under `[mcp.<slug>]` it lazily spawns the configured `command`, then exposes:

- `GET  /<slug>/sse` — opens an SSE stream; first event is `endpoint`
- `POST /<slug>/messages?sessionId=<uuid>` — sends a JSON-RPC message

Concretely: an entry named `mcp-brave` running `npx -y @modelcontextprotocol/server-brave-search`
shows up at `http://localhost:9000/mcp-brave/sse`.

## Tech stack

- **Language:** Rust 2021
- **Runtime:** `tokio` (full features)
- **HTTP:** `axum` 0.8, `tower-http`
- **Process control:** `tokio::process::Command` with piped `stdin`/`stdout`/`stderr`
- **Serde:** `serde`, `serde_json`, `toml`
- **Logs:** `tracing`, `tracing-subscriber`
- **IDs:** `uuid` v4
- **Errors:** `thiserror` for `ProxyError`, `anyhow` only at the `main` boundary

## Repository layout

```
mcp-server/
├── PLAN.md              implementation plan (this file's sibling)
├── AGENTS.md            ← you are here
├── README.md            user docs (generated in Task 12 of PLAN.md)
├── Cargo.toml
├── config.example.toml  sample config
├── src/
│   ├── main.rs          CLI + tokio main + axum boot
│   ├── lib.rs           module re-exports
│   ├── error.rs         ProxyError enum
│   ├── config.rs        Config / McpEntry + TOML loader + slug validation
│   ├── jsonrpc.rs       IdMux (rewrites JSON-RPC ids for multiplexing)
│   ├── child.rs         StdioChild (spawn + line-oriented stdio)
│   ├── session.rs       SessionRegistry (per-child SSE session map)
│   ├── proxy.rs         ChildProxy (Child + IdMux + Registry)
│   ├── state.rs         AppState (config + lazy proxy spawn)
│   ├── routes.rs        axum Router with all HTTP handlers
│   └── shutdown.rs      Ctrl-C / SIGTERM handling
└── tests/
    ├── echo_fixture/    tiny Rust binary used as a fake MCP server
    └── integration.rs   end-to-end SSE round-trip
```

If you add a new file, put it next to the file most closely related to it and
add the `mod` declaration in `lib.rs`. **Do not** introduce a `modules/` or
`utils/` dumping ground.

## Architecture in one diagram

```
HTTP client ──▶ axum routes ──▶ AppState
                                  │
                                  ▼ get_or_spawn(id)
                            ┌──ChildProxy──┐
                            │ IdMux        │   send_from_session
                            │ SessionReg   │ ──────────────────────▶ ChildStdin
                            └──────────────┘
                                  ▲                                  │
                                  │ classify_outbound                ▼
                            ChildStdout ◀── tokio::process::Child ◀──┘
```

Key invariants:

1. **One child per slug, shared across sessions.** Multiple SSE clients to the
   same `/mcp-brave/sse` URL all talk to the same `mcp-brave` subprocess.
2. **JSON-RPC `id` is rewritten in both directions.** The proxy never lets two
   sessions see each other's ids; the child sees a single monotonic id stream.
3. **Server-initiated messages are broadcast.** Notifications (no `id`) and
   server-originated requests go to every attached session of that child.
4. **Lazy spawn, kill-on-drop.** Children are spawned on first `/sse` hit and
   killed when the `tokio::process::Child` is dropped (`kill_on_drop(true)`).

## Build & test commands

Run from `mcp-server/` unless noted.

```bash
# Build the main binary
cargo build
cargo build --release

# Build the test fixture (required before running tests that touch a child)
cd tests/echo_fixture && cargo build && cd ../..

# Run all tests (unit + integration)
cargo test

# Run just one module's unit tests
cargo test jsonrpc::

# Run the binary against a config
cargo run --bin mcp-server -- --config config.example.toml

# Lint and format
cargo clippy --all-targets -- -D warnings
cargo fmt --all
```

CI gate (recommended): `cargo fmt --check && cargo clippy -- -D warnings && cargo test`.

## Conventions

- **Errors:** Library code returns `crate::error::Result<T>`. Use `?`. Convert
  `std::io::Error` and `serde_json::Error` via the `#[from]` impls already on
  `ProxyError`. Use `anyhow::Result` only in `main.rs`.
- **Logging:** Prefer `tracing::info!`/`warn!`/`debug!` with structured fields
  (`server = %id`, `session = %sid`). Never `println!` outside of `main.rs`.
- **Channels:** Use `tokio::sync::mpsc` with `buffer = 64` unless profiling
  shows otherwise. Use `parking_lot::Mutex` for non-async shared state
  (registries, id maps); never hold a `parking_lot` lock across `.await`.
- **Strings on the wire:** Lines passing stdin/stdout are kept as `String` —
  do not pre-parse them in the writer/reader tasks; classification happens in
  `proxy.rs`.
- **Public API surface:** keep `lib.rs` re-exports tight. The integration test
  is the only external consumer of the library API; production users only see
  the CLI.
- **Slug validation:** `^[A-Za-z0-9][A-Za-z0-9_-]*$`. Any new code that
  accepts a slug must route through `config::validate_slug`.

## Things to be careful with

- **Don't hold the proxy `Mutex` across an async spawn.** `AppState::get_or_spawn`
  releases the lock between the cache check and the spawn — preserve that
  shape. If two callers race, a second insert overwrites the first; mitigate
  later by switching to `tokio::sync::OnceCell` per slug if needed.
- **Windows process termination.** `kill_on_drop(true)` works on Windows for
  the immediate child, but children-of-children survive. If we ever spawn
  shells, switch to a job object via `windows-rs`.
- **SSE keep-alive interval is 15s.** Anything longer risks proxies (corporate
  HTTP intermediaries) dropping the stream. Don't raise it without testing.
- **Don't add a streamable-HTTP transport in v1.** PLAN.md scopes that out.
  If a request comes in, link to the plan and open a discussion first.
- **Don't add auth to v1.** Bind defaults to `127.0.0.1`. If you change the
  default to `0.0.0.0`, the change needs explicit user approval.

## How to extend safely

| Want to add…                   | Touch…                                  | Don't forget…                                  |
| ------------------------------ | --------------------------------------- | ---------------------------------------------- |
| A new endpoint                 | `routes.rs`                             | Unit test with `Router::oneshot`               |
| A new config field             | `config.rs` (`McpEntry` or `ServerSection`) | Update `config.example.toml` and README        |
| Per-server idle shutdown       | `proxy.rs` + `state.rs`                 | New unit test that verifies child is killed    |
| A non-stdio transport upstream | New module, `Box<dyn Transport>` trait  | Discuss in PLAN.md before coding               |
| A metric / health detail       | `routes.rs::list_servers`               | Cheap reads only — no locking child state      |

## Test fixtures

The `tests/echo_fixture/` crate builds a standalone binary that reads
newline-delimited JSON-RPC from stdin and echoes the params back as a result.
Several unit tests and the integration test reference its path via
`CARGO_MANIFEST_DIR`. If you change the fixture, the existing tests must
still pass — keep the echo contract stable.

To rebuild the fixture: `cd tests/echo_fixture && cargo build`.

## Working with the parent project

`mcp-server/` lives inside the [`callmodel`](../) repo but is currently a
**standalone** Cargo crate (not a workspace member). That's deliberate: it
lets the proxy be built and shipped independently of the egui app. If you
need to convert to a workspace, plan the change separately — it touches
`callmodel/Cargo.toml`, `target/` layouts, and CI.

## Quick reference for common asks

- **"Add a server to my running proxy"** — not supported in v1; edit
  `config.toml` and restart.
- **"Why isn't `/mcp-brave/sse` responding?"** — check that the slug exists in
  the config (`curl http://localhost:9000/` lists registered ids), then check
  `tracing` output for spawn errors. The child stderr is forwarded to
  `tracing::debug!`.
- **"Can I run this from `callmodel`'s GUI?"** — out of scope for v1. The
  binary is launchable as a sidecar process from anywhere, including the
  llama-server-style process manager already in `callmodel`.

---

When in doubt, read `PLAN.md` and follow the existing task patterns: write
the failing test first, write the smallest passing implementation, commit
in small steps.
