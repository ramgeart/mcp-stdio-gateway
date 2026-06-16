use std::path::PathBuf;

use clap::Parser;
use mcp_server::{config, routes, shutdown, state::AppState};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "mcp-server", about = "Local HTTP+SSE proxy for stdio MCP servers")]
struct Cli {
    /// Path to the TOML config file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Override the bind host from config
    #[arg(long)]
    host: Option<String>,

    /// Override the bind port from config
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,mcp_server=debug")))
        .init();

    let cli = Cli::parse();
    let mut cfg = config::load_from_path(&cli.config)?;
    if let Some(h) = cli.host { cfg.server.host = h; }
    if let Some(p) = cli.port { cfg.server.port = p; }

    let bind_addr = format!("{}:{}", cfg.server.host, cfg.server.port);
    tracing::info!("loaded {} server(s) from {}", cfg.mcp.len(), cli.config.display());
    for id in cfg.mcp.keys() {
        tracing::info!("  /{id}/sse → command `{}`", cfg.mcp[id].command);
    }

    let state = AppState::new(cfg);
    let app = routes::router(state).layer(tower_http::trace::TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("mcp-server listening on http://{}", bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::ctrl_c())
        .await?;

    Ok(())
}
