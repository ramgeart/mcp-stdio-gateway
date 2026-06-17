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

    let state = AppState::new(cfg).with_path(cli.config.clone());
    
    // Spawn interactive TUI REPL in background
    spawn_tui(state.clone());

    let app = routes::router(state).layer(tower_http::trace::TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("mcp-server listening on http://{}", bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::ctrl_c())
        .await?;

    Ok(())
}

fn spawn_tui(state: AppState) {
    tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin).lines();

        println!("\n========================================================");
        println!("       MCP-SERVER INTERACTIVE POWER CONSOLE / TUI       ");
        println!("========================================================");
        println!("Type /help or /mcp help to view all available commands.");
        println!("Type /list to show status of configured stdio tools.");
        println!("========================================================\n");
        
        print_prompt();

        while let Ok(Some(line)) = reader.next_line().await {
            let line = line.trim();
            if !line.is_empty() {
                handle_command(line, &state).await;
            }
            print_prompt();
        }
    });
}

fn print_prompt() {
    use std::io::Write;
    print!("mcp-server> ");
    let _ = std::io::stdout().flush();
}

async fn handle_command(input: &str, state: &AppState) {
    let input = input.trim();
    if input == "/help" || input == "/mcp help" || input == "help" {
        print_help();
        return;
    }

    if input == "/mcp server reload" || input == "/server reload" {
        match state.reload_from_disk().await {
            Ok(_) => println!("Success: configuration reloaded from disk."),
            Err(e) => println!("Error reloading config: {}", e),
        }
        return;
    }

    if let Some(stripped) = input.strip_prefix("/mcp disable") {
        let rest = stripped.trim();
        let name = if rest.starts_with('"') && rest.ends_with('"') && rest.len() >= 2 {
            &rest[1..rest.len() - 1]
        } else {
            rest
        };

        if name.is_empty() {
            println!("Error: please specify a tool name to disable, e.g.: /mcp disable \"mcp-filesystem\"");
            return;
        }

        if state.disable_server(name).await {
            println!("Success: tool \"{}\" has been disabled and stopped successfully.", name);
        } else {
            println!("Error: tool \"{}\" not found in active configurations.", name);
        }
        return;
    }

    if let Some(stripped) = input.strip_prefix("/mcp add") {
        let rest = stripped.trim();
        let (name, after_name) = if let Some(stripped_quote) = rest.strip_prefix('"') {
            if let Some(idx) = stripped_quote.find('"') {
                (stripped_quote[..idx].trim(), stripped_quote[idx + 1..].trim())
            } else {
                println!("Error: unclosed double quotes in tool name.");
                return;
            }
        } else {
            let first_space = rest.find(' ');
            if let Some(idx) = first_space {
                (rest[..idx].trim(), rest[idx..].trim())
            } else {
                (rest.trim(), "")
            }
        };

        if name.is_empty() {
            println!("Error: please specify a tool name, e.g.: /mcp add \"mcp-filesystem\" npx \"args\"");
            return;
        }

        // Parse command and args
        let first_space = after_name.find(' ');
        let (cmd, args_part) = if let Some(idx) = first_space {
            (after_name[..idx].trim(), after_name[idx..].trim())
        } else {
            (after_name.trim(), "")
        };

        if cmd.is_empty() {
            println!("Error: missing command for tool \"{}\". Usage: /mcp add \"tool_name\" <command> [args]", name);
            return;
        }

        let args_cleaned = args_part
            .trim_start_matches('<').trim_end_matches('>')
            .trim_start_matches('[').trim_end_matches(']')
            .trim();

        let mut args = Vec::new();
        let mut chars = args_cleaned.chars().peekable();
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() || c == ',' {
                chars.next();
                continue;
            }
            if c == '"' {
                chars.next();
                let mut arg = String::new();
                let mut escaped = false;
                for nc in chars.by_ref() {
                    if escaped {
                        arg.push(nc);
                        escaped = false;
                    } else if nc == '\\' {
                        escaped = true;
                    } else if nc == '"' {
                        break;
                    } else {
                        arg.push(nc);
                    }
                }
                args.push(arg);
            } else {
                let mut arg = String::new();
                while let Some(&nc) = chars.peek() {
                    if nc.is_whitespace() || nc == ',' || nc == '"' {
                        break;
                    }
                    arg.push(chars.next().unwrap());
                }
                args.push(arg);
            }
        }

        // Validate the slug name
        if let Err(e) = crate::config::validate_slug(name) {
            println!("Error: invalid tool name '{}': {}", name, e);
            return;
        }

        let is_http = cmd.starts_with("http://") || cmd.starts_with("https://");

        let entry = crate::config::McpEntry {
            command: cmd.to_string(),
            args,
            env: std::collections::HashMap::new(),
            cwd: None,
            is_http,
        };

        match state.add_server(name.to_string(), entry) {
            Ok(_) => {
                if is_http {
                    println!("Success: gateway HTTP/Streamable tool \"{}\" added and saved successfully. Proxying to external: {}", name, cmd);
                } else {
                    println!("Success: tool \"{}\" has been added/updated and saved successfully with command '{}'. Connect to it at http://localhost:9000/{}/sse or /{}/mcp", name, cmd, name, name);
                }
            }
            Err(e) => {
                println!("Error saving configuration change to disk: {}", e);
            }
        }
        return;
    }

    if input == "/list" || input == "/mcp list" || input == "list" {
        print_server_list(state);
        return;
    }

    if input == "/status" || input == "/mcp status" || input == "status" {
        print_status(state);
        return;
    }

    if input == "/exit" || input == "/shutdown" || input == "exit" {
        println!("Shutting down mcp-server gracefully...");
        std::process::exit(0);
    }

    println!("Unknown command: '{}'. Type /help for all available control commands.", input);
}

fn print_help() {
    println!("\n----------------------- AVAILABLE COMMANDS -----------------------");
    println!("  /help                          Show this help documentation section");
    println!("  /list                          List all configured tools with status");
    println!("  /status                        Show active server stats and listener options");
    println!("  /mcp add \"slug\" <cmd> <args>   Add / update an MCP tool process config");
    println!("  /mcp disable \"slug\"            Stop and disable an active MCP tool");
    println!("  /mcp server reload             Reload config file dynamically from disk");
    println!("  /exit, /shutdown               Gracefully exit the proxy server");
    println!("------------------------------------------------------------------\n");
}

fn print_server_list(state: &AppState) {
    let ids = state.ids();
    let live = state.live_ids();
    println!("\nConfigured MCP Tools ({} total):", ids.len());
    if ids.is_empty() {
        println!("  (No tools configured)");
    } else {
        for id in ids {
            let config = state.config.lock();
            let entry = config.mcp.get(&id).unwrap();
            let status_str = if live.contains(&id) { "[ACTIVE]" } else { "[INACTIVE]" };
            println!("  - {:<18} {:<10} command: {} {:?}", id, status_str, entry.command, entry.args);
        }
    }
    println!();
}

fn print_status(state: &AppState) {
    let config = state.config.lock();
    let ids = state.ids();
    let live = state.live_ids();
    println!("\nProxy Status:");
    println!("  - Address:       http://{}:{}", config.server.host, config.server.port);
    println!("  - Total Tools:   {}", ids.len());
    println!("  - Live Proxies:  {}", live.len());
    if !live.is_empty() {
        println!("  - Active Slugs:  {:?}", live);
    }
    println!();
}

