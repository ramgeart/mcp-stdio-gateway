use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::Stream;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tower_http::cors::{Any, CorsLayer};

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers(Any)
        .allow_origin(Any)
        .expose_headers(Any);

    Router::new()
        .route("/", get(list_servers))
        .route("/health", get(health))
        .route("/ui", get(serve_ui))
        .route("/api/config", get(get_api_config))
        .route("/api/servers/{id}", post(post_api_server).delete(delete_api_server))
        .route("/api/reload", post(post_api_reload))
        .route("/{id}/sse", get(open_sse))
        .route("/{id}/messages", post(post_message))
        .route("/{id}/mcp", get(get_mcp_sse).post(post_mcp_message).delete(delete_mcp_session))
        .layer(cors)
        .with_state(state)
}

async fn health() -> &'static str { "ok" }

async fn list_servers(State(state): State<AppState>) -> Json<serde_json::Value> {
    let ids = state.ids();
    let live = state.live_ids();
    Json(json!({
        "servers": ids.iter().map(|id| json!({
            "id": id,
            "sse": format!("/{}/sse", id),
            "messages": format!("/{}/messages", id),
            "live": live.contains(id),
        })).collect::<Vec<_>>()
    }))
}

#[derive(Debug, Deserialize)]
struct MessagesQuery { #[serde(rename = "sessionId")] session_id: String }

async fn open_sse(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, std::convert::Infallible>>>, (StatusCode, String)> {
    let proxy = state.get_or_spawn(&id).await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    let session_id = uuid::Uuid::new_v4().to_string();

    let (tx, rx) = mpsc::channel::<String>(64);
    proxy.register_session(session_id.clone(), tx);

    let endpoint_url = format!("/{}/messages?sessionId={}", id, session_id);
    let endpoint_event = Event::default().event("endpoint").data(endpoint_url);

    let proxy_for_cleanup = proxy.clone();
    let session_id_for_cleanup = session_id.clone();

    let stream = async_stream::stream! {
        yield Ok(endpoint_event);
        let mut rx = ReceiverStream::new(rx);
        while let Some(line) = rx.next().await {
            yield Ok(Event::default().event("message").data(line));
        }
        proxy_for_cleanup.drop_session(&session_id_for_cleanup);
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

async fn post_message(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<MessagesQuery>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let proxy = match state.get_or_spawn(&id).await {
        Ok(p) => p,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };
    if let Err(e) = proxy.send_from_session(&q.session_id, body).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    StatusCode::ACCEPTED.into_response()
}

async fn post_mcp_message(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let session_id_opt = headers.get("mcp-session-id")
        .or_else(|| headers.get("MCP-Session-Id"))
        .and_then(|val| val.to_str().ok().map(|s| s.to_string()));

    if let Some(session_id) = session_id_opt {
        let proxy = match state.get_or_spawn(&id).await {
            Ok(p) => p,
            Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
        };

        if !proxy.has_active_session(&session_id) {
            return (StatusCode::NOT_FOUND, "Session expired or invalid".to_string()).into_response();
        }

        let has_id = body.get("id").is_some_and(|val| !val.is_null());
        let has_method = body.get("method").is_some();

        if has_id && has_method {
            match proxy.send_request_from_session(&session_id, body).await {
                Ok(res) => (StatusCode::OK, Json(res)).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        } else {
            if let Err(e) = proxy.send_from_session(&session_id, body).await {
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
            StatusCode::ACCEPTED.into_response()
        }
    } else {
        let is_initialize = body.get("method").and_then(|m| m.as_str()) == Some("initialize");
        if !is_initialize {
            return (StatusCode::BAD_REQUEST, "Missing or invalid session ID for non-initialize request".to_string()).into_response();
        }

        let proxy = match state.get_or_spawn(&id).await {
            Ok(p) => p,
            Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
        };

        let new_session_id = uuid::Uuid::new_v4().to_string();
        proxy.add_active_session(new_session_id.clone());

        match proxy.send_request_from_session(&new_session_id, body).await {
            Ok(res) => {
                let mut resp_headers = HeaderMap::new();
                if let Ok(hv) = new_session_id.parse() {
                    resp_headers.insert("mcp-session-id", hv);
                }
                if let Ok(hv) = new_session_id.parse() {
                    resp_headers.insert("MCP-Session-Id", hv);
                }
                resp_headers.insert("Access-Control-Expose-Headers", "mcp-session-id, MCP-Session-Id".parse().unwrap());
                (StatusCode::OK, resp_headers, Json(res)).into_response()
            }
            Err(e) => {
                proxy.drop_active_session(&new_session_id);
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}

async fn get_mcp_sse(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session_id = match headers.get("mcp-session-id")
        .or_else(|| headers.get("MCP-Session-Id"))
        .and_then(|val| val.to_str().ok().map(|s| s.to_string())) {
        Some(sid) => sid,
        None => return (StatusCode::BAD_REQUEST, "Missing mcp-session-id header".to_string()).into_response(),
    };

    let proxy = match state.get_or_spawn(&id).await {
        Ok(p) => p,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };

    if !proxy.has_active_session(&session_id) {
        return (StatusCode::NOT_FOUND, "Session expired or invalid".to_string()).into_response();
    }

    let (tx, rx) = mpsc::channel::<String>(64);
    proxy.register_session(session_id.clone(), tx);

    let proxy_for_cleanup = proxy.clone();
    let session_id_for_cleanup = session_id.clone();

    let stream = async_stream::stream! {
        let mut rx = ReceiverStream::new(rx);
        while let Some(line) = rx.next().await {
            yield Ok::<Event, std::convert::Infallible>(Event::default().event("message").data(line));
        }
        proxy_for_cleanup.drop_session(&session_id_for_cleanup);
        proxy_for_cleanup.drop_active_session(&session_id_for_cleanup);
    };

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))).into_response()
}

async fn delete_mcp_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session_id = match headers.get("mcp-session-id")
        .or_else(|| headers.get("MCP-Session-Id"))
        .and_then(|val| val.to_str().ok().map(|s| s.to_string())) {
        Some(sid) => sid,
        None => return (StatusCode::BAD_REQUEST, "Missing mcp-session-id header".to_string()).into_response(),
    };

    let proxy = match state.get_or_spawn(&id).await {
        Ok(p) => p,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };

    proxy.drop_session(&session_id);
    proxy.drop_active_session(&session_id);

    StatusCode::OK.into_response()
}

#[derive(Debug, Deserialize)]
struct AddServerPayload {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: std::collections::HashMap<String, String>,
    #[serde(default)]
    cwd: Option<String>,
}

async fn get_api_config(State(state): State<AppState>) -> Json<serde_json::Value> {
    let config = state.config.lock();
    let live = state.live_ids();
    let servers = config.mcp.iter().map(|(id, entry)| {
        json!({
            "id": id,
            "command": entry.command,
            "args": entry.args,
            "env": entry.env,
            "cwd": entry.cwd,
            "is_http": entry.is_http,
            "live": live.contains(id),
        })
    }).collect::<Vec<_>>();
    Json(json!({ "servers": servers }))
}

async fn post_api_server(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<AddServerPayload>,
) -> impl IntoResponse {
    if let Err(e) = crate::config::validate_slug(&id) {
        return (StatusCode::BAD_REQUEST, format!("Invalid slug: {}", e)).into_response();
    }
    let is_http = payload.command.starts_with("http://") || payload.command.starts_with("https://");
    let entry = crate::config::McpEntry {
        command: payload.command,
        args: payload.args,
        env: payload.env,
        cwd: payload.cwd,
        is_http,
    };
    match state.add_server(id, entry) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_api_server(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if state.disable_server(&id).await {
        StatusCode::OK.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn post_api_reload(State(state): State<AppState>) -> impl IntoResponse {
    match state.reload_from_disk().await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn serve_ui() -> impl IntoResponse {
    let html = r#"<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MCP gateway Admin Control Panel</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        background: '#0d1117',
                        cardBg: '#161b22',
                        cardHover: '#1f242c',
                        borderClr: '#30363d',
                        txtMuted: '#8b949e',
                        accentBlue: '#58a6ff',
                        accentPurple: '#a371f7',
                        accentGreen: '#3fb950',
                        accentRed: '#ff7b72'
                    }
                }
            }
        }
    </script>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Plus Jakarta Sans', sans-serif; }
        ::-webkit-scrollbar { width: 8px; }
        ::-webkit-scrollbar-track { background: #0d1117; }
        ::-webkit-scrollbar-thumb { background: #30363d; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #8b949e; }
    </style>
</head>
<body class="bg-background text-slate-100 min-h-screen flex flex-col antialiased">
    <!-- Header Row -->
    <header class="border-b border-borderClr py-4 px-6 md:px-12 flex flex-col md:flex-row items-start md:items-center justify-between gap-4 sticky top-0 bg-background/95 backdrop-blur z-20">
        <div class="flex items-center gap-3">
            <div class="bg-purple-900/40 p-2 rounded-lg border border-purple-500/30">
                <!-- Stacked plug logo/icon for MCP -->
                <svg class="h-6 w-6 text-accentPurple" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" stroke-dasharray="100" stroke-dashoffset="0"/>
                </svg>
            </div>
            <div>
                <h1 class="text-xl md:text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                    MCP Servers Gateway
                    <span class="text-xs bg-slate-800 text-accentBlue px-2.5 py-0.5 rounded-full border border-slate-700">v0.1.0</span>
                </h1>
                <p class="text-xs text-txtMuted mt-0.5">Multiplex local stdio and streamable HTTP Model Context Protocol servers</p>
            </div>
        </div>
        
        <div class="flex items-center gap-3 w-full md:w-auto">
            <button onclick="triggerReload()" class="flex items-center justify-center gap-2 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-borderClr text-sm font-medium px-4 py-2 rounded-lg transition" title="Reload settings from config.toml on disk">
                <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 1121.21 8H18" />
                </svg>
                Reload config
            </button>
            <button onclick="openAddModal()" class="flex items-center justify-center gap-2 bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-500 hover:to-indigo-500 text-white text-sm font-semibold px-4 py-2 rounded-lg shadow-lg shadow-indigo-900/40 transition">
                <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M12 4v16m8-8H4" />
                </svg>
                Add New Server
            </button>
        </div>
    </header>

    <!-- Main Workspace Container -->
    <main class="flex-1 py-8 px-6 md:px-12 max-w-7xl mx-auto w-full flex flex-col gap-8">
        <!-- Live status bar -->
        <div id="no-servers-alert" class="hidden bg-slate-900/60 rounded-xl border border-dashed border-borderClr p-8 text-center flex flex-col items-center justify-center gap-3">
            <svg class="h-12 w-12 text-txtMuted" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                <path stroke-linecap="round" stroke-linejoin="round" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
            </svg>
            <div>
                <p class="text-base font-semibold text-slate-200">No MCP Servers Configured Yet</p>
                <p class="text-sm text-txtMuted mt-1">Add a local stdio command-based tool or set up a custom Streamable HTTP remote server below.</p>
            </div>
            <button onclick="openAddModal()" class="mt-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-semibold px-4.5 py-1.5 rounded-lg border border-borderClr transition">Add First Server</button>
        </div>

        <!-- Servers Cards Grid -->
        <div id="grid-container" class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <!-- Dynamic Server Cards injected here -->
        </div>

        <!-- Documentation & Help Command Sheet -->
        <section class="mt-8 border-t border-borderClr pt-8">
            <h2 class="text-lg font-bold text-white flex items-center gap-2">
                <svg class="h-5 w-5 text-accentBlue" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                </svg>
                Gateway Command References (/mcp Console shortcuts)
            </h2>
            <p class="text-xs text-txtMuted mt-1">Input the following control sequences in the running gateway terminal window to fast-manage active processes.</p>
            
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
                <div class="bg-cardBg rounded-xl border border-borderClr p-4">
                    <p class="text-sm font-semibold text-slate-200">/mcp add &lt;id&gt; &lt;cmd&gt; [&lt;args&gt;]</p>
                    <p class="text-xs text-txtMuted mt-1.5">Dynamically load or update a server. Supports quotes/commas for args, plus checks for URLs to proxy over stateless streamable HTTP requests automatically.</p>
                </div>
                <div class="bg-cardBg rounded-xl border border-borderClr p-4">
                    <p class="text-sm font-semibold text-slate-200">/mcp disable &lt;id&gt;</p>
                    <p class="text-xs text-txtMuted mt-1.5">Disable, shut down, and purge existing proxy allocations. Removes the server from memory and immediately stops the associated background child process.</p>
                </div>
                <div class="bg-cardBg rounded-xl border border-borderClr p-4">
                    <p class="text-sm font-semibold text-slate-200">/mcp server reload</p>
                    <p class="text-xs text-txtMuted mt-1.5">Triggers hot configuration parsing. Reads the on-disk TOML again and recycles active proxies if settings differ or have been deleted.</p>
                </div>
            </div>
        </section>
    </main>

    <!-- Footer -->
    <footer class="border-t border-borderClr py-4 px-12 text-center text-xs text-txtMuted flex items-center justify-between mt-auto">
        <p>mcp-stdio-gateway &bull; Designed to empower local environments</p>
        <p class="flex items-center gap-1.5">
            <span class="h-2 w-2 rounded-full bg-accentGreen shrink-0 inline-block animate-pulse"></span>
            Proxy engine online
        </p>
    </footer>

    <!-- ADD/EDIT SERVICE MODAL -->
    <div id="add-modal" class="hidden fixed inset-0 bg-background/80 backdrop-blur-sm flex items-center justify-center p-4 z-50 transition-all duration-300">
        <div class="bg-cardBg border border-borderClr rounded-2xl w-full max-w-lg shadow-2xl p-6 relative">
            <button onclick="closeAddModal()" class="absolute top-4 right-4 text-txtMuted hover:text-white transition">
                <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" />
                </svg>
            </button>
            
            <h3 class="text-lg font-bold text-white mb-1 flex items-center gap-2">
                <svg class="h-5 w-5 text-accentBlue" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                    <path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                </svg>
                Configure MCP Server
            </h3>
            <p class="text-xs text-txtMuted mb-4">Set up command paths for local stdio tools or target HTTPS URLs for Streamable HTTP endpoints which will be persisted immediately in config.toml.</p>
            
            <form id="add-form" onsubmit="event.preventDefault(); submitServerForm();">
                <div class="flex flex-col gap-4">
                    <div>
                        <label class="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5">Server Name / ID (Slug)</label>
                        <input id="input-slug" type="text" placeholder="e.g. mcp-filesystem" required class="w-full bg-slate-900 border border-borderClr rounded-lg px-3 py-2 text-sm text-slate-100 placeholder:text-slate-600 focus:outline-none focus:border-accentPurple transition">
                        <p class="text-[10px] text-txtMuted mt-1">Must be alphanumeric and use hyphens/underscores only: ^[A-Za-z0-9][A-Za-z0-9_-]*$</p>
                    </div>

                    <div>
                        <label class="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5">Command, File path, or Endpoint URL</label>
                        <input id="input-command" type="text" placeholder="e.g. npx or https://mcp.serpapi.com/.../mcp" required class="w-full bg-slate-900 border border-borderClr rounded-lg px-3 py-2 text-sm text-slate-100 placeholder:text-slate-600 focus:outline-none focus:border-accentBlue transition">
                    </div>

                    <div>
                        <label class="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5">Arguments (Comma or Space separated)</label>
                        <input id="input-args" type="text" placeholder="e.g. -y, @modelcontextprotocol/server-filesystem, C:/temp" class="w-full bg-slate-900 border border-borderClr rounded-lg px-3 py-2 text-sm text-slate-100 placeholder:text-slate-600 focus:outline-none focus:border-accentBlue transition">
                    </div>

                    <div>
                        <label class="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5">Working Directory (CWD, Optional)</label>
                        <input id="input-cwd" type="text" placeholder="e.g. C:/Projects (leave empty for default)" class="w-full bg-slate-900 border border-borderClr rounded-lg px-3 py-2 text-sm text-slate-100 placeholder:text-slate-600 focus:outline-none focus:border-accentBlue transition">
                    </div>
                </div>

                <div class="flex justify-end gap-3 mt-6 border-t border-borderClr pt-4">
                    <button type="button" onclick="closeAddModal()" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 border border-borderClr text-sm font-medium rounded-lg text-slate-200 transition">Cancel</button>
                    <button type="submit" class="px-5 py-2 bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-500 hover:to-indigo-500 text-sm font-semibold rounded-lg text-white shadow-md transition">Save &amp; Active</button>
                </div>
            </form>
        </div>
    </div>

    <!-- TOAST OVERLAYS -->
    <div id="toast-bin" class="fixed bottom-6 right-6 flex flex-col gap-3 z-50"></div>

    <!-- FRONT-END CLIENT CONTROLLER SCRIPT -->
    <script>
        async function fetchConfig() {
            try {
                const response = await fetch('/api/config');
                const data = await response.json();
                renderCards(data.servers || []);
            } catch (err) {
                showToast('Error syncing configured MCP tools list: ' + err.message, 'error');
            }
        }

        function renderCards(servers) {
            const container = document.getElementById('grid-container');
            const alertBox = document.getElementById('no-servers-alert');
            container.innerHTML = '';

            if (servers.length === 0) {
                alertBox.classList.remove('hidden');
                return;
            }
            alertBox.classList.add('hidden');

            servers.forEach(server => {
                const card = document.createElement('div');
                card.className = 'bg-cardBg border border-borderClr rounded-2xl p-5 hover:border-slate-500 transition duration-300 flex flex-col gap-4 relative justify-between';
                
                // Set badge colors
                const liveBadge = server.live 
                    ? '<span class="inline-flex items-center gap-1 text-[10px] uppercase font-bold bg-emerald-950 text-accentGreen px-2.5 py-0.5 rounded-full border border-emerald-800/40">Active</span>' 
                    : '<span class="inline-flex items-center gap-1 text-[10px] uppercase font-bold bg-slate-800 text-txtMuted px-2.5 py-0.5 rounded-full border border-slate-700">Inactive</span>';
                
                const typeBadge = server.is_http
                    ? '<span class="inline-flex items-center gap-1 text-[10px] uppercase font-bold bg-sky-950 text-accentBlue px-2.5 py-0.5 rounded-full border border-sky-800/40">HTTP Streamable</span>'
                    : '<span class="inline-flex items-center gap-1 text-[10px] uppercase font-bold bg-violet-950 text-accentPurple px-2.5 py-0.5 rounded-full border border-violet-800/40">Local Stdio</span>';

                const argsText = server.args && server.args.length > 0
                    ? `<code class="block text-xs bg-slate-900 border border-borderClr px-2.5 py-2 rounded-lg text-slate-300 mt-2 whitespace-pre-wrap font-mono">${server.args.join(', ')}</code>`
                    : '<span class="text-xs text-slate-600 block mt-1">(No arguments specified)</span>';

                const cwdSection = server.cwd
                    ? `<div class="mt-2 text-[11px] text-txtMuted font-mono">CWD: <span class="text-slate-300">${server.cwd}</span></div>`
                    : '';

                card.innerHTML = `
                    <div>
                        <div class="flex items-start justify-between gap-4">
                            <div class="flex items-center gap-2">
                                <h3 class="text-lg font-bold text-white tracking-tight">${server.id}</h3>
                            </div>
                            <div class="flex items-center gap-1.5">
                                ${typeBadge}
                                ${liveBadge}
                            </div>
                        </div>

                        <div class="mt-3">
                            <p class="text-xs font-semibold text-slate-400 uppercase tracking-widest">Target Command / URL</p>
                            <span class="text-sm font-mono text-slate-200 bg-slate-900 border border-borderClr px-2 py-1.5 rounded-lg block mt-1 overflow-x-auto">${server.command}</span>
                        </div>

                        <div class="mt-3">
                            <p class="text-xs font-semibold text-slate-400 uppercase tracking-widest">Arguments</p>
                            ${argsText}
                        </div>
                        
                        ${cwdSection}
                    </div>

                    <div class="flex items-center justify-between border-t border-borderClr pt-4 mt-2">
                        <!-- Left-aligned toggle or generic actions -->
                        <div class="text-[10px] text-txtMuted">
                            ${server.is_http ? 'Routes messages via POST requests' : 'Spawns command shell on sse connection'}
                        </div>

                        <!-- Right-aligned action layout -->
                        <div class="flex items-center gap-2">
                            <button onclick="editServer('${server.id}', '${encodeURIComponent(server.command)}', '${encodeURIComponent(server.args ? server.args.join(', ') : '')}', '${encodeURIComponent(server.cwd || '')}')" class="p-2 border border-borderClr bg-slate-900 hover:border-accentBlue text-slate-300 rounded-lg hover:text-accentBlue transition" title="Modify Server Config">
                                <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                    <path stroke-linecap="round" stroke-linejoin="round" d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z" />
                                </svg>
                            </button>
                            <button onclick="disableServer('${server.id}')" class="p-2 border border-borderClr bg-slate-900 hover:border-accentRed text-slate-300 rounded-lg hover:text-accentRed transition" title="Stop &amp; Unregister Server">
                                <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                    <path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                                </svg>
                            </button>
                        </div>
                    </div>
                `;
                
                container.appendChild(card);
            });
        }

        function openAddModal() {
            document.getElementById('input-slug').value = '';
            document.getElementById('input-slug').disabled = false;
            document.getElementById('input-command').value = '';
            document.getElementById('input-args').value = '';
            document.getElementById('input-cwd').value = '';
            document.getElementById('add-modal').classList.remove('hidden');
        }

        function editServer(slug, cmd, args, cwd) {
            document.getElementById('input-slug').value = slug;
            document.getElementById('input-slug').disabled = true; // disable key rename
            document.getElementById('input-command').value = decodeURIComponent(cmd);
            document.getElementById('input-args').value = decodeURIComponent(args);
            document.getElementById('input-cwd').value = decodeURIComponent(cwd) === 'undefined' ? '' : decodeURIComponent(cwd);
            document.getElementById('add-modal').classList.remove('hidden');
        }

        function closeAddModal() {
            document.getElementById('add-modal').classList.add('hidden');
        }

        async function submitServerForm() {
            const slug = document.getElementById('input-slug').value.trim();
            const command = document.getElementById('input-command').value.trim();
            const argsRaw = document.getElementById('input-args').value;
            const cwdRaw = document.getElementById('input-cwd').value.trim();

            // Simple logic parsing comma separated args
            const args = argsRaw.split(',').map(s => s.trim()).filter(s => s.length > 0);
            const cwd = cwdRaw.length > 0 ? cwdRaw : null;

            try {
                const response = await fetch(`/api/servers/${slug}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ command, args, env: {}, cwd })
                });

                if (response.ok) {
                    showToast(`Success: Tool "${slug}" updated and file configuration stored successfully.`, 'success');
                    closeAddModal();
                    fetchConfig();
                } else {
                    const text = await response.text();
                    showToast('Failed to save settings: ' + text, 'error');
                }
            } catch (err) {
                showToast('Error sending server configurations: ' + err.message, 'error');
            }
        }

        async function disableServer(slug) {
            if (!confirm(`Are you absolutely sure you want to stop and disable "${slug}" server configurations?`)) {
                return;
            }
            try {
                const response = await fetch(`/api/servers/${slug}`, { method: 'DELETE' });
                if (response.ok) {
                    showToast(`Success: Server "${slug}" deleted and configs updated successfully!`, 'success');
                    fetchConfig();
                } else {
                    showToast(`Failed to unregister ${slug}`, 'error');
                }
            } catch (err) {
                showToast('Network error omitting tool: ' + err.message, 'error');
            }
        }

        async function triggerReload() {
            try {
                const response = await fetch('/api/reload', { method: 'POST' });
                if (response.ok) {
                    showToast('Success: Configuration list successfully hot-reloaded from disk config.toml!', 'success');
                    fetchConfig();
                } else {
                    const text = await response.text();
                    showToast('Disk reload failed: ' + text, 'error');
                }
            } catch (err) {
                showToast('Reload communication failed: ' + err.message, 'error');
            }
        }

        function showToast(message, type = 'success') {
            const bin = document.getElementById('toast-bin');
            
            const toast = document.createElement('div');
            toast.className = 'flex items-center gap-2 px-4 py-3 bg-slate-900 border text-sm font-medium rounded-xl shadow-2xl min-w-[280px] max-w-sm transition-all duration-300 toast-enter';
            
            if (type === 'success') {
                toast.classList.add('border-emerald-500/30', 'text-emerald-300');
                toast.innerHTML = `
                    <svg class="h-4 w-4 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    <span>${message}</span>
                `;
            } else {
                toast.classList.add('border-red-500/30', 'text-red-300');
                toast.innerHTML = `
                    <svg class="h-4 w-4 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                    </svg>
                    <span>${message}</span>
                `;
            }

            bin.appendChild(toast);
            
            // Trigger animation frame
            setTimeout(() => {
                toast.classList.remove('toast-enter');
                toast.classList.add('toast-active');
            }, 10);

            // Remove toast after duration
            setTimeout(() => {
                toast.style.opacity = '0';
                toast.style.transform = 'translateY(-10px)';
                setTimeout(() => {
                    toast.remove();
                }, 300);
            }, 4000);
        }

        // Boot-load settings configs
        fetchConfig();
    </script>
</body>
</html>
"#;
    axum::response::Html(html)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, McpEntry, ServerSection};
    use axum::body::Body;
    use axum::http::Request;
    use std::collections::HashMap;
    use tower::ServiceExt;

    fn empty_state() -> AppState {
        AppState::new(Config {
            server: ServerSection::default(),
            mcp: HashMap::new(),
        })
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let app = router(empty_state());
        let resp = app.oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_returns_configured_ids() {
        let mut mcp = HashMap::new();
        mcp.insert("mcp-foo".into(), McpEntry {
            command: "true".into(), args: vec![], env: HashMap::new(), cwd: None, is_http: false,
        });
        let state = AppState::new(Config { server: ServerSection::default(), mcp });
        let app = router(state);

        let resp = app.oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["servers"][0]["id"], serde_json::json!("mcp-foo"));
        assert_eq!(v["servers"][0]["sse"], serde_json::json!("/mcp-foo/sse"));
    }

    #[tokio::test]
    async fn unknown_id_returns_404_on_sse() {
        let app = router(empty_state());
        let resp = app.oneshot(Request::builder().uri("/nope/sse").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
