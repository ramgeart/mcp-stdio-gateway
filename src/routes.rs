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
