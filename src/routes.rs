use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
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

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(list_servers))
        .route("/health", get(health))
        .route("/{id}/sse", get(open_sse))
        .route("/{id}/messages", post(post_message))
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
            command: "true".into(), args: vec![], env: HashMap::new(), cwd: None,
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
