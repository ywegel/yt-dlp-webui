use std::time::Duration;

use axum::body::Body;
use axum::body::BodyDataStream;
use axum::http::Request;
use axum::http::StatusCode;
use axum::response::Response;
use futures::StreamExt;
use sqlx::SqlitePool;
use tokio::sync::watch;
use tower::ServiceExt;

use crate::AppState;
use crate::Progress;
use crate::job_status::JobStatus;

const EVENT_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn insert_job(
    db: &SqlitePool,
    id: &str,
    status: JobStatus,
    file: Option<&str>,
    error: Option<&str>,
) {
    sqlx::query("INSERT INTO jobs (id, url, mode, status, file, error) VALUES (?,?,'video',?,?,?)")
        .bind(id)
        .bind("https://example.invalid/video")
        .bind(status)
        .bind(file)
        .bind(error)
        .execute(db)
        .await
        .unwrap();
}

pub async fn job_status(db: &SqlitePool, id: &str) -> JobStatus {
    sqlx::query_scalar("SELECT status FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_one(db)
        .await
        .unwrap()
}

/// Registers a progress channel the way `spawn_job` does: the map holds a
/// clone, the caller keeps the sender and plays the writer.
pub async fn register_channel(st: &AppState, id: &str) -> watch::Sender<Progress> {
    let (tx, _) = watch::channel(Progress::Queued);
    st.channels.lock().await.insert(id.to_string(), tx.clone());
    tx
}

/// Opens `/api/jobs/{id}/events` through the real router, returning the status
/// code if the response was not a 200.
///
/// By the time this returns, the handler has subscribed to the channel and read
/// the row, but the stream body has not been polled yet, so a test can make the
/// writer finish inside exactly that window.
pub async fn open_events(st: &AppState, id: &str) -> Result<SseClient, StatusCode> {
    let req = Request::builder()
        .uri(format!("/api/jobs/{id}/events"))
        .body(Body::empty())
        .unwrap();

    let resp = crate::app(st.clone()).oneshot(req).await.unwrap();

    match resp.status() {
        StatusCode::OK => Ok(SseClient::new(resp)),
        other => Err(other),
    }
}

/// Reads an SSE response event by event. Never collect the whole body of a live
/// stream: it only ends once the job reaches a terminal state.
pub struct SseClient {
    body: BodyDataStream,
    buf: String,
}

impl SseClient {
    fn new(resp: Response) -> Self {
        let content_type = resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .expect("SSE response must carry a content type");
        assert_eq!(
            content_type, "text/event-stream",
            "EventSource rejects any other content type"
        );

        Self {
            body: resp.into_body().into_data_stream(),
            buf: String::new(),
        }
    }

    /// The next event's `data:` payload, or `None` once the server closed the
    /// stream. Panics if neither happens within [`EVENT_TIMEOUT`].
    pub async fn next_json(&mut self) -> Option<serde_json::Value> {
        tokio::time::timeout(EVENT_TIMEOUT, self.read_event())
            .await
            .expect("SSE stream produced neither an event nor an end")
    }

    async fn read_event(&mut self) -> Option<serde_json::Value> {
        loop {
            if let Some(end) = self.buf.find("\n\n") {
                let block: String = self.buf.drain(..end + 2).collect();
                let data = block
                    .lines()
                    .filter_map(|line| line.strip_prefix("data:"))
                    .map(str::trim_start)
                    .collect::<Vec<_>>()
                    .join("\n");

                // Keep-alive comments carry no data field.
                if data.is_empty() {
                    continue;
                }
                return Some(serde_json::from_str(&data).expect("event payload must be JSON"));
            }

            let chunk = self.body.next().await?.expect("SSE body failed");
            self.buf
                .push_str(std::str::from_utf8(&chunk).expect("SSE body must be UTF-8"));
        }
    }
}
