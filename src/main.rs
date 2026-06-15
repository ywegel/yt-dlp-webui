mod download;
mod events;
mod reaper;
mod run_job;
mod submit;

use crate::download::download;
use crate::events::events;
use crate::reaper::reaper;
use crate::submit::submit;
use axum::Router;
use axum::routing::{get, post};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, Semaphore};
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Clone)]
struct AppState {
    db: sqlx::SqlitePool,
    limiter: Arc<Semaphore>,
    channels: Arc<Mutex<HashMap<String, Sender<Progress>>>>,
}

#[derive(Clone, serde::Serialize)]
#[serde(tag = "status")]
enum Progress {
    Running { percent: f32 },
    Done { file: String },
    Failed { error: String },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO) // TODO: Set tracing leve from env
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .compact()
        .init();

    tracing::debug!("yt-dlp-webui starting...");

    let db = sqlx::SqlitePool::connect("sqlite:jobs.db?mode=rwc")
        .await
        .unwrap();
    sqlx::query(
        "
        CREATE TABLE IF NOT EXISTS jobs (
            id              TEXT PRIMARY KEY,
            url             TEXT NOT NULL,
            mode            TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'queued',
            file            TEXT,
            created_at      INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            completed_at    INTEGER,
            last_download_at INTEGER
        )
    ",
    )
    .execute(&db)
    .await
    .unwrap();

    let st = AppState {
        db,
        limiter: Arc::new(Semaphore::new(3)),
        channels: Arc::new(Mutex::new(HashMap::new())),
    };

    tokio::spawn(reaper(st.clone(), 600));

    let app = Router::new()
        .route("/api/submit", post(submit))
        .route("/api/jobs/{id}/events", get(events))
        .route("/api/jobs/{id}/file", get(download))
        .fallback_service(tower_http::services::ServeDir::new("./serve"))
        .with_state(st);

    let l = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!("Listening on http://{}", "0.0.0.0:8080");
    axum::serve(l, app).await.unwrap();
}
