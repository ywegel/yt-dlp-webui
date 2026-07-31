mod config;
mod download;
mod events;
mod reaper;
mod run_job;
mod submit;

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use axum::routing::post;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::broadcast::Sender;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::download::download;
use crate::events::events;
use crate::reaper::reaper;
use crate::submit::submit;

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
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .compact()
        .init();

    let config = config::Config::load();

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
        limiter: Arc::new(Semaphore::new(config.jobs.max_concurrent)),
        channels: Arc::new(Mutex::new(HashMap::new())),
    };

    tokio::spawn(reaper(st.clone(), config.jobs.reaper_interval_secs));

    let app = Router::new()
        .route("/api/submit", post(submit))
        .route("/api/jobs/{id}/events", get(events))
        .route("/api/jobs/{id}/file", get(download))
        .fallback_service(tower_http::services::ServeDir::new("./serve"))
        .with_state(st);

    let addr = format!("{}:{}", config.server.host, config.server.port);
    let l = tokio::net::TcpListener::bind(&addr).await.unwrap();
    tracing::info!("Listening on http://{}", addr);
    axum::serve(l, app).await.unwrap();
}
