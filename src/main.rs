mod config;
mod download;
mod events;
mod job_status;
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
use tokio::sync::watch;
use tokio::sync::watch::Sender;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::download::download;
use crate::events::events;
use crate::job_status::JobStatus;
use crate::reaper::reaper;
use crate::run_job::run_job;
use crate::submit::Mode;
use crate::submit::submit;

#[derive(Clone)]
struct AppState {
    db: sqlx::SqlitePool,
    limiter: Arc<Semaphore>,
    channels: Arc<Mutex<HashMap<String, Sender<Progress>>>>,
}

impl AppState {
    /// Registers the progress channel before spawning, so a client asking about
    /// the job always finds either a live channel or an authoritative row.
    async fn spawn_job(&self, id: String, url: String, mode: Mode) {
        let (tx, _) = watch::channel(Progress::Queued);
        self.channels.lock().await.insert(id.clone(), tx.clone());
        tokio::spawn(run_job(self.clone(), id, url, mode, tx));
    }
}

#[derive(Clone, serde::Serialize)]
#[serde(tag = "status")]
enum Progress {
    Queued,
    Running { percent: f32 },
    Done { file: String },
    Failed { user_facing_error: String },
    Expired,
}

impl Progress {
    fn is_terminal(&self) -> bool {
        match self {
            Progress::Queued | Progress::Running { .. } => false,
            Progress::Done { .. } | Progress::Failed { .. } | Progress::Expired => true,
        }
    }

    fn failed(msg: impl Into<String>) -> Self {
        Progress::Failed {
            user_facing_error: msg.into(),
        }
    }
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

    let config = config::Config::load();

    tracing::debug!("yt-dlp-webui starting...");

    // TODO: Consider adding WAL + busy_timeout
    let db = sqlx::SqlitePool::connect("sqlite:jobs.db?mode=rwc")
        .await
        .unwrap();
    sqlx::query(
        "
        CREATE TABLE IF NOT EXISTS jobs (
            id              TEXT PRIMARY KEY,
            url             TEXT NOT NULL,
            mode            TEXT NOT NULL CHECK (mode IN ('video','audio')) ,
            status          TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'done','failed','expired')),
            error           TEXT,
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

    match requeue_interrupted(&st).await {
        Ok(0) => {}
        Ok(n) => tracing::info!("Requeued {n} interrupted job(s)"),
        Err(e) => tracing::error!("Could not requeue interrupted jobs: {e:?}"),
    }

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

/// Anything still non-terminal was interrupted by a restart. Reset it to
/// `queued` so run_job's `queued -> running` transition applies again, and let
/// the semaphore decide the actual order.
async fn requeue_interrupted(st: &AppState) -> Result<usize, sqlx::Error> {
    sqlx::query("UPDATE jobs SET status = ? WHERE status = ?")
        .bind(JobStatus::Queued)
        .bind(JobStatus::Running)
        .execute(&st.db)
        .await?;

    let unfinished_jobs: Vec<(String, String, Mode)> =
        sqlx::query_as("SELECT id, url, mode FROM jobs WHERE status = ?")
            .bind(JobStatus::Queued)
            .fetch_all(&st.db)
            .await?;

    let count = unfinished_jobs.len();
    for (id, url, mode) in unfinished_jobs {
        st.spawn_job(id, url, mode).await;
    }
    Ok(count)
}
