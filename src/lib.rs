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

use crate::download::download;
use crate::events::events;
use crate::job_status::JobStatus;
pub use crate::reaper::reaper;
use crate::run_job::run_job;
use crate::submit::Mode;
use crate::submit::submit;

#[derive(Clone)]
pub struct AppState {
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

    pub fn new(db: sqlx::SqlitePool, max_concurrent_jobs: usize) -> Self {
        AppState {
            db,
            limiter: Arc::new(Semaphore::new(max_concurrent_jobs)),
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
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

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/api/submit", post(submit))
        .route("/api/jobs/{id}/events", get(events))
        .route("/api/jobs/{id}/file", get(download))
        .fallback_service(tower_http::services::ServeDir::new("./serve"))
        .with_state(state)
}

pub async fn connect_db(url: &str) -> Result<sqlx::SqlitePool, sqlx::Error> {
    // TODO: Consider adding WAL + busy_timeout
    let db = sqlx::SqlitePool::connect(url).await?;
    init_db(&db).await?;
    Ok(db)
}

async fn init_db(db: &sqlx::SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        CREATE TABLE IF NOT EXISTS jobs (
            id              TEXT PRIMARY KEY,
            url             TEXT NOT NULL,
            mode            TEXT NOT NULL CHECK (mode IN ('video','audio')),
            status          TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'done','failed','expired')),
            error           TEXT,
            file            TEXT,
            created_at      INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            completed_at    INTEGER,
            last_download_at INTEGER
        )
    ",
    )
        .execute(db)
        .await?;

    Ok(())
}

/// Anything still non-terminal was interrupted by a restart. Reset it to
/// `queued` so run_job's `queued -> running` transition applies again, and let
/// the semaphore decide the actual order.
pub async fn requeue_interrupted(st: &AppState) -> Result<usize, sqlx::Error> {
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
