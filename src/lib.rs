mod download;
mod events;
mod job_status;
mod reaper;
mod run_job;
mod submit;
#[cfg(test)]
mod test_helpers;

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

    sqlx::migrate!().run(&db).await?;

    Ok(db)
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

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use super::*;
    use crate::test_helpers::insert_job;
    use crate::test_helpers::job_status;

    /// Restart recovery: everything non-terminal is picked up again, everything
    /// terminal is left alone.
    #[sqlx::test]
    async fn requeue_restarts_only_unfinished_jobs(db: SqlitePool) {
        // Semaphore of 0, so that `yt-dlp` never starts
        let st = AppState::new(db, 0);
        insert_job(&st.db, "was-running", JobStatus::Running, None, None).await;
        insert_job(&st.db, "was-queued", JobStatus::Queued, None, None).await;
        insert_job(
            &st.db,
            "finished",
            JobStatus::Done,
            Some("/tmp/f.mp4"),
            None,
        )
        .await;
        insert_job(&st.db, "broken", JobStatus::Failed, None, Some("nope")).await;

        assert_eq!(requeue_interrupted(&st).await.unwrap(), 2);

        assert_eq!(job_status(&st.db, "was-running").await, JobStatus::Queued);
        assert_eq!(job_status(&st.db, "was-queued").await, JobStatus::Queued);
        assert_eq!(job_status(&st.db, "finished").await, JobStatus::Done);
        assert_eq!(job_status(&st.db, "broken").await, JobStatus::Failed);

        // Every requeued job owns a channel before the listener binds, so no
        // client can arrive while neither a channel nor a row is available.
        let channels = st.channels.lock().await;
        assert_eq!(channels.len(), 2);
        assert!(channels.contains_key("was-running"));
        assert!(channels.contains_key("was-queued"));
    }
}
