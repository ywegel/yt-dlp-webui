use std::time::Duration;

use crate::AppState;
use crate::job_status::JobStatus;

/// Runs [`reap_once`] every `interval`. This wrapper is deliberately trivial:
/// everything except the timing lives in `reap_once`, which a test can call
/// directly instead of waiting for a tick.
///
/// `interval` is a [`Duration`] rather than a second count so it cannot be
/// swapped with `ttl` at the call site.
pub async fn reaper(st: AppState, ttl: i64, interval: Duration) {
    // `tokio::time::interval` panics on a zero period, which would kill this
    // task silently and stop all cleanup until the next restart.
    let interval = interval.max(Duration::from_secs(1));

    tracing::info!(
        "Reaper spawned: TTL={}s, interval={}s",
        ttl,
        interval.as_secs()
    );

    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        reap_once(&st, ttl).await;
    }
}

/// One sweep: every `done` job untouched for longer than `ttl` seconds becomes
/// `expired` and its file is deleted. Returns how many jobs were expired.
///
/// The row is marked `expired` before the file is removed, so a failed delete
/// leaks disk space rather than leaving a client with a `done` job whose file
/// is already gone.
pub async fn reap_once(st: &AppState, ttl: i64) -> usize {
    let rows = sqlx::query_as::<_, (String, String)>(
        "SELECT id, file FROM jobs WHERE status=? AND \
         (strftime('%s','now') - max(completed_at, coalesce(last_download_at,0))) > ?",
    )
    .bind(JobStatus::Done)
    .bind(ttl)
    .fetch_all(&st.db)
    .await;

    let rows = match rows {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!("Could not query expired jobs: {e:?}");
            return 0;
        }
    };

    let mut expired = 0;
    for (id, file) in rows {
        tracing::info!("Removing expired file: id={}, file={}", id, file);

        if let Err(e) = sqlx::query("UPDATE jobs SET status=? WHERE id=?")
            .bind(JobStatus::Expired)
            .bind(&id)
            .execute(&st.db)
            .await
        {
            tracing::error!("Could not mark job expired: id={id}, error={e:?}");
            continue;
        }
        expired += 1;

        if let Err(e) = tokio::fs::remove_file(&file).await {
            tracing::warn!("Failed to remove file {}: {}", file, e);
        }
    }

    expired
}
