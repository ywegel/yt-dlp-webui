use crate::AppState;
use std::time::Duration;
use tracing;

pub async fn reaper(st: AppState, ttl: i64) {
    tracing::info!("Reaper spawned: TTL={} seconds", ttl);
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    loop {
        tick.tick().await;
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT id, file FROM jobs WHERE status='done' AND \
             (strftime('%s','now') - max(completed_at, coalesce(last_download_at,0))) > ?",
        )
        .bind(ttl)
        .fetch_all(&st.db)
        .await
        .unwrap_or_default();

        for (id, file) in rows {
            tracing::info!("Removing expired file: id={}, file={}", id, file);
            sqlx::query("UPDATE jobs SET status='expired' WHERE id=?")
                .bind(&id)
                .execute(&st.db)
                .await
                .ok();
            let res = tokio::fs::remove_file(&file).await;
            if let Err(e) = res {
                tracing::warn!("Failed to remove file {}: {}", file, e);
            }
        }
    }
}
