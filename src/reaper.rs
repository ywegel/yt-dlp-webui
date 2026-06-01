use crate::AppState;
use std::time::Duration;

pub async fn reaper(st: AppState, ttl: i64) {
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
            sqlx::query("UPDATE jobs SET status='expired' WHERE id=?")
                .bind(&id)
                .execute(&st.db)
                .await
                .ok();
            tokio::fs::remove_file(&file).await.ok();
        }
    }
}
