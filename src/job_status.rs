/// Lifecycle of a job, as stored in the `jobs.status` column.
///
/// SQLite has no native enum type, so this is stored as `TEXT`. `sqlx::Type`
/// on a fieldless enum encodes/decodes it as a string using the variant names
/// below, which keeps the on-disk values identical to the literals we used
/// before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, serde::Serialize)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Queued,
    Done,
    Failed,
    Expired,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the on-disk representation: these strings predate the enum and
    /// exist in already-deployed databases.
    #[tokio::test]
    async fn round_trips_as_lowercase_text() {
        let db = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE t (status TEXT NOT NULL)")
            .execute(&db)
            .await
            .unwrap();

        for (status, text) in [
            (JobStatus::Queued, "queued"),
            (JobStatus::Done, "done"),
            (JobStatus::Failed, "failed"),
            (JobStatus::Expired, "expired"),
        ] {
            sqlx::query("INSERT INTO t (status) VALUES (?)")
                .bind(status)
                .execute(&db)
                .await
                .unwrap();

            let stored: String = sqlx::query_scalar("SELECT status FROM t")
                .fetch_one(&db)
                .await
                .unwrap();
            assert_eq!(stored, text);

            let decoded: JobStatus = sqlx::query_scalar("SELECT status FROM t")
                .fetch_one(&db)
                .await
                .unwrap();
            assert_eq!(decoded, status);

            sqlx::query("DELETE FROM t").execute(&db).await.unwrap();
        }
    }
}
