/// Lifecycle of a job, as stored in the `jobs.status` column.
///
/// SQLite has no native enum type, so this is stored as `TEXT`. `sqlx::Type`
/// on a fieldless enum encodes/decodes it as a string using the variant names
/// below, which keeps the on-disk values identical to the literals we used
/// before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, serde::Serialize)]
#[cfg_attr(test, derive(strum::EnumIter, strum::VariantArray))]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Queued,
    Running,
    Done,
    Failed,
    Expired,
}

#[cfg(test)]
mod tests {
    use strum::VariantArray;

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

        let all_status = [
            (JobStatus::Queued, "queued"),
            (JobStatus::Running, "running"),
            (JobStatus::Done, "done"),
            (JobStatus::Failed, "failed"),
            (JobStatus::Expired, "expired"),
        ];

        // Make sure that all status variants are checked, so that changes don't forget
        let covered: Vec<JobStatus> = all_status.iter().map(|(s, _)| *s).collect();
        assert_eq!(covered.as_slice(), JobStatus::VARIANTS);

        for (status, text) in all_status {
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
