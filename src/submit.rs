use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

use crate::AppState;
use crate::job_status::JobStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, sqlx::Type)]
#[cfg_attr(test, derive(strum::EnumIter, strum::VariantArray))]
#[serde(rename_all = "lowercase")]
#[sqlx(rename_all = "lowercase")]
pub enum Mode {
    Video,
    Audio,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::Video => "video",
            Mode::Audio => "audio",
        }
    }
}

#[derive(serde::Deserialize)]
pub struct SubmitReq {
    url: String,
    mode: Mode,
}

#[derive(serde::Serialize)]
pub struct SubmitResp {
    id: String,
}

pub async fn submit(
    State(st): State<AppState>,
    Json(req): Json<SubmitReq>,
) -> Result<Json<SubmitResp>, StatusCode> {
    tracing::info!(
        "Submitting new job: mode={}, url={}",
        req.mode.as_str(),
        req.url
    );

    let id = uuid::Uuid::new_v4().to_string();

    sqlx::query("INSERT INTO jobs (id, url, mode, status) VALUES (?,?,?,?)")
        .bind(&id)
        .bind(&req.url)
        .bind(req.mode)
        .bind(JobStatus::Queued)
        .execute(&st.db)
        .await
        .map_err(|e| {
            tracing::error!("Failed to insert job: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    tracing::debug!("Job inserted with id={}", id);

    st.spawn_job(id.clone(), req.url, req.mode).await;

    Ok(Json(SubmitResp { id }))
}

#[cfg(test)]
mod tests {
    use strum::VariantArray;

    use super::*;

    /// Pins the on-disk representation: these strings predate the enum and
    /// exist in already-deployed databases. The `CHECK` mirrors the one in the
    /// real schema, so a variant the constraint does not allow fails here
    /// instead of silently rejecting inserts at runtime.
    #[tokio::test]
    async fn round_trips_as_lowercase_text() {
        let db = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE t (mode TEXT NOT NULL CHECK (mode IN ('video','audio')))")
            .execute(&db)
            .await
            .unwrap();

        let all_modes = [(Mode::Video, "video"), (Mode::Audio, "audio")];

        // Make sure that all mode variants are checked, so that changes don't forget
        let covered: Vec<Mode> = all_modes.iter().map(|(m, _)| *m).collect();
        assert_eq!(covered.as_slice(), Mode::VARIANTS);

        for (mode, text) in all_modes {
            sqlx::query("INSERT INTO t (mode) VALUES (?)")
                .bind(mode)
                .execute(&db)
                .await
                .unwrap();

            let stored: String = sqlx::query_scalar("SELECT mode FROM t")
                .fetch_one(&db)
                .await
                .unwrap();
            assert_eq!(stored, text);

            let decoded: Mode = sqlx::query_scalar("SELECT mode FROM t")
                .fetch_one(&db)
                .await
                .unwrap();
            assert_eq!(decoded, mode);

            // `as_str` is a second, hand-written source for the same strings.
            assert_eq!(mode.as_str(), text);

            sqlx::query("DELETE FROM t").execute(&db).await.unwrap();
        }
    }
}
