use crate::run_job::run_job;
use crate::{AppState, Progress};
use axum::Json;
use axum::extract::State;
use tokio::sync::broadcast;

#[derive(serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Video,
    Audio,
}

impl Mode {
    fn as_str(&self) -> &'static str {
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

pub async fn submit(State(st): State<AppState>, Json(req): Json<SubmitReq>) -> Json<SubmitResp> {
    let id = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO jobs (id, url, mode, status) VALUES (?,?,?,'queued')")
        .bind(&id)
        .bind(&req.url)
        .bind(req.mode.as_str())
        .execute(&st.db)
        .await
        .unwrap();

    let (tx, _) = broadcast::channel::<Progress>(64);
    st.channels.lock().await.insert(id.clone(), tx.clone());
    tokio::spawn(run_job(st.clone(), id.clone(), req.url, req.mode, tx));
    Json(SubmitResp { id })
}
