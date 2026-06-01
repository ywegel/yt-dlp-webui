use crate::AppState;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{Response, StatusCode, header},
};
use tokio_util::io::ReaderStream;

pub async fn download(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response<Body>, StatusCode> {
    let file_path: Option<String> = sqlx::query_scalar(
        "SELECT file FROM jobs WHERE id = ? AND status = 'done' AND file IS NOT NULL",
    )
    .bind(&id)
    .fetch_optional(&st.db)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let file_path = file_path.ok_or(StatusCode::NOT_FOUND)?;

    sqlx::query("UPDATE jobs SET last_download_at = strftime('%s','now') WHERE id = ?")
        .bind(&id)
        .execute(&st.db)
        .await
        .ok();

    let file = tokio::fs::File::open(&file_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;

    let file_size = file
        .metadata()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .len();

    let file_name = std::path::Path::new(&file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download")
        .to_owned();

    let stream = ReaderStream::new(file);
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, file_size)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", file_name),
        )
        .body(Body::from_stream(stream))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}
