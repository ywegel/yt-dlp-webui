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
    tracing::info!("Downloading file: id={}", id);

    let file_path: Option<String> = sqlx::query_scalar(
        "SELECT file FROM jobs WHERE id = ? AND status = 'done' AND file IS NOT NULL",
    )
    .bind(&id)
    .fetch_optional(&st.db)
    .await
    .map_err(|e| {
        tracing::error!("Database query error: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let file_path = match file_path {
        Some(path) => {
            tracing::debug!("Found file: {}", path);
            path
        }
        None => {
            tracing::warn!("Job not found or incomplete: id={}", id);
            return Err(StatusCode::NOT_FOUND);
        }
    };

    sqlx::query("UPDATE jobs SET last_download_at = strftime('%s','now') WHERE id = ?")
        .bind(&id)
        .execute(&st.db)
        .await
        .ok();

    let file = tokio::fs::File::open(&file_path)
        .await
        .map_err(|e| {
            tracing::error!("Failed to open file {}: {e:?}", file_path);
            StatusCode::NOT_FOUND
        })?;

    let file_size = file
        .metadata()
        .await
        .map_err(|e| {
            tracing::error!("Failed to get file metadata: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .len();

    let file_name = std::path::Path::new(&file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download")
        .to_owned();

    tracing::info!("Sending file: name={}, size={} bytes", file_name, file_size);

    let stream = ReaderStream::new(file);
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, file_size)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", file_name),
        )
        .body(Body::from_stream(stream))
        .map_err(|e| {
            tracing::error!("Failed to build response: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}
