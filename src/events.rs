use std::convert::Infallible;

use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::response::sse::Sse;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

use crate::AppState;
use crate::Progress;
use crate::job_status::JobStatus;

pub async fn events(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    tracing::info!("Events requested for job: id={}", id);

    let rx = st.channels.lock().await.get(&id).map(|tx| tx.subscribe());

    let job_row: Option<(JobStatus, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT status, file, error FROM jobs WHERE id = ?")
            .bind(&id)
            .fetch_optional(&st.db)
            .await
            .map_err(|e| {
                tracing::error!("Failed to fetch job: id={id}, error={e:?}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

    let Some((status, file, error)) = job_row else {
        tracing::warn!("Events requested for unknown job: id={id}");
        return Err(StatusCode::NOT_FOUND);
    };

    let progress_snapshot = match (status, file) {
        (JobStatus::Queued, _) => Progress::Queued,
        (JobStatus::Running, _) => Progress::Running { percent: 0.0 },
        (JobStatus::Done, Some(file)) => Progress::Done { file },
        (JobStatus::Done, None) => {
            Progress::failed("Download finished, but no output file found. Please try again")
        }
        (JobStatus::Failed, _) => {
            Progress::failed(error.unwrap_or_else(|| "Download failed".into()))
        }
        (JobStatus::Expired, _) => Progress::Expired,
    };

    let stream = async_stream::stream! {
        let snapshot_terminal = progress_snapshot.is_terminal();
        yield Ok(Event::default().json_data(&progress_snapshot).expect("infallible"));
        if snapshot_terminal {return;}
        if let Some(rx) = rx {
            let mut s = tokio_stream::wrappers::WatchStream::new(rx);
            while let Some(p) = s.next().await {
                let terminal = p.is_terminal();
                if let Progress::Running {percent} = p {
                        tracing::debug!("Progress event: {:?}", percent);
                }
                yield Ok(Event::default().json_data(p).expect("Progress serialization is infallible"));
                if terminal { break; }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
