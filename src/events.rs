use crate::{AppState, Progress};
use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use std::convert::Infallible;
use tokio_stream::{Stream, StreamExt};
use tracing;

pub async fn events(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    tracing::info!("Events requested for job: id={}", id);

    let rx = st.channels.lock().await.get(&id).map(|tx| tx.subscribe());

    let stream = async_stream::stream! {
        if let Some(rx) = rx {
            let mut s = tokio_stream::wrappers::BroadcastStream::new(rx);
            while let Some(item) = s.next().await {
                if let Ok(p) = item {
                    let terminal = !matches!(p, Progress::Running { .. });
                    if let Progress::Running {percent} = p {
                        tracing::debug!("Progress event: {:?}", percent);
                    }
                    yield Ok(Event::default().json_data(p).expect("Progress serialization is infallible"));
                    if terminal { break; }
                } else {
                    tracing::warn!("Received invalid progress event for job: {}", id);
                }
            }
        } else {
            tracing::warn!("No channel found for job: id={}", id);
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}
