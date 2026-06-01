use crate::{AppState, Progress};
use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use std::convert::Infallible;
use tokio_stream::{Stream, StreamExt};

pub async fn events(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = st.channels.lock().await.get(&id).map(|tx| tx.subscribe());

    let stream = async_stream::stream! {
        if let Some(rx) = rx {
            let mut s = tokio_stream::wrappers::BroadcastStream::new(rx);
            while let Some(item) = s.next().await {
                if let Ok(p) = item {
                    let terminal = !matches!(p, Progress::Running { .. });
                    yield Ok(Event::default().json_data(p).unwrap());
                    if terminal { break; }
                }
            }
        } else {
            // TODO: Log if an error occured
            // (SELECT status, file FROM jobs WHERE id = ?)
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}
