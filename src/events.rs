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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::SqlitePool;

    use super::*;
    use crate::test_support::insert_job;
    use crate::test_support::open_events;
    use crate::test_support::register_channel;

    /// A semaphore of 0 blocks `yt-dlp`, as it is not available in tests.
    fn state(db: SqlitePool) -> AppState {
        AppState::new(db, 0)
    }

    #[sqlx::test]
    async fn unknown_job_is_not_found(db: SqlitePool) {
        let st = state(db);

        // A 404 makes EventSource give up. A closed 200 would make it reconnect
        // forever against a job that will never exist.
        let Err(status) = open_events(&st, "nope").await else {
            panic!("events for an unknown job must not open a stream");
        };
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// Fixed race condition: download finished and tore its channel down before
    /// the browser subscribed. Without the database snapshot the client would
    /// wait forever.
    #[sqlx::test]
    async fn finished_job_without_channel_replays_and_ends(db: SqlitePool) {
        let st = state(db);
        insert_job(&st.db, "j", JobStatus::Done, Some("/tmp/f.mp4"), None).await;

        let mut client = open_events(&st, "j").await.unwrap();

        assert_eq!(
            client.next_json().await.unwrap(),
            json!({"status": "Done", "file": "/tmp/f.mp4"})
        );
        assert!(client.next_json().await.is_none());
    }

    /// The other half of that window: the db entry is already committed but the
    /// channel has not been removed yet. The terminal db entry must win,
    /// otherwise the handler blocks on a channel nobody will ever send on
    /// again.
    #[sqlx::test]
    async fn terminal_row_wins_over_live_channel(db: SqlitePool) {
        let st = state(db);
        insert_job(&st.db, "j", JobStatus::Done, Some("/tmp/f.mp4"), None).await;
        let _tx = register_channel(&st, "j").await;

        let mut client = open_events(&st, "j").await.unwrap();

        assert_eq!(client.next_json().await.unwrap()["status"], "Done");
        assert!(client.next_json().await.is_none());
    }

    /// Teardown lands between the subscribe and the first poll of the stream.
    /// `WatchStream` still hands out the value the sender left behind, so the
    /// terminal event is not lost.
    #[sqlx::test]
    async fn terminal_event_survives_writer_teardown(db: SqlitePool) {
        let st = state(db);
        insert_job(&st.db, "j", JobStatus::Running, None, None).await;
        let tx = register_channel(&st, "j").await;

        // Subscribed and db entry read; the stream body has not run yet.
        let mut client = open_events(&st, "j").await.unwrap();

        // The writer finishes and disappears completely.
        tx.send(Progress::Done {
            file: "/tmp/f.mp4".into(),
        })
        .unwrap();
        st.channels.lock().await.remove("j");
        drop(tx);

        assert_eq!(client.next_json().await.unwrap()["status"], "Running");
        assert_eq!(client.next_json().await.unwrap()["status"], "Done");
        assert!(client.next_json().await.is_none());
    }

    #[sqlx::test]
    async fn live_job_streams_until_terminal(db: SqlitePool) {
        let st = state(db);
        insert_job(&st.db, "j", JobStatus::Queued, None, None).await;
        let tx = register_channel(&st, "j").await;

        let mut client = open_events(&st, "j").await.unwrap();

        // Twice: once from the database snapshot, once as the watch channel's
        // current value
        assert_eq!(
            client.next_json().await.unwrap(),
            json!({"status": "Queued"})
        );
        assert_eq!(
            client.next_json().await.unwrap(),
            json!({"status": "Queued"})
        );

        tx.send(Progress::Running { percent: 42.0 }).unwrap();
        assert_eq!(
            client.next_json().await.unwrap(),
            json!({"status": "Running", "percent": 42.0})
        );

        tx.send(Progress::Done {
            file: "/tmp/f.mp4".into(),
        })
        .unwrap();
        assert_eq!(
            client.next_json().await.unwrap(),
            json!({"status": "Done", "file": "/tmp/f.mp4"})
        );
        assert!(client.next_json().await.is_none());
    }

    /// The frontend reads `user_facing_error`
    #[sqlx::test]
    async fn failed_job_replays_its_error(db: SqlitePool) {
        let st = state(db);
        insert_job(
            &st.db,
            "j",
            JobStatus::Failed,
            None,
            Some("Download failed"),
        )
        .await;

        let mut client = open_events(&st, "j").await.unwrap();

        assert_eq!(
            client.next_json().await.unwrap(),
            json!({"status": "Failed", "user_facing_error": "Download failed"})
        );
        assert!(client.next_json().await.is_none());
    }

    /// A `done` db entry without a file reports that something went wrong
    #[sqlx::test]
    async fn done_without_file_reports_failure(db: SqlitePool) {
        let st = state(db);
        insert_job(&st.db, "j", JobStatus::Done, None, None).await;

        let mut client = open_events(&st, "j").await.unwrap();

        assert_eq!(client.next_json().await.unwrap()["status"], "Failed");
        assert!(client.next_json().await.is_none());
    }
}
