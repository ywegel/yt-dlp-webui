use tokio::sync::broadcast::Sender;

use crate::AppState;
use crate::Progress;
use crate::job_status::JobStatus;
use crate::submit::Mode;

#[derive(Debug, thiserror::Error)]
enum DownloadError {
    #[error("failed to start yt-dlp")]
    Spawn(#[source] std::io::Error),
    #[error("yt-dlp stdout was not captured")]
    StdoutNotCaptured,
    #[error("failed while waiting for yt-dlp to exit")]
    Wait(#[source] std::io::Error),
    #[error("yt-dlp exited with status {0:?}")]
    NonZeroExit(Option<i32>),
    #[error("yt-dlp produced no output file")]
    NoOutputFile,
}

impl DownloadError {
    /// Message safe to show to the client; internal detail stays in the logs.
    fn user_message(&self) -> &'static str {
        match self {
            DownloadError::Spawn(_) => "Failed to start the download",
            DownloadError::StdoutNotCaptured => "Internal error starting the download",
            DownloadError::Wait(_) => "Download process failed unexpectedly",
            DownloadError::NonZeroExit(_) => "Download failed",
            DownloadError::NoOutputFile => "Download completed but produced no file",
        }
    }
}

pub async fn run_job(st: AppState, id: String, url: String, mode: Mode, tx: Sender<Progress>) {
    tracing::info!(
        "Starting download job: id={}, url={}, mode={}",
        id,
        url,
        mode.as_str()
    );

    let _permit = st
        .limiter
        .acquire()
        .await
        .expect("semaphore was closed before run_job could acquire a permit");

    match do_download(&id, &url, mode, &tx).await {
        Ok(file) => {
            let _ = sqlx::query(
                "UPDATE jobs SET status=?, file=?, completed_at=strftime('%s','now') WHERE id=?",
            )
            .bind(JobStatus::Done)
            .bind(&file)
            .bind(&id)
            .execute(&st.db)
            .await
            .inspect_err(|e| tracing::error!("Failed to update job status: {e:?}"));

            tracing::info!("Download completed: id={}, file={}", id, file);
            let _ = tx.send(Progress::Done { file });
        }
        Err(e) => {
            let _ = sqlx::query("UPDATE jobs SET status=? WHERE id=?")
                .bind(JobStatus::Failed)
                .bind(&id)
                .execute(&st.db)
                .await
                .inspect_err(|e| tracing::error!("Failed to update failed job: {e:?}"));

            tracing::error!("Download failed: id={}, error={:?}", id, e);
            let _ = tx.send(Progress::Failed {
                error: e.user_message().to_string(),
            });
        }
    }

    st.channels.lock().await.remove(&id);
}

async fn do_download(
    id: &str,
    url: &str,
    mode: Mode,
    tx: &Sender<Progress>,
) -> Result<String, DownloadError> {
    let out = format!("/tmp/ytdlp/{id}/%(title)s.%(ext)s");

    let mut args = vec![
        "-o",
        &out,
        "--no-playlist",
        "--newline",
        "--progress-template",
        "download:%(progress._percent_str)s",
    ];
    match mode {
        Mode::Video => args.extend(["-f", "bv*+ba/b"]),
        Mode::Audio => args.extend(["-x", "--audio-quality", "0"]),
    }
    args.extend(["--", url]);

    tracing::debug!("yt-dlp args: {:?}", args);

    let mut child = tokio::process::Command::new("yt-dlp")
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(DownloadError::Spawn)?;

    let stdout = child
        .stdout
        .take()
        .ok_or(DownloadError::StdoutNotCaptured)?;

    use tokio::io::AsyncBufReadExt;
    use tokio::io::BufReader;
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(pct) = parse_percent(&line) {
            let _ = tx.send(Progress::Running { percent: pct });
            tracing::debug!("Progress: {}%", pct);
        }
    }

    let status = child.wait().await.map_err(DownloadError::Wait)?;
    if !status.success() {
        return Err(DownloadError::NonZeroExit(status.code()));
    }

    let file = first_file_in(&format!("/tmp/ytdlp/{id}")).ok_or(DownloadError::NoOutputFile)?;
    Ok(file)
}

fn parse_percent(s: &str) -> Option<f32> {
    s.trim().strip_suffix('%')?.parse().ok()
}

fn first_file_in(dir: &str) -> Option<String> {
    let temp = [".part", ".ytdl", ".temp"];
    std::fs::read_dir(dir)
        .ok()?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .filter(|p| {
            let n = p.to_string_lossy();
            !temp.iter().any(|t| n.ends_with(t))
        })
        .max_by_key(|p| p.metadata().and_then(|m| m.modified()).ok())
        .and_then(|p| p.to_str().map(str::to_owned))
}
