use crate::submit::Mode;
use crate::{AppState, Progress};
use tokio::sync::broadcast::Sender;
use tracing;

pub async fn run_job(st: AppState, id: String, url: String, mode: Mode, tx: Sender<Progress>) {
    tracing::info!(
        "Starting download job: id={}, url={}, mode={}",
        id,
        url,
        mode.as_str()
    );

    let _permit = st.limiter.acquire().await.unwrap();

    let fmt = match mode {
        Mode::Video => "bv*+ba/b",
        Mode::Audio => "ba",
    };

    let out = format!("/tmp/ytdlp/{id}/%(title)s.%(ext)s");

    tracing::debug!("Download format: {}, output: {}", fmt, out);

    let mut child = tokio::process::Command::new("yt-dlp")
        .args([
            "-f",
            fmt,
            "-o",
            &out,
            "--no-playlist",
            "--newline",
            "--progress-template",
            "download:%(progress._percent_str)s",
            "--",
            &url,
        ])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(pct) = parse_percent(&line) {
            let _ = tx.send(Progress::Running { percent: pct });
            tracing::debug!("Progress: {}%", pct);
        }
    }

    let ok = child.wait().await.map(|s| s.success()).unwrap_or(false);
    if ok {
        let file = first_file_in(&format!("/tmp/ytdlp/{id}")).unwrap_or_default();
        let result = sqlx::query(
            "UPDATE jobs SET status='done', file=?, completed_at=strftime('%s','now') WHERE id=?",
        )
        .bind(&file)
        .bind(&id)
        .execute(&st.db)
        .await;

        match result {
            Ok(_) => {
                tracing::info!("Download completed: id={}, file={}", id, file);
            }
            Err(e) => {
                tracing::error!("Failed to update job status: {e:?}");
            }
        }

        let _ = tx.send(Progress::Done { file });
    } else {
        let result = sqlx::query("UPDATE jobs SET status='failed' WHERE id=?")
            .bind(&id)
            .execute(&st.db)
            .await;

        match result {
            Ok(_) => {
                tracing::error!("Download failed: id={}, error={}", id, "yt-dlp failed");
            }
            Err(e) => {
                tracing::error!("Failed to update failed job: {e:?}");
            }
        }

        let _ = tx.send(Progress::Failed {
            error: "yt-dlp failed".into(),
        });
        eprintln!("yt-dlp error: {:?}", child.stderr.take().unwrap())
    }
    st.channels.lock().await.remove(&id);
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
