mod config;

use axum::serve;
use tracing_subscriber::fmt::format::FmtSpan;
use yt_dlp_webui::AppState;

use crate::config::ConfigurationError;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigurationError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO) // TODO: Set tracing leve from env
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .compact()
        .init();

    let config = config::Config::load()?;

    tracing::debug!("yt-dlp-webui starting...");

    let db = yt_dlp_webui::connect_db("sqlite:jobs.db?mode=rwc").await?;

    let st = AppState::new(db, config.jobs.max_concurrent);

    match yt_dlp_webui::requeue_interrupted(&st).await {
        Ok(0) => {}
        Ok(n) => tracing::info!("Requeued {n} interrupted job(s)"),
        Err(e) => tracing::error!("Could not requeue interrupted jobs: {e:?}"),
    }

    tokio::spawn(yt_dlp_webui::reaper(st.clone(), config.jobs.file_ttl_secs));

    let addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!("Listening on http://{}", addr);

    serve(listener, yt_dlp_webui::app(st)).await?;

    Ok(())
}
