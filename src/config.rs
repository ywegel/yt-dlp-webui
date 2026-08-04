#[derive(thiserror::Error, Debug)]
pub enum ConfigurationError {
    #[error("Failed to load toml file: {0}")]
    TomlError(#[from] toml::de::Error),
    #[error("An IO error occurred: {0}")]
    IoError(#[from] std::io::Error),
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8080
}

fn default_max_concurrent() -> usize {
    3
}

fn default_reaper_file_ttl_secs() -> i64 {
    600
}

#[derive(serde::Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub jobs: JobsConfig,
}

#[derive(serde::Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

#[derive(serde::Deserialize)]
pub struct JobsConfig {
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    #[serde(default = "default_reaper_file_ttl_secs")]
    pub file_ttl_secs: i64,
}

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            max_concurrent: default_max_concurrent(),
            file_ttl_secs: default_reaper_file_ttl_secs(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, ConfigurationError> {
        let contents = match std::fs::read_to_string("config.toml") {
            Ok(contents) => contents,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("No config.toml found, using defaults");
                return Ok(Self::default());
            }
            Err(e) => return Err(e.into()),
        };

        Ok(toml::from_str(&contents)?)
    }
}
