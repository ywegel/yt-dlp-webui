fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8080
}

fn default_max_concurrent() -> usize {
    3
}

fn default_reaper_interval_secs() -> i64 {
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
    #[serde(default = "default_reaper_interval_secs")]
    pub reaper_interval_secs: i64,
}

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            max_concurrent: default_max_concurrent(),
            reaper_interval_secs: default_reaper_interval_secs(),
        }
    }
}

impl Config {
    pub fn load() -> Self {
        match std::fs::read_to_string("config.toml") {
            Ok(contents) => toml::from_str(&contents).expect("Invalid config.toml"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("No config.toml found, using defaults");
                Self::default()
            }
            Err(e) => panic!("Failed to read config.toml: {e}"),
        }
    }
}
