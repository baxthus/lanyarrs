use config::{Config, Environment, File};
use garde::Validate;
use regex::Regex;
use serde::Deserialize;
use std::sync::LazyLock;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("configuration error: {0}")]
    ConfigError(#[from] config::ConfigError),
    #[error("validation error: {0}")]
    ValidationError(#[from] garde::Report),
}

static NUMERIC_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d+$").unwrap());

#[derive(Debug, Clone, Deserialize, Validate)]
pub(crate) struct DiscordConfig {
    #[garde(length(min = 1))]
    pub bot_token: String,
    #[garde(length(min = 1), pattern(*NUMERIC_REGEX))]
    pub guild_id: String,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub(crate) struct AppConfig {
    #[garde(dive)]
    pub discord: DiscordConfig,
    #[garde(url)]
    pub redis_url: String,
}

impl AppConfig {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let settings = Config::builder()
            .add_source(File::with_name("config"))
            .add_source(Environment::default().separator("__"))
            .build()?;

        let config: AppConfig = settings.try_deserialize()?;
        config.validate()?;

        info!("configuration loaded successfully");
        Ok(config)
    }
}
