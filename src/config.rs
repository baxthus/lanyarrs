use config::{Config, Environment, File};
use regex::Regex;
use serde::Deserialize;
use std::sync::LazyLock;
use thiserror::Error;
use tracing::info;
use validator::Validate;

static NUMERIC_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d+$").unwrap());

#[derive(Debug, Clone, Deserialize, Validate)]
pub(crate) struct DiscordConfig {
    #[validate(length(min = 1))]
    pub bot_token: String,
    // numeric
    #[validate(length(min = 1), regex(path = *NUMERIC_REGEX))]
    pub guild_id: String,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub(crate) struct AppConfig {
    pub discord: DiscordConfig,
}

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("configuration error: {0}")]
    ConfigError(#[from] config::ConfigError),
    #[error("validation error: {0}")]
    ValidationError(#[from] validator::ValidationErrors),
}

impl AppConfig {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let settings = Config::builder()
            .add_source(File::with_name("config"))
            .add_source(Environment::default().separator("_"))
            .build()?;

        let config: AppConfig = settings.try_deserialize()?;
        config.validate()?;

        info!("configuration loaded successfully");
        Ok(config)
    }
}
