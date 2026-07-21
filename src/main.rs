use time::macros::format_description;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::fmt::time::LocalTime;
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::AppConfig;
use crate::discord::Gateway;
use crate::storage::Storage;

mod api;
mod config;
mod discord;
mod storage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_logging();

    let app_config = AppConfig::new()?;
    let storage = Storage::connect(&app_config).await?;

    let token = CancellationToken::new();
    {
        let token = token.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            info!("shutting down");
            token.cancel();
        });
    }

    {
        let token = token.clone();
        let app_config = app_config.clone();
        let storage = storage.clone();
        tokio::spawn(async move {
            api::new(app_config, storage, token).await.unwrap();
        });
    }

    Gateway::new(app_config, storage).run(token).await;

    Ok(())
}

fn setup_logging() {
    let filter = EnvFilter::builder().try_from_env().unwrap_or_else(|_| {
        EnvFilter::builder()
            .with_default_directive(tracing::Level::WARN.into())
            .parse("lanyarrs=debug")
            .unwrap()
    });

    let timer = LocalTime::new(format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second]"
    ));

    fmt()
        .with_env_filter(filter)
        .with_timer(timer)
        .with_target(false)
        .pretty()
        .init();
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

    select! {
        _ = ctrl_c => {},
        _ = sigterm.recv() => {},
    }
}
