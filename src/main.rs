use std::env;
use std::process::exit;

use time::macros::format_description;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::time::LocalTime;

use crate::config::AppConfig;
use crate::discord::Gateway;

mod config;
mod discord;

fn setup_logging() {
    let is_debug = env!("PROFILE") == "debug";

    let timer = LocalTime::new(format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second]"
    ));

    fmt()
        .with_max_level(if is_debug {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .with_timer(timer)
        .pretty()
        .with_target(false)
        .with_file(false)
        .init();
}

#[tokio::main]
async fn main() {
    setup_logging();

    let app_config = AppConfig::new().unwrap_or_else(|e| {
        error!("failed to load configuration: {}", e);
        exit(1);
    });

    let token = CancellationToken::new();
    {
        let token = token.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            info!("shutting down");
            token.cancel();
        });
    }

    Gateway::new(app_config).run(token).await;
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
