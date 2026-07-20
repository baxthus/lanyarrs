use std::sync::Arc;

use axum::{Router, routing::get};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{api::routes, config, storage};

pub struct RouterState {
    pub storage: storage::Storage,
}

pub async fn new(
    config: config::AppConfig,
    storage: storage::Storage,
    token: CancellationToken,
) -> Result<(), std::io::Error> {
    let shared_state = Arc::new(RouterState { storage });

    let app = Router::new()
        .route("/", get(|| async { "Hello, World" }))
        .route("/user/{id}", get(routes::get_user))
        .with_state(shared_state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.api.port)).await?;
    info!("listening on port {}", config.api.port);
    axum::serve(listener, app)
        .with_graceful_shutdown(token.cancelled_owned())
        .await?;
    Ok(())
}
