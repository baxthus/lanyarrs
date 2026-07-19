use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::{select, sync::mpsc, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};

use crate::{config, discord::client, storage};

const SOCKET_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("dial: {0}")]
    DialError(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("read: {0}")]
    ReadError(tokio_tungstenite::tungstenite::Error),
}

struct Backoff {
    min: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    fn new(min: Duration, max: Duration) -> Self {
        Self {
            min,
            max,
            current: min,
        }
    }

    fn reset(&mut self) {
        self.current = self.min;
    }

    fn next(&mut self) -> Duration {
        let d = self.current;
        self.current = (self.current * 2).min(self.max);
        d
    }
}

pub struct Gateway {
    config: config::AppConfig,
    storage: storage::Storage,
}

impl Gateway {
    pub fn new(config: config::AppConfig, storage: storage::Storage) -> Self {
        Self { config, storage }
    }

    pub async fn run(self, token: CancellationToken) {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(39));

        while !token.is_cancelled() {
            let connected_at = Instant::now();

            match self.connect_and_run(&token).await {
                Ok(()) => info!("connection closed"),
                Err(e) => error!(error = %e, "connection dropped"),
            }

            if token.is_cancelled() {
                return;
            }

            if connected_at.elapsed() >= Duration::from_secs(10) {
                backoff.reset();
            }

            let wait = backoff.next();
            info!(?wait, "reconnecting");

            select! {
                _ = token.cancelled() => return,
                _ = sleep(wait) => {}
            }
        }
    }

    #[instrument(skip_all)]
    async fn connect_and_run(&self, token: &CancellationToken) -> Result<(), GatewayError> {
        let (ws_stream, _) = connect_async(SOCKET_URL).await?;
        info!("connected to gateway");

        let (mut write, mut read) = ws_stream.split();

        let (tx, mut rx) = mpsc::channel::<Message>(32);
        let writer = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Err(e) = write.send(msg).await {
                    warn!(error = %e, "write failed, closing writer task");
                    break;
                }
            }
        });

        let client =
            client::Client::new(self.config.clone(), self.storage.clone(), tx, token.clone());

        let result: Result<(), GatewayError> = loop {
            select! {
                _ = token.cancelled() => {
                    info!("shutting down websocket connection");
                    break Ok(());
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => client.handle_message(text.as_bytes()).await,
                        Some(Ok(_)) => {}, // hanlded internally
                        Some(Err(e)) => break Err(GatewayError::ReadError(e)),
                        None => break Ok(()), // stream closed
                    }
                }
            }
        };

        drop(client);
        let _ = writer.await;

        result
    }
}
