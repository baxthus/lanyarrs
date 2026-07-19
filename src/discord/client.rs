use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;
use tokio::time::MissedTickBehavior;
use tokio::{select, sync::mpsc::Sender};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    config,
    discord::types::{Opcode, Presence, User, event_type},
};

#[derive(Debug, Error)]
enum ClientError {
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("send: channel closed")]
    Send,
}

#[derive(Clone)]
pub struct Client {
    config: config::AppConfig,
    tx: Sender<WsMessage>,
    token: CancellationToken,
    sequence: Arc<Mutex<Option<i64>>>,
}

#[derive(Debug, Deserialize)]
struct GatewayMessage<'a> {
    op: Opcode,
    #[serde(borrow, default)]
    d: Option<&'a RawValue>,
    #[serde(default)]
    t: Option<String>,
    #[serde(default)]
    s: Option<i64>,
}

#[derive(Debug, Serialize)]
struct OutgoingMessage<T: Serialize> {
    op: Opcode,
    d: T,
}

#[derive(Debug, Deserialize)]
struct HelloData {
    heartbeat_interval: u64,
}

#[derive(Debug, Serialize)]
struct IdentifyProperties {
    os: &'static str,
    browser: &'static str,
    device: &'static str,
}

#[derive(Debug, Serialize)]
struct IdentifyData {
    token: String,
    intents: u32,
    properties: IdentifyProperties,
}

#[derive(Debug, Deserialize)]
struct ReadyData {
    session_id: String,
    user: User,
}

#[derive(Debug, Deserialize)]
struct GuildMember {
    user: User,
}

#[derive(Debug, Deserialize)]
struct GuildCreateData {
    id: String,
    #[serde(default)]
    members: Vec<GuildMember>,
    #[serde(default)]
    presences: Vec<Presence>,
}

impl Client {
    pub fn new(config: config::AppConfig, tx: Sender<WsMessage>, token: CancellationToken) -> Self {
        Self {
            config,
            tx,
            token,
            sequence: Arc::new(Mutex::new(None)),
        }
    }

    async fn write_json<T: Serialize>(&self, op: Opcode, d: T) -> Result<(), ClientError> {
        let text = serde_json::to_string(&OutgoingMessage { op, d })?;
        self.tx
            .send(WsMessage::text(text))
            .await
            .map_err(|_| ClientError::Send)
    }

    #[instrument(skip_all)]
    pub async fn handle_message(&self, raw: &[u8]) {
        let msg: GatewayMessage = match serde_json::from_slice(raw) {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "failed to decode message");
                return;
            }
        };

        if let Some(seq) = msg.s {
            *self.sequence.lock().unwrap() = Some(seq);
        }

        match (msg.op, msg.t.as_deref()) {
            (Opcode::Hello, _) => self.handle_hello(msg.d).await,
            (Opcode::HeartbeatAck, _) => info!("received heartbeat ack"),
            (_, Some(event_type::READY)) => self.handle_ready(msg.d),
            (_, Some(event_type::GUILD_CREATE)) => self.handle_guild_create(msg.d),
            (_, Some(event_type::PRESENCE_UPDATE)) => self.handle_presence_update(msg.d),
            _ => warn!(op = ?msg.op, t = ?msg.t, "received unhandled message"),
        }
    }

    async fn handle_hello(&self, data: Option<&RawValue>) {
        let Some(data) = data else {
            error!("hello message missing data");
            return;
        };
        let hello: HelloData = match serde_json::from_str(data.get()) {
            Ok(h) => h,
            Err(e) => {
                error!(error = %e, "failed to decode hello message");
                return;
            }
        };

        info!(
            heartbeat_interval = hello.heartbeat_interval,
            "received hello message"
        );

        self.send_identify().await;

        let client = self.clone();
        tokio::spawn(async move {
            client
                .heartbeat_loop(Duration::from_millis(hello.heartbeat_interval))
                .await;
        });
    }

    async fn send_identify(&self) {
        let data = IdentifyData {
            token: self.config.discord.bot_token.clone(),
            // intents: 1 | 2 | 256, // GUILDS | GUILD_MEMBERS | GUILD_PRESENCES
            intents: 1 << 0 | 1 << 1 | 1 << 8, // GUILDS | GUILD_MEMBERS | GUILD_PRESENCES
            properties: IdentifyProperties {
                os: "linux",
                browser: "lanyarrs",
                device: "lanyarrs",
            },
        };

        if let Err(e) = self.write_json(Opcode::Identify, data).await {
            error!(error = %e, "failed to send identify message");
            return;
        }
        info!("sent identify message");
    }

    #[instrument(skip_all)]
    async fn heartbeat_loop(self, interval: Duration) {
        if interval.is_zero() {
            error!(?interval, "invalid heartbeat interval");
            return;
        }

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            select! {
                _ = self.token.cancelled() => {
                    info!("heartbeat loop cancelled");
                    return;
                }
                _ = ticker.tick() => {
                    if self.send_heartbeat().await.is_err() {
                        return;
                    }
                }
            }
        }
    }

    async fn send_heartbeat(&self) -> Result<(), ClientError> {
        let seq = *self.sequence.lock().unwrap();
        match self.write_json(Opcode::Heartbeat, seq).await {
            Ok(()) => {
                info!("sent heartbeat message");
                Ok(())
            }
            Err(e) => {
                error!(error = %e, "failed to send heartbeat message");
                Err(e.into())
            }
        }
    }

    fn handle_ready(&self, data: Option<&RawValue>) {
        let Some(data) = data else { return };
        let ready: ReadyData = match serde_json::from_str(data.get()) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "failed to parse ready data");
                return;
            }
        };

        info!(session_id = %ready.session_id, user = format!("{}#{}", ready.user.username, ready.user.discriminator), "ready event received");
    }

    fn handle_guild_create(&self, data: Option<&RawValue>) {
        let Some(data) = data else { return };
        let guild: GuildCreateData = match serde_json::from_str(data.get()) {
            Ok(g) => g,
            Err(e) => {
                error!(error = %e, "failed to parse guild create data");
                return;
            }
        };

        if guild.id != self.config.discord.guild_id {
            return;
        }

        let users: Vec<User> = guild.members.into_iter().map(|m| m.user).collect();

        // TODO: store users in some kind of state
        debug!(users = ?users, "guild create event received");

        info!("guild create event received");
    }

    fn handle_presence_update(&self, data: Option<&RawValue>) {
        let Some(data) = data else { return };
        let presence: Presence = match serde_json::from_str(data.get()) {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "failed to parse presence update data");
                return;
            }
        };

        if presence.guild_id.as_ref().map(|id| id.as_str())
            != Some(self.config.discord.guild_id.as_str())
        {
            return;
        }

        // TODO: store presence in some kind of state
        debug!(presence = ?presence, "presence update event received");

        info!("presence update event received");
    }
}
