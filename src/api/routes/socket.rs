use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::{
        State,
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures_util::{
    Stream, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_repr::{Deserialize_repr, Serialize_repr};
use tokio::{sync::mpsc, time};
use tracing::{debug, error};

use crate::{
    api::{router::RouterState, types::ResponseData},
    discord::{Presence, User},
    storage::Storage,
};

const HEARTBEAT_INTERVAL_MS: u64 = 10_000;
const HEARTBEAT_TICK: Duration = Duration::from_millis(HEARTBEAT_INTERVAL_MS);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
enum Opcode {
    Event,
    Hello,
    Initialize,
    Heartbeat,
    Unsubscribe,
}

mod event_type {
    pub const INIT_STATE: &str = "INIT_STATE";
    pub const PRESENCE_UPDATE: &str = "PRESENCE_UPDATE";
}

#[derive(Debug, Deserialize)]
struct IncomingMessage<'a> {
    op: Opcode,
    #[serde(borrow, default)]
    d: Option<&'a RawValue>,
}

#[derive(Debug, Serialize)]
struct OutgoingMessage<T: Serialize> {
    op: Opcode,
    #[serde(skip_serializing_if = "Option::is_none")]
    t: Option<&'static str>,
    #[serde(skip_serializing_if = "is_zero")]
    seq: i64,
    d: T,
}

fn is_zero(seq: &i64) -> bool {
    *seq == 0
}

#[derive(Debug, Serialize)]
struct HelloData {
    heartbeat_interval: u64,
}

#[derive(Debug, Deserialize)]
struct InitializeData {
    subscribe_to_id: String,
}

pub async fn socket(
    ws: WebSocketUpgrade,
    State(state): State<Arc<RouterState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| serve(socket, state.storage.clone()))
}

struct Heartbeat {
    ticker: time::Interval,
    last_beat: Instant,
}

impl Heartbeat {
    async fn start() -> Self {
        let last_beat = Instant::now();
        let mut ticker = time::interval(HEARTBEAT_TICK);
        ticker.tick().await;
        Self { ticker, last_beat }
    }

    fn beat(&mut self) {
        self.last_beat = Instant::now();
    }

    async fn is_alive(&mut self) -> bool {
        self.ticker.tick().await;
        self.last_beat.elapsed() <= HEARTBEAT_TIMEOUT
    }
}

async fn serve(socket: WebSocket, storage: Storage) {
    let (sink, mut stream) = socket.split();
    let (tx, rx) = mpsc::channel::<WsMessage>(32);
    let writer = tokio::spawn(write_loop(sink, rx));

    run_session(&mut stream, &tx, storage).await;

    drop(tx);
    let _ = writer.await;
}

async fn run_session(
    stream: &mut SplitStream<WebSocket>,
    tx: &mpsc::Sender<WsMessage>,
    storage: Storage,
) {
    if !send_hello(tx).await {
        return;
    }

    let mut heartbeat = Heartbeat::start().await;

    let Some(subscribe_to_id) = read_initialize(stream, &mut heartbeat).await else {
        return;
    };

    let sequence = 1;
    match fetch_user_presence(&storage, &subscribe_to_id).await {
        Some((user, presence)) => {
            let msg = OutgoingMessage {
                op: Opcode::Event,
                t: Some(event_type::INIT_STATE),
                seq: sequence,
                d: ResponseData::build(user, presence),
            };
            send_json(tx, &msg).await;
        }
        None => debug!("no initial presence available"),
    }

    let forwarder = match storage.subscribe_updates(&subscribe_to_id).await {
        Ok(updates) => Some(tokio::spawn(forward_updates(
            updates,
            storage,
            subscribe_to_id,
            sequence,
            tx.clone(),
        ))),
        Err(e) => {
            error!(error = %e, "failed to subscribe to updates");
            None
        }
    };

    read_loop(stream, heartbeat).await;

    if let Some(forwarder) = forwarder {
        forwarder.abort();
    }
}

async fn write_loop(mut sink: SplitSink<WebSocket, WsMessage>, mut rx: mpsc::Receiver<WsMessage>) {
    use futures_util::SinkExt;

    while let Some(msg) = rx.recv().await {
        match time::timeout(WRITE_TIMEOUT, sink.send(msg)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => break,
            Err(_) => {
                debug!("write timed out, closing connection");
                break;
            }
        }
    }
}

async fn send_json<T: Serialize>(tx: &mpsc::Sender<WsMessage>, msg: &T) -> bool {
    let text = match serde_json::to_string(msg) {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, "failed to serialize message");
            return false;
        }
    };
    tx.send(WsMessage::Text(text.into())).await.is_ok()
}

async fn send_hello(tx: &mpsc::Sender<WsMessage>) -> bool {
    let msg = OutgoingMessage {
        op: Opcode::Hello,
        t: None,
        seq: 0,
        d: HelloData {
            heartbeat_interval: HEARTBEAT_INTERVAL_MS,
        },
    };
    if !send_json(tx, &msg).await {
        error!("failed to send hello message");
        return false;
    }
    true
}

async fn read_initialize(
    stream: &mut SplitStream<WebSocket>,
    heartbeat: &mut Heartbeat,
) -> Option<String> {
    loop {
        tokio::select! {
            alive = heartbeat.is_alive() => {
                if !alive {
                    debug!("heartbeat timeout during handshake, closing connection");
                    return None;
                }
            }
            msg = stream.next() => {
                let text = match msg {
                    Some(Ok(WsMessage::Text(text))) => text,
                    Some(Ok(WsMessage::Close(_))) | None => {
                        debug!("websocket closed by client");
                        return None;
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => {
                        debug!(error = %e, "failed to read initialize message");
                        return None;
                    }
                };

                let parsed: IncomingMessage = match serde_json::from_str(&text) {
                    Ok(m) => m,
                    Err(e) => {
                        debug!(error = %e, "failed to decode initialize message");
                        return None;
                    }
                };

                match parsed.op {
                    Opcode::Heartbeat => heartbeat.beat(),
                    Opcode::Initialize => {
                        let data = parsed.d?;
                        return match serde_json::from_str::<InitializeData>(data.get()) {
                            Ok(d) => Some(d.subscribe_to_id),
                            Err(e) => {
                                debug!(error = %e, "failed to decode initialize message");
                                None
                            }
                        };
                    }
                    other => {
                        debug!(op = ?other, "expected initialize message");
                        return None;
                    }
                }
            }
        }
    }
}

async fn fetch_user_presence(storage: &Storage, id: &str) -> Option<(User, Presence)> {
    let (user, presence) = tokio::join!(storage.get_user(id), storage.get_presence(id));

    let user = match user {
        Ok(Some(u)) => u,
        Ok(None) => return None,
        Err(e) => {
            error!(error = %e, "failed to get user");
            return None;
        }
    };
    let presence = match presence {
        Ok(Some(p)) => p,
        Ok(None) => return None,
        Err(e) => {
            error!(error = %e, "failed to get presence");
            return None;
        }
    };

    Some((user, presence))
}

async fn forward_updates(
    updates: impl Stream<Item = String> + Send,
    storage: Storage,
    subscribe_to_id: String,
    mut sequence: i64,
    tx: mpsc::Sender<WsMessage>,
) {
    tokio::pin!(updates);

    while updates.next().await.is_some() {
        sequence += 1;

        let Some((user, presence)) = fetch_user_presence(&storage, &subscribe_to_id).await else {
            error!("failed to get user or presence");
            continue;
        };

        let msg = OutgoingMessage {
            op: Opcode::Event,
            t: Some(event_type::PRESENCE_UPDATE),
            seq: sequence,
            d: ResponseData::build(user, presence),
        };

        if !send_json(&tx, &msg).await {
            return;
        }
    }
}

async fn read_loop(stream: &mut SplitStream<WebSocket>, mut heartbeat: Heartbeat) {
    loop {
        tokio::select! {
            alive = heartbeat.is_alive() => {
                if !alive {
                    debug!("heartbeat timeout, closing connection");
                    return;
                }
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => match serde_json::from_str::<IncomingMessage>(&text) {
                        Ok(IncomingMessage { op: Opcode::Heartbeat, .. }) => heartbeat.beat(),
                        Ok(IncomingMessage { op: Opcode::Unsubscribe, .. }) => {
                            debug!("unsubscribing from updates");
                            return;
                        }
                        Ok(m) => debug!(op = ?m.op, "unknown opcode"),
                        Err(e) => error!(error = %e, "failed to decode message"),
                    },
                    Some(Ok(WsMessage::Close(_))) | None => {
                        debug!("websocket closed by client");
                        return;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        error!(error = %e, "failed to read message");
                        return;
                    }
                }
            }
        }
    }
}
