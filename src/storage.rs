use futures_util::Stream;
use redis::{AsyncTypedCommands, aio::ConnectionManager};
use thiserror::Error;
use tracing::{error, info, instrument};

use crate::config::AppConfig;
use crate::discord::{Presence, User};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct Storage {
    client: redis::Client,
    conn: ConnectionManager,
}

fn user_key(id: &str) -> String {
    format!("user:{id}")
}

fn presence_key(id: &str) -> String {
    format!("presence:{id}")
}

fn updates_channel(id: &str) -> String {
    format!("updates:{id}")
}

impl Storage {
    #[instrument(skip_all)]
    pub async fn connect(config: &AppConfig) -> Result<Self, StorageError> {
        let client = redis::Client::open(config.redis_url.as_str())?;
        let conn = client.get_connection_manager().await?;

        redis::cmd("PING")
            .query_async::<String>(&mut conn.clone())
            .await?;

        info!("connected to redis");

        Ok(Self { client, conn })
    }

    pub async fn set_user(&self, user: &User) {
        self.set_json(user_key(&user.id), user, "failed to set user in redis")
            .await;
    }

    pub async fn set_users(&self, users: &[User]) {
        let pairs = self.serialize_pairs(users, |u| user_key(&u.id), "failed to serialize users");
        self.mset(pairs, "failed to set users in redis").await;
    }

    pub async fn get_user(&self, id: &str) -> Result<Option<User>, StorageError> {
        self.get_json(user_key(id)).await
    }

    pub async fn set_presence(&self, presence: &Presence) {
        self.set_json(
            presence_key(&presence.user.id),
            presence,
            "failed to set presence in redis",
        )
        .await;
    }

    pub async fn set_presences(&self, presences: &[Presence]) {
        let pairs = self.serialize_pairs(
            presences,
            |p| presence_key(&p.user.id),
            "failed to serialize presences",
        );
        self.mset(pairs, "failed to set presences in redis").await;
    }

    pub async fn get_presence(&self, id: &str) -> Result<Option<Presence>, StorageError> {
        self.get_json(presence_key(id)).await
    }

    pub async fn publish_update(&self, id: &str) {
        let mut conn = self.conn.clone();
        if let Err(e) = conn.publish(updates_channel(id), id).await {
            error!(error = %e, "failed to publish update");
        }
    }

    pub async fn subscribe_updates(
        &self,
        id: &str,
    ) -> Result<impl Stream<Item = String>, StorageError> {
        use futures_util::StreamExt;

        let mut pubsub = self.client.get_async_pubsub().await?;
        pubsub.subscribe(updates_channel(id)).await?;

        Ok(pubsub
            .into_on_message()
            .map(|msg| msg.get_payload().unwrap_or_default()))
    }

    // helpers

    async fn set_json<T: serde::Serialize>(&self, key: String, value: &T, err_msg: &'static str) {
        let payload = match serde_json::to_string(value) {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "failed to serialize value");
                return;
            }
        };

        let mut conn = self.conn.clone();
        if let Err(e) = conn.set(key, payload).await {
            error!(error = %e, "{}", err_msg);
        }
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        key: String,
    ) -> Result<Option<T>, StorageError> {
        let mut conn = self.conn.clone();
        let payload: Option<String> = conn.get(key).await?;
        Ok(match payload {
            Some(p) => Some(serde_json::from_str(&p)?),
            None => None,
        })
    }

    fn serialize_pairs<T: serde::Serialize>(
        &self,
        items: &[T],
        key_fn: impl Fn(&T) -> String,
        err_msg: &'static str,
    ) -> Vec<(String, String)> {
        items
            .iter()
            .filter_map(|item| match serde_json::to_string(item) {
                Ok(payload) => Some((key_fn(item), payload)),
                Err(e) => {
                    error!(error = %e, "{}", err_msg);
                    None
                }
            })
            .collect()
    }

    async fn mset(&self, pairs: Vec<(String, String)>, err_msg: &'static str) {
        if pairs.is_empty() {
            return;
        }
        let mut conn = self.conn.clone();
        if let Err(e) = conn.mset(&pairs).await {
            error!(error = %e, "{}", err_msg);
        }
    }
}
