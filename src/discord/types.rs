use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Debug, Clone, Copy, PartialEq, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum Opcode {
    Dispatch = 0,
    Heartbeat = 1,
    Identify = 2,
    PresenceUpdate = 3,
    Hello = 10,
    HeartbeatAck = 11,
}

pub mod event_type {
    pub const READY: &str = "READY";
    pub const GUILD_CREATE: &str = "GUILD_CREATE";
    pub const PRESENCE_UPDATE: &str = "PRESENCE_UPDATE";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryGuild {
    pub identity_guild_id: Option<String>,
    pub identity_enabled: Option<bool>,
    pub tag: Option<String>,
    pub badge: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvatarDecorationData {
    pub asset: String,
    pub sku_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    // undocumented
    pub expires_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Nameplate {
    pub sku_id: String,
    pub asset: String,
    pub label: String,
    pub palette: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    // undocumented
    pub expired_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collectibles {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nameplate: Option<Nameplate>,
}

// undocumented
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayNameStyles {
    pub colors: Vec<i32>,
    pub effect_id: String,
    pub font_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub discriminator: String, // only bots have a discriminator, "0" if not a bot
    pub global_name: Option<String>,
    pub avatar: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_flags: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_decoration_data: Option<AvatarDecorationData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collectibles: Option<Collectibles>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_guild: Option<PrimaryGuild>,
    // undocumented
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name_styles: Option<DisplayNameStyles>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum ActivityType {
    Playing,
    Streaming,
    Listening,
    Watching,
    Custom,
    Competing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timestamps {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum StatusDisplayType {
    Name,
    Details,
    State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Emoji {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub animated: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Party {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    // TODO: make sure serde is actually able to serialize/deserialize this
    // if not, use Vec<i32>
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<(i32, i32)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assets {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub large_image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub large_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub large_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub small_image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub small_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub small_url: Option<String>,
    // almost never used
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invite_cover_image: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Secrets {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spectate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#match: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Button {
    pub label: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activity {
    pub name: String,
    // since i'll have to access it frequently, it's better than doing r#type
    #[serde(rename = "type")]
    pub kind: ActivityType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamps: Option<Timestamps>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_display_type: Option<StatusDisplayType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoji: Option<Emoji>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub party: Option<Party>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assets: Option<Assets>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<Secrets>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flags: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buttons: Option<Vec<Button>>,
    // undocumented, used for Spotify integration (the track id)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_id: Option<String>,
    // undocumented, seems to be only present for Spotify integration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClientStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desktop: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mobile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedded: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceUser {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Presence {
    pub user: PresenceUser,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    pub status: String,
    #[serde(default)]
    pub activities: Vec<Activity>,
    #[serde(default)]
    pub client_status: ClientStatus,
}
