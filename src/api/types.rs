use std::sync::LazyLock;

use regex::Regex;
use serde::Serialize;

use crate::{
    api::utils::is_active,
    discord::{Activity, ActivityType, Presence, Timestamps, User},
};

pub static DISCORD_ID_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{17,19}$").unwrap());

#[derive(Debug, Clone, Serialize)]
pub struct ResponseSpotify {
    pub track_id: String,
    pub song: String,
    pub artist: String,
    pub album: String,
    pub album_art_url: String,
    pub timestamps: Timestamps,
}

impl ResponseSpotify {
    const ACTIVITY_ID: &'static str = "spotify:1";
    const IMAGE_ID_PREFIX: &'static str = "spotify:";

    fn from_presence(presence: &Presence) -> Option<Self> {
        let activity = presence.activities.iter().find(|activity| {
            activity.kind == ActivityType::Listening
                && activity.id.as_deref() == Some(Self::ACTIVITY_ID)
        })?;

        let assets = activity.assets.as_ref()?;
        let large_image = assets.large_image.as_deref()?;

        Some(Self {
            track_id: activity.sync_id.clone()?,
            song: activity.details.clone()?,
            artist: activity.state.clone()?,
            album: assets.large_text.clone()?,
            album_art_url: format!(
                "https://i.scdn.co/image/{}",
                large_image.strip_prefix(Self::IMAGE_ID_PREFIX)?
            ),
            timestamps: activity.timestamps.clone()?,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseData {
    pub active_on_discord_web: bool,
    pub active_on_discord_mobile: bool,
    pub active_on_discord_desktop: bool,
    pub active_on_discord_embedded: bool,
    pub active_on_discord_vr: bool,
    pub listening_to_spotify: bool,
    pub spotify: Option<ResponseSpotify>,
    pub discord_user: User,
    pub discord_status: String,
    pub activities: Vec<Activity>,
}

impl ResponseData {
    pub fn build(user: User, presence: Presence) -> Self {
        let spotify = ResponseSpotify::from_presence(&presence);
        let status = &presence.client_status;

        Self {
            active_on_discord_web: is_active(status.web.as_deref()),
            active_on_discord_mobile: is_active(status.mobile.as_deref()),
            active_on_discord_desktop: is_active(status.desktop.as_deref()),
            active_on_discord_embedded: is_active(status.embedded.as_deref()),
            active_on_discord_vr: is_active(status.vr.as_deref()),
            listening_to_spotify: spotify.is_some(),
            spotify,
            discord_user: user,
            discord_status: presence.status,
            activities: presence.activities,
        }
    }
}
