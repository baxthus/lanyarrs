use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Serialize;

use crate::api::{
    router::RouterState,
    types::{DISCORD_ID_REGEX, ResponseData},
    utils::ok_or_status,
};

#[derive(Debug, Clone, Serialize)]
pub struct UserResponse {
    pub success: bool,
    pub data: ResponseData,
}

pub async fn get_user(
    State(state): State<Arc<RouterState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let id = id.as_str();
    if !DISCORD_ID_REGEX.is_match(id) {
        return Err((StatusCode::BAD_REQUEST, "Invalid ID".to_string()));
    }

    let user = ok_or_status(
        state.storage.get_user(id).await,
        "User not found",
        "Failed to get user",
    )?;
    let presence = ok_or_status(
        state.storage.get_presence(id).await,
        "Presence not found",
        "Failed to get presence",
    )?;

    let data = ResponseData::build(user, presence);
    let response = UserResponse {
        success: true,
        data,
    };

    Ok(serde_json::to_string(&response).unwrap())
}
