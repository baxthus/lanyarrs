use axum::http::StatusCode;

pub fn ok_or_status<T>(
    result: Result<Option<T>, impl std::error::Error>,
    not_found_msg: &str,
    err_msg: &str,
) -> Result<T, (StatusCode, String)> {
    match result {
        Ok(Some(val)) => Ok(val),
        Ok(None) => Err((StatusCode::NOT_FOUND, not_found_msg.to_string())),
        Err(_) => Err((StatusCode::INTERNAL_SERVER_ERROR, err_msg.to_string())),
    }
}

pub fn is_active(status: Option<&str>) -> bool {
    !matches!(status, None | Some("offline"))
}
