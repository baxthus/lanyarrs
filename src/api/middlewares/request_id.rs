use axum::{
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use nanoid::nanoid;

static X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
static X_POWERED_BY: HeaderName = HeaderName::from_static("x-powered-by");
static EASTER_EGG: HeaderValue = HeaderValue::from_static("Etanol Cachaca Pinga Reactor 4");

#[derive(Clone)]
#[allow(dead_code)] // probably never going to read it in a request
struct RequestId(pub String);

pub async fn request_id(mut req: Request, next: Next) -> Response {
    let id = nanoid!();

    req.extensions_mut().insert(RequestId(id.clone()));

    let mut res = next.run(req).await;

    if let Ok(value) = HeaderValue::from_str(&id) {
        res.headers_mut().insert(X_REQUEST_ID.clone(), value);
    }
    res.headers_mut()
        .insert(X_POWERED_BY.clone(), EASTER_EGG.clone());

    res
}
