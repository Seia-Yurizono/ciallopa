//! Shared management error responses, account checks and write provenance guards.
use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub(crate) fn err(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"error": {"message": msg}}))).into_response()
}

/// CSRF rules for every state-changing management / usage call. Authentication is by header only
/// (`Authorization: Bearer` / `x-api-key`, checked by the middleware); cookies never authenticate,
/// so a cookie added by a front proxy (Cloudflare Access `CF_Authorization`) is simply ignored.
/// On top of that:
/// - the body must be JSON: a cross-origin page cannot send that (or the key header) without a
///   CORS preflight, which this server never answers;
/// - when the browser says where the call comes from, it must be this origin:
///   `Sec-Fetch-Site` must be `same-origin` / `none`; without it, an `Origin` header must name
///   the request's own host (`Host`, or `X-Forwarded-Host` set by a front proxy). Clients that send
///   neither (curl, scripts) are not affected.
///
/// `Some(rejection)` when the request does not follow them.
pub(crate) fn csrf(headers: &HeaderMap) -> Option<Response> {
    let hv = |n: &str| headers.get(n).and_then(|v| v.to_str().ok()).map(str::trim);
    let json_ct =
        hv("content-type").is_some_and(|v| v.to_ascii_lowercase().starts_with("application/json"));
    if !json_ct {
        return Some(err(
            StatusCode::BAD_REQUEST,
            "send JSON (Content-Type: application/json) with the key in Authorization: Bearer",
        ));
    }
    if !same_origin(headers) {
        return Some(err(
            StatusCode::FORBIDDEN,
            "cross-origin request rejected; use the management page on this host",
        ));
    }
    None
}

/// Browser provenance check of [`csrf`]; true when the headers do not say otherwise.
fn same_origin(headers: &HeaderMap) -> bool {
    let hv = |n: &str| headers.get(n).and_then(|v| v.to_str().ok()).map(str::trim);
    if let Some(site) = hv("sec-fetch-site") {
        return site.eq_ignore_ascii_case("same-origin") || site.eq_ignore_ascii_case("none");
    }
    let Some(origin) = hv("origin") else {
        return true;
    };
    // "null" (sandboxed / opaque origins) or anything unparsable is not this page.
    let Some(o) = origin
        .split_once("://")
        .map(|(_, rest)| rest.trim_end_matches('/').to_ascii_lowercase())
        .filter(|h| !h.is_empty() && !h.contains('/'))
    else {
        return false;
    };
    let ohost = strip_default_port(&o, origin.starts_with("https://"));
    let hosts = headers
        .get_all("x-forwarded-host")
        .iter()
        .chain(headers.get_all(http::header::HOST).iter())
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|h| h.trim().to_ascii_lowercase());
    hosts.into_iter().any(|h| {
        h == o || strip_default_port(&h, true) == ohost || strip_default_port(&h, false) == ohost
    })
}

fn strip_default_port(host: &str, https: bool) -> &str {
    host.strip_suffix(if https { ":443" } else { ":80" })
        .unwrap_or(host)
}

pub(crate) fn write_guard(config: &ciallopa_core::Config, headers: &HeaderMap) -> Option<Response> {
    if !config.ui.allow_config_write {
        return Some(err(StatusCode::FORBIDDEN,
            "changes from the management page are disabled; set [ui] allow_config_write = true in the config file"));
    }
    csrf(headers)
}

pub(crate) async fn require_account(
    st: &ciallopa_forward::AppState,
    id: &str,
) -> Result<(), Response> {
    if st.accounts.accounts().iter().any(|a| a.id == id) {
        Ok(())
    } else {
        Err(err(StatusCode::NOT_FOUND, "unknown account"))
    }
}
