//! Built-in read-only management page (`GET /ui`). One static, self-contained HTML file
//! (inline CSS/JS, no external resources). The page holds no data: it asks for the access
//! key (sessionStorage, sent as `Authorization: Bearer`) and calls `/usage/v1/*`.
//!
//! CSP allows exactly the page's inline `<script>` / `<style>` by SHA-256 hash; everything
//! else is `'none'` except `connect-src 'self'`.
use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;
use base64::Engine;
use sha2::{Digest, Sha256};
use std::sync::LazyLock;

pub const PAGE: &str = include_str!("ui.html");

fn inline_blocks<'a>(html: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = vec![];
    let mut rest = html;
    while let Some(i) = rest.find(&open) {
        let body = &rest[i + open.len()..];
        let Some(j) = body.find(&close) else { break };
        out.push(&body[..j]);
        rest = &body[j + close.len()..];
    }
    out
}

fn hashes(tag: &str) -> String {
    inline_blocks(PAGE, tag)
        .iter()
        .map(|b| {
            let h = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(b.as_bytes()));
            format!("'sha256-{h}'")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub static CSP: LazyLock<String> = LazyLock::new(|| {
    format!(
        "default-src 'none'; script-src {}; style-src {}; connect-src 'self'; img-src 'self' data:; \
         base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        hashes("script"),
        hashes("style")
    )
});

async fn page() -> Response {
    let mut r = PAGE.into_response();
    let h = r.headers_mut();
    let hv = |s: &str| HeaderValue::from_str(s).unwrap_or(HeaderValue::from_static(""));
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    h.insert(header::CONTENT_SECURITY_POLICY, hv(&CSP));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

pub fn routes() -> Router {
    Router::new()
        .route("/ui", get(page))
        .route("/", get(|| async { Redirect::temporary("/ui") }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_contained_and_hashed() {
        // exactly one inline script and style, no external resources, no inline handlers
        assert_eq!(inline_blocks(PAGE, "script").len(), 1);
        assert_eq!(inline_blocks(PAGE, "style").len(), 1);
        for bad in [
            "src=\"http",
            "href=\"http",
            "<link",
            "@import",
            " onclick=",
            " onload=",
            "style=\"",
            "innerHTML",
            "outerHTML",
            "insertAdjacentHTML",
            "document.write",
            "eval(",
            "new Function",
        ] {
            assert!(!PAGE.contains(bad), "page contains {bad:?}");
        }
        assert!(CSP.contains("script-src 'sha256-"));
        assert!(CSP.contains("default-src 'none'"));
        assert!(!CSP.contains("unsafe"));
        // window cards are labeled only by window length (no 主/次 prefix)
        assert!(
            PAGE.contains("\"5 小时窗口\"")
                && PAGE.contains("\"周窗口\"")
                && PAGE.contains("\" 天窗口\"")
        );
        assert!(!PAGE.contains("\"主\"") && !PAGE.contains("\"次\""));
        // error detail panel: fetched by id, rendered via textContent only, copy button
        assert!(PAGE.contains("/usage/v1/records/\"+r.request_id+\"/detail"));
        assert!(PAGE.contains("$(\"dbody\").textContent="));
        assert!(PAGE.contains("navigator.clipboard.writeText"));
    }
}
