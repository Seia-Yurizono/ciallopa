//! Built-in management UI: one static single-page document served at `/ui`, `/ui/requests`,
//! `/ui/accounts`, `/ui/settings` and `/ui/system` (client-side routing). It is assembled from
//! `ui/index.html` plus the stylesheets and scripts under `ui/` (see `ui/README.md`), all inline,
//! no external resources. The page holds no data: it asks for the access key (sessionStorage, sent
//! as `Authorization: Bearer`) and calls the key-protected `/usage/v1/*` and `/admin/v1/*` APIs.
//!
//! CSP allows exactly the page's inline `<script>` / `<style>` by SHA-256 hash; everything
//! else is `'none'` except `connect-src 'self'`.
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;
use base64::Engine;
use sha2::{Digest, Sha256};
use std::sync::LazyLock;

#[path = "runtime_api.rs"]
mod runtime_api;
pub use runtime_api::routes as runtime_routes;

// The page manifest: one document, its stylesheets and scripts concatenated in this order.
// scripts/lib/page.cjs (Node checks and the dev server) parses these include_str! lines, so this
// list is the single source of the load order.
const SHELL: &str = include_str!("ui/index.html");
const CSS: &str = concat!(
    include_str!("ui/css/tokens.css"),
    include_str!("ui/css/base.css"),
    include_str!("ui/css/components.css"),
    include_str!("ui/css/charts.css"),
    include_str!("ui/css/shell.css"),
    include_str!("ui/css/overview.css"),
    include_str!("ui/css/requests.css"),
    include_str!("ui/css/accounts.css"),
    include_str!("ui/css/settings.css"),
    include_str!("ui/css/system.css"),
);
const JS: &str = concat!(
    include_str!("ui/js/core.js"),
    include_str!("ui/js/i18n.js"),
    include_str!("ui/js/i18n/zh.js"),
    include_str!("ui/js/i18n/en.js"),
    include_str!("ui/js/api.js"),
    include_str!("ui/js/components.js"),
    include_str!("ui/js/charts.js"),
    include_str!("ui/js/shell.js"),
    include_str!("ui/js/views/overview.js"),
    include_str!("ui/js/views/requests/text.js"),
    include_str!("ui/js/views/requests/model.js"),
    include_str!("ui/js/views/requests/detail.js"),
    include_str!("ui/js/views/requests.js"),
    include_str!("ui/js/views/accounts/i18n.js"),
    include_str!("ui/js/views/accounts/model.js"),
    include_str!("ui/js/views/accounts/history.js"),
    include_str!("ui/js/views/accounts/detail.js"),
    include_str!("ui/js/views/accounts/add.js"),
    include_str!("ui/js/views/accounts.js"),
    include_str!("ui/js/views/settings/text.js"),
    include_str!("ui/js/views/settings/model.js"),
    include_str!("ui/js/views/settings/controls.js"),
    include_str!("ui/js/views/settings/panels.js"),
    include_str!("ui/js/views/settings.js"),
    include_str!("ui/js/views/system.js"),
    include_str!("ui/js/main.js"),
);

/// Client-side routes: every path serves the same document; the page routes with the History API.
pub const PATHS: [&str; 5] = [
    "/ui",
    "/ui/requests",
    "/ui/accounts",
    "/ui/settings",
    "/ui/system",
];

static PAGE: LazyLock<String> =
    LazyLock::new(|| SHELL.replace("/*@css*/", CSS).replace("/*@js*/", JS));

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

fn hashes(page: &str, tag: &str) -> String {
    inline_blocks(page, tag)
        .iter()
        .map(|b| {
            let h = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(b.as_bytes()));
            format!("'sha256-{h}'")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// CSP of a page: exactly its own inline script and style, by SHA-256.
pub fn csp_for(page: &str) -> String {
    format!(
        "default-src 'none'; script-src {}; style-src {}; connect-src 'self'; img-src 'self' data:; \
         base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        hashes(page, "script"),
        hashes(page, "style")
    )
}

static CSP: LazyLock<String> = LazyLock::new(|| csp_for(&PAGE));

/// The document, gzip-compressed once (about a quarter of its size; phones load it over the network).
static PAGE_GZ: LazyLock<Vec<u8>> = LazyLock::new(|| {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    enc.write_all(PAGE.as_bytes())
        .and_then(|()| enc.finish())
        .unwrap_or_default()
});

fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|coding| {
            let mut parts = coding.split(';').map(str::trim);
            parts.next().is_some_and(|c| c.eq_ignore_ascii_case("gzip"))
                && !parts.any(|q| matches!(q, "q=0" | "q=0.0" | "q=0.00" | "q=0.000"))
        })
}

async fn page(headers: HeaderMap) -> Response {
    let gzip = accepts_gzip(&headers) && !PAGE_GZ.is_empty();
    let mut r = if gzip {
        PAGE_GZ.as_slice().into_response()
    } else {
        PAGE.as_str().into_response()
    };
    let h = r.headers_mut();
    if gzip {
        h.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    h.insert(header::VARY, HeaderValue::from_static("accept-encoding"));
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
    // `no-transform`: a CDN in front (Cloudflare Rocket Loader, Email Obfuscation, Web Analytics
    // beacon, ...) must not rewrite or inject scripts; the CSP would block them anyway, and a
    // rewritten inline script no longer matches its hash, leaving the page dead.
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-transform"),
    );
    r
}

/// Tab icon (the pages cannot `<link>` one under their CSP; browsers ask for this path).
const FAVICON: &str = "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='8' fill='#1c2024'/><path d='M20.5 11.2a7 7 0 1 0 0 9.6' fill='none' stroke='#fff' stroke-width='3.2' stroke-linecap='round'/><circle cx='23.4' cy='16' r='2.1' fill='#0090ff'/></svg>";

async fn favicon() -> Response {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        FAVICON,
    )
        .into_response()
}

pub fn routes() -> Router {
    LazyLock::force(&CSP);
    LazyLock::force(&PAGE_GZ);
    PATHS
        .iter()
        .fold(Router::new(), |r, path| r.route(path, get(page)))
        .route("/favicon.ico", get(favicon))
        .route("/", get(|| async { Redirect::temporary("/ui") }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_page_is_one_hashed_document() {
        assert_eq!(inline_blocks(&PAGE, "script").len(), 1);
        assert_eq!(inline_blocks(&PAGE, "style").len(), 1);
        assert!(!PAGE.contains("/*@css*/") && !PAGE.contains("/*@js*/"));
        // An inline "</script>" inside the bundle would end the block early.
        assert!(!JS.contains("</script") && !CSS.contains("</style"));
        for tag in ["script", "style"] {
            let h = hashes(&PAGE, tag);
            assert!(h.starts_with("'sha256-") && CSP.contains(&h), "{tag}");
        }
        assert!(!CSP.contains("unsafe"));
    }

    #[test]
    fn ui_favicon_is_plain_svg() {
        assert!(
            FAVICON.starts_with("<svg")
                && !FAVICON.contains("<script")
                && !FAVICON.contains("<text")
        );
    }

    #[test]
    fn gzip_is_negotiated_and_round_trips() {
        use std::io::Read;
        let with = |v: &'static str| {
            let mut h = HeaderMap::new();
            h.insert(header::ACCEPT_ENCODING, HeaderValue::from_static(v));
            accepts_gzip(&h)
        };
        assert!(with("gzip, deflate, br") && with("br;q=1.0, GZIP;q=0.8"));
        assert!(!with("br, deflate") && !with("gzip;q=0, br") && !accepts_gzip(&HeaderMap::new()));
        let mut out = String::new();
        flate2::read::GzDecoder::new(PAGE_GZ.as_slice())
            .read_to_string(&mut out)
            .unwrap();
        assert_eq!(out, *PAGE);
        assert!(PAGE_GZ.len() * 2 < PAGE.len());
    }
}
