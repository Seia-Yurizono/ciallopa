//! Built-in management pages: overview (`GET /ui`), settings (`/ui/settings`) and accounts
//! (`/ui/accounts`). Each is one static, self-contained HTML file (inline CSS/JS, no external
//! resources). The pages hold no data: they ask for the access key (sessionStorage, sent as
//! `Authorization: Bearer`) and call the key-protected `/usage/v1/*` and `/admin/v1/*` APIs.
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
pub const SETTINGS_PAGE: &str = include_str!("settings.html");
pub const ACCOUNTS_PAGE: &str = include_str!("accounts.html");
pub const PAGES: [&str; 3] = [PAGE, SETTINGS_PAGE, ACCOUNTS_PAGE];

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

pub static CSP: LazyLock<String> = LazyLock::new(|| csp_for(PAGE));
static CSPS: LazyLock<[String; 3]> = LazyLock::new(|| PAGES.map(csp_for));

async fn page() -> Response {
    serve(0)
}

fn serve(i: usize) -> Response {
    let mut r = PAGES[i].into_response();
    let h = r.headers_mut();
    let hv = |s: &str| HeaderValue::from_str(s).unwrap_or(HeaderValue::from_static(""));
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    h.insert(header::CONTENT_SECURITY_POLICY, hv(&CSPS[i]));
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
const FAVICON: &str = "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><defs><linearGradient id='g' x1='0' y1='0' x2='1' y2='1'><stop offset='0' stop-color='#5b7cff'/><stop offset='1' stop-color='#8b5cf6'/></linearGradient></defs><rect width='32' height='32' rx='8' fill='url(#g)'/><text x='16' y='22.5' font-family='system-ui,sans-serif' font-size='18' font-weight='700' text-anchor='middle' fill='#fff'>C</text></svg>";

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
    Router::new()
        .route("/ui", get(page))
        .route("/favicon.ico", get(favicon))
        .route("/ui/settings", get(|| async { serve(1) }))
        .route("/ui/accounts", get(|| async { serve(2) }))
        .route("/", get(|| async { Redirect::temporary("/ui") }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_contained_and_hashed() {
        for page in PAGES {
            // static table markup stays well-formed (every <thead> is closed)
            assert_eq!(
                page.matches("<thead>").count(),
                page.matches("</thead>").count()
            );
            assert!(!page.contains("\"ead>"));
            // exactly one inline script and style, no external resources, no inline handlers
            assert_eq!(inline_blocks(page, "script").len(), 1);
            assert_eq!(inline_blocks(page, "style").len(), 1);
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
                assert!(!page.contains(bad), "page contains {bad:?}");
            }
            let csp = csp_for(page);
            assert!(csp.contains("script-src 'sha256-") && !csp.contains("unsafe"));
            // every page links the other two
            for href in [
                "href=\"/ui\"",
                "href=\"/ui/settings\"",
                "href=\"/ui/accounts\"",
            ] {
                assert!(page.contains(href), "missing nav {href}");
            }
        }
        // write controls only exist behind the server-side switch; secrets never rendered
        assert!(SETTINGS_PAGE.contains("If-Match") && ACCOUNTS_PAGE.contains("confirm:a.id"));
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

    /// A second `function x(` in the same page script silently replaces the first (sloppy
    /// hoisting), as happened when the problem chips reused the reset-card chip's name.
    #[test]
    fn no_duplicate_function_names() {
        for page in PAGES {
            let script = inline_blocks(page, "script")[0];
            let mut seen = std::collections::HashSet::new();
            for part in script.split("function ").skip(1) {
                let name: String = part
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
                    .collect();
                if !name.is_empty() && part[name.len()..].starts_with('(') {
                    assert!(seen.insert(name.clone()), "function {name} declared twice");
                }
            }
        }
    }

    /// The shell (tokens, header, login, toasts, dialogs, click-again) is copied into each
    /// page between shared markers; the copies must not drift apart.
    #[test]
    fn shared_blocks_identical() {
        fn shared<'a>(page: &'a str, tag: &str) -> &'a str {
            let block = inline_blocks(page, tag)[0];
            let (a, b) = (
                block.find("/* ==== shared:begin").expect("shared:begin"),
                block
                    .find("/* ==== shared:end ==== */")
                    .expect("shared:end"),
            );
            &block[a..b]
        }
        for tag in ["style", "script"] {
            let first = shared(PAGES[0], tag);
            assert!(first.len() > 1000);
            for page in &PAGES[1..] {
                assert_eq!(shared(page, tag), first, "shared {tag} block differs");
            }
        }
        // the login key is shared by all pages of the tab
        for page in PAGES {
            assert!(page.contains("KS=\"ciallopa_key\"") && page.contains("sessionStorage"));
        }
    }
}
