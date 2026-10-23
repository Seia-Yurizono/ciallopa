//! Optional per-account ChatGPT infrastructure cookie store (`[upstream_opts]
//! cloudflare_cookies`). Mirrors codex-rs `http-client/src/chatgpt_cloudflare_cookies.rs` +
//! `chatgpt_hosts.rs`: only Cloudflare service cookies and the `__oailb` routing cookie are
//! kept, only for `https` ChatGPT hosts, in memory, with `reqwest::cookie::Jar` semantics
//! (domain/path scope, `Max-Age`/`Expires`). Unlike Codex (one process-global jar) there is
//! one store per account, so infrastructure cookies never link two accounts. Account,
//! session or auth cookies are never stored.
use http::header::SET_COOKIE;
use http::{HeaderMap, HeaderValue};
use reqwest::cookie::{CookieStore, Jar};

/// codex-rs `chatgpt_hosts::is_allowed_chatgpt_host`.
pub fn is_allowed_chatgpt_host(host: &str) -> bool {
    const EXACT_HOSTS: &[&str] = &["chatgpt.com", "chat.openai.com", "chatgpt-staging.com"];
    const SUBDOMAIN_SUFFIXES: &[&str] = &[".chatgpt.com", ".chatgpt-staging.com"];
    EXACT_HOSTS.contains(&host) || SUBDOMAIN_SUFFIXES.iter().any(|s| host.ends_with(s))
}

/// codex-rs `is_allowed_cloudflare_cookie_name` (codex main 400f235).
pub fn is_allowed_cloudflare_cookie_name(name: &str) -> bool {
    matches!(
        name,
        "__cf_bm"
            | "__cflb"
            | "__cfruid"
            | "__cfseq"
            | "__cfwaitingroom"
            | "__oailb"
            | "_cfuvid"
            | "cf_clearance"
            | "cf_ob_info"
            | "cf_use_ob"
    ) || name.starts_with("cf_chl_")
}

fn set_cookie_name(header: &str) -> Option<&str> {
    let (name, _) = header.split_once('=')?;
    let name = name.trim();
    (!name.is_empty()).then_some(name)
}

fn is_allowed_set_cookie(h: &HeaderValue) -> bool {
    h.to_str()
        .ok()
        .and_then(set_cookie_name)
        .is_some_and(is_allowed_cloudflare_cookie_name)
}

fn only_cloudflare_cookies(header: HeaderValue) -> Option<HeaderValue> {
    let header = header.to_str().ok()?;
    let cookies = header
        .split(';')
        .filter_map(|c| {
            let c = c.trim();
            let name = c.split_once('=')?.0.trim();
            is_allowed_cloudflare_cookie_name(name).then_some(c)
        })
        .collect::<Vec<_>>()
        .join("; ");
    if cookies.is_empty() {
        None
    } else {
        HeaderValue::from_str(&cookies).ok()
    }
}

/// One account's jar.
#[derive(Debug, Default)]
pub struct CookieJar {
    jar: Jar,
    /// Tests only: treat any http(s)/ws(s) host as a ChatGPT https host (mock upstreams).
    any_host: bool,
}

impl CookieJar {
    pub fn new(any_host: bool) -> Self {
        Self {
            jar: Jar::default(),
            any_host,
        }
    }

    /// codex-rs `chatgpt_cookie_url`: `wss` has the HTTPS cookie scope; only https ChatGPT
    /// hosts qualify.
    fn cookie_url(&self, url: &str) -> Option<reqwest::Url> {
        let mut u = reqwest::Url::parse(url).ok()?;
        if self.any_host {
            if matches!(u.scheme(), "http" | "ws" | "wss") {
                u.set_scheme("https").ok()?;
            }
        } else if u.scheme() == "wss" {
            u.set_scheme("https").ok()?;
        }
        if u.scheme() != "https" {
            return None;
        }
        let host = u.host_str()?;
        (self.any_host || is_allowed_chatgpt_host(host)).then_some(u)
    }

    /// `Cookie` header value for a request to `url` (HTTP or WS handshake), if any.
    pub fn header_for(&self, url: &str) -> Option<HeaderValue> {
        let u = self.cookie_url(url)?;
        let mut v = self.jar.cookies(&u).and_then(only_cloudflare_cookies)?;
        v.set_sensitive(true);
        Some(v)
    }

    /// Keep allowlisted `Set-Cookie`s from a response (HTTP, WS handshake or rejected upgrade).
    pub fn store_from(&self, url: &str, headers: &HeaderMap) {
        let Some(u) = self.cookie_url(url) else {
            return;
        };
        let mut it = headers
            .get_all(SET_COOKIE)
            .iter()
            .filter(|h| is_allowed_set_cookie(h));
        self.jar.set_cookies(&mut it, &u);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(j: &CookieJar, url: &str, v: &'static str) {
        let mut h = HeaderMap::new();
        h.append(SET_COOKIE, HeaderValue::from_static(v));
        j.store_from(url, &h);
    }
    const R: &str = "https://chatgpt.com/backend-api/codex/responses";

    #[test]
    fn allowlist_scope_expiry() {
        let j = CookieJar::new(false);
        set(
            &j,
            R,
            "__oailb=route; Path=/backend-api; Max-Age=3600; Secure; HttpOnly",
        );
        set(
            &j,
            R,
            "__Secure-next-auth.session-token=secret; Path=/; Secure",
        );
        set(&j, R, "cf_chl_rc_i=1; Path=/");
        let h = j
            .header_for("https://chatgpt.com/backend-api/wham/usage")
            .unwrap();
        let mut got: Vec<&str> = h.to_str().unwrap().split("; ").collect();
        got.sort();
        assert_eq!(got, vec!["__oailb=route", "cf_chl_rc_i=1"]);
        assert!(h.is_sensitive());
        // wss shares the https scope; other hosts / plain http never get them.
        assert!(j
            .header_for("wss://chatgpt.com/backend-api/codex/responses")
            .is_some());
        for u in [
            "http://chatgpt.com/backend-api/x",
            "ws://chatgpt.com/backend-api/x",
            "https://api.openai.com/backend-api/x",
            "https://evilchatgpt.com/backend-api/x",
        ] {
            assert!(
                j.header_for(u)
                    .is_none_or(|h| !h.to_str().unwrap().contains("__oailb")),
                "{u}"
            );
        }
        set(&j, R, "__oailb=; Path=/backend-api; Max-Age=0");
        set(&j, R, "cf_chl_rc_i=; Path=/; Max-Age=0");
        assert!(j.header_for(R).is_none());
    }

    #[test]
    fn non_chatgpt_responses_are_ignored() {
        let j = CookieJar::new(false);
        set(
            &j,
            "https://api.openai.com/v1/responses",
            "_cfuvid=v; Path=/",
        );
        set(&j, "http://chatgpt.com/backend-api/x", "_cfuvid=v; Path=/");
        assert!(j.header_for(R).is_none());
    }

    #[test]
    fn hosts() {
        for h in [
            "chatgpt.com",
            "a.chatgpt.com",
            "chat.openai.com",
            "api.chatgpt-staging.com",
        ] {
            assert!(is_allowed_chatgpt_host(h));
        }
        for h in [
            "evilchatgpt.com",
            "chatgpt.com.evil.example",
            "api.openai.com",
            "foo.chat.openai.com",
        ] {
            assert!(!is_allowed_chatgpt_host(h));
        }
    }
}
