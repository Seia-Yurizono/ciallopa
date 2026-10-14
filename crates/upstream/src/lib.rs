//! Upstream HTTP/WS client. **Owner: forwarding worker** (with `ciallopa-forward`).
//!
//! Construction mirrors codex-rs `http-client/src/client_builder.rs::base_reqwest_builder`
//! and `utils/rustls-provider`: plain `reqwest::Client::builder()`, aws-lc-rs installed as
//! the process-wide rustls provider, `use_rustls_tls()`, native roots, no h2/keepalive
//! tuning (reqwest/hyper defaults). Anything that changes the ClientHello or the h2
//! SETTINGS/WINDOW_UPDATE/pseudo-header order must keep scripts/fp-check.sh green.
use ciallopa_core::Config;
use std::sync::Once;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("build client: {0}")]
    Build(#[from] reqwest::Error),
    #[error("websocket: {0}")]
    Ws(#[from] Box<tungstenite::Error>),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

/// Same as codex-rs `ensure_rustls_crypto_provider` (aws-lc-rs ⇒ X25519MLKEM768 key share).
pub fn ensure_rustls_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClientOptions {
    /// Empty = direct (environment proxies are ignored on purpose).
    pub proxy_url: String,
    pub connect_timeout: Option<Duration>,
    /// Keep a per-account in-memory jar of ChatGPT infrastructure cookies (see [`cookies`]).
    pub cloudflare_cookies: bool,
}
impl From<&Config> for ClientOptions {
    fn from(c: &Config) -> Self {
        Self {
            proxy_url: c.proxy_url.clone(),
            connect_timeout: Some(Duration::from_secs(c.upstream_opts.connect_timeout_secs)),
            cloudflare_cookies: c.upstream_opts.cloudflare_cookies,
        }
    }
}

pub fn build_reqwest(opts: &ClientOptions) -> Result<reqwest::Client, UpstreamError> {
    ensure_rustls_crypto_provider();
    let mut b = reqwest::Client::builder().use_rustls_tls();
    // Proxy responses (3xx) must reach the client untouched.
    b = b.redirect(reqwest::redirect::Policy::none()).no_proxy();
    if !opts.proxy_url.is_empty() {
        b = b.proxy(reqwest::Proxy::all(&opts.proxy_url)?);
    }
    if let Some(t) = opts.connect_timeout {
        b = b.connect_timeout(t);
    }
    Ok(b.build()?)
}

/// One `reqwest::Client` (and therefore one connection pool) per account, lazily built with
/// the exact same [`build_reqwest`] settings, so accounts never share a TCP/TLS/h2
/// connection (no cross-account linkage via connection reuse) while the ClientHello and h2
/// fingerprint stay identical.
#[derive(Debug)]
pub struct ClientPool {
    opts: std::sync::Mutex<ClientOptions>,
    map: std::sync::Mutex<std::collections::HashMap<String, PoolEntry>>,
    any_cookie_host: std::sync::atomic::AtomicBool,
}
#[derive(Debug, Clone)]
struct PoolEntry {
    client: reqwest::Client,
    jar: Option<std::sync::Arc<cookies::CookieJar>>,
}
impl ClientPool {
    pub fn new(opts: ClientOptions) -> Self {
        Self {
            opts: std::sync::Mutex::new(opts),
            map: Default::default(),
            any_cookie_host: Default::default(),
        }
    }
    /// Tests only: let the cookie jars accept a non-ChatGPT (mock) upstream host.
    #[doc(hidden)]
    pub fn allow_any_cookie_host_for_tests(&self) {
        self.any_cookie_host
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn entry(&self, account: &str) -> Result<PoolEntry, UpstreamError> {
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = m.get(account) {
            return Ok(e.clone());
        }
        let opts = self.options();
        let e = PoolEntry {
            client: build_reqwest(&opts)?,
            jar: opts.cloudflare_cookies.then(|| {
                std::sync::Arc::new(cookies::CookieJar::new(
                    self.any_cookie_host
                        .load(std::sync::atomic::Ordering::SeqCst),
                ))
            }),
        };
        m.insert(account.to_string(), e.clone());
        Ok(e)
    }
    pub fn options(&self) -> ClientOptions {
        self.opts.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    /// Hot-reload. Proxy or connect-timeout changes drop every cached client (rebuilt lazily
    /// on next use; requests and WebSockets in flight keep the client they already hold).
    /// A cookie toggle only adds empty jars or drops all jars; connections are kept.
    /// Fails (and changes nothing) when a client cannot be built with `new`.
    pub fn reconfigure(&self, new: ClientOptions) -> Result<(), UpstreamError> {
        build_reqwest(&new)?;
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let mut o = self.opts.lock().unwrap_or_else(|e| e.into_inner());
        if o.proxy_url != new.proxy_url || o.connect_timeout != new.connect_timeout {
            m.clear();
        } else if o.cloudflare_cookies != new.cloudflare_cookies {
            let any = self
                .any_cookie_host
                .load(std::sync::atomic::Ordering::SeqCst);
            for e in m.values_mut() {
                e.jar = new
                    .cloudflare_cookies
                    .then(|| std::sync::Arc::new(cookies::CookieJar::new(any)));
            }
        }
        *o = new;
        Ok(())
    }
    /// The account's own client, created on first use.
    pub fn get(&self, account: &str) -> Result<reqwest::Client, UpstreamError> {
        Ok(self.entry(account)?.client)
    }
    /// The account's cookie jar (`None` when `cloudflare_cookies` is off).
    pub fn jar(&self, account: &str) -> Option<std::sync::Arc<cookies::CookieJar>> {
        self.entry(account).ok()?.jar
    }
    /// Drop clients (and their idle connections) of accounts that no longer exist.
    pub fn retain<F: Fn(&str) -> bool>(&self, keep: F) {
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        m.retain(|k, _| keep(k));
    }
    pub fn remove(&self, account: &str) {
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        m.remove(account);
    }
    pub fn len(&self) -> usize {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub mod cookies;
pub mod retry;
pub mod ws;

pub use ws::{close as ws_close, UpstreamWs, WsMessage};

/// Shared upstream handle placed in `AppState`.
#[derive(Clone)]
pub struct UpstreamClient {
    pub base_url: String,
    /// Account-less client (not used for account traffic).
    pub http: reqwest::Client,
    /// Current options (swapped by [`Self::reconfigure`]); WebSocket connects read them.
    pub opts: std::sync::Arc<arc_swap::ArcSwap<ClientOptions>>,
    /// Per-account clients; all account-authenticated HTTP goes through [`Self::http_for`].
    pub pools: std::sync::Arc<ClientPool>,
}
impl UpstreamClient {
    pub fn new(cfg: &Config) -> Result<Self, UpstreamError> {
        let opts = ClientOptions::from(cfg);
        Ok(Self {
            base_url: cfg.upstream.trim_end_matches('/').to_string(),
            http: build_reqwest(&opts)?,
            pools: std::sync::Arc::new(ClientPool::new(opts.clone())),
            opts: std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(opts)),
        })
    }
    /// Hot-reload of `proxy_url` / `connect_timeout_secs` / `cloudflare_cookies`.
    pub fn reconfigure(&self, cfg: &Config) -> Result<(), UpstreamError> {
        let o = ClientOptions::from(cfg);
        self.pools.reconfigure(o.clone())?;
        self.opts.store(std::sync::Arc::new(o));
        Ok(())
    }
    /// The account's own client/connection pool (lazy). Falls back to the shared client only
    /// if building fails, which cannot happen for a config that already built `http`.
    pub fn http_for(&self, account: &str) -> reqwest::Client {
        self.pools
            .get(account)
            .unwrap_or_else(|_| self.http.clone())
    }
    /// `base_url` + `path` (path starts with `/`, may carry a query).
    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
    /// `Cookie` header for `account`'s request to `url` (None when disabled / empty).
    pub fn cookie_header(&self, account: &str, url: &str) -> Option<http::HeaderValue> {
        self.pools.jar(account)?.header_for(url)
    }
    /// Remember allowlisted `Set-Cookie`s from `account`'s upstream response.
    pub fn store_cookies(&self, account: &str, url: &str, headers: &http::HeaderMap) {
        if let Some(j) = self.pools.jar(account) {
            j.store_from(url, headers);
        }
    }
    /// Account-scoped WS connect: adds the account's cookies to the handshake (if enabled and
    /// the caller sent none) and stores `Set-Cookie` from the handshake response, including a
    /// rejected upgrade (codex-rs `websocket-client` behaviour).
    pub async fn ws_connect_as(
        &self,
        account: &str,
        path: &str,
        mut headers: http::HeaderMap,
    ) -> Result<(UpstreamWs, http::Response<Option<Vec<u8>>>), UpstreamError> {
        let url = self.ws_url(path);
        if !headers.contains_key(http::header::COOKIE) {
            if let Some(c) = self.cookie_header(account, &url) {
                headers.insert(http::header::COOKIE, c);
            }
        }
        let res = self.ws_connect(path, headers).await;
        match &res {
            Ok((_, r)) => self.store_cookies(account, &url, r.headers()),
            Err(e) => {
                if let Some((_, h, _)) = ws::handshake_status(e) {
                    self.store_cookies(account, &url, &h);
                }
            }
        }
        res
    }
    /// WebSocket connect mirroring codex-rs `codex-websocket-client` (tokio-tungstenite fork,
    /// tungstenite default rustls connector on the aws-lc-rs provider, permessage-deflate
    /// offered, TCP_NODELAY) through the global egress proxy (http / socks5 / socks5h).
    pub async fn ws_connect(
        &self,
        path: &str,
        headers: http::HeaderMap,
    ) -> Result<(UpstreamWs, http::Response<Option<Vec<u8>>>), UpstreamError> {
        ensure_rustls_crypto_provider();
        ws::connect(&self.ws_url(path), headers, &self.opts.load()).await
    }
    pub fn ws_url(&self, path: &str) -> String {
        let u = self.url(path);
        if let Some(r) = u.strip_prefix("https://") {
            format!("wss://{r}")
        } else if let Some(r) = u.strip_prefix("http://") {
            format!("ws://{r}")
        } else {
            u
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_with_and_without_proxy() {
        build_reqwest(&ClientOptions::default()).unwrap();
        build_reqwest(&ClientOptions {
            proxy_url: "socks5h://127.0.0.1:1080".into(),
            ..Default::default()
        })
        .unwrap();
        build_reqwest(&ClientOptions {
            proxy_url: "http://127.0.0.1:3128".into(),
            ..Default::default()
        })
        .unwrap();
    }

    #[test]
    fn pool_per_account_lazy_and_prunable() {
        let p = ClientPool::new(ClientOptions::default());
        assert!(p.is_empty());
        p.get("a").unwrap();
        p.get("a").unwrap();
        p.get("b").unwrap();
        assert_eq!(p.len(), 2);
        p.retain(|k| k == "a");
        assert_eq!(p.len(), 1);
        p.remove("a");
        assert!(p.is_empty());
    }

    #[test]
    fn reconfigure_rebuilds_only_when_needed() {
        let p = ClientPool::new(ClientOptions::default());
        let held = p.get("a").unwrap();
        p.reconfigure(ClientOptions {
            cloudflare_cookies: true,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(p.len(), 1, "cookie toggle keeps connections");
        assert!(p.jar("a").is_some());
        p.reconfigure(ClientOptions {
            proxy_url: "http://127.0.0.1:3128".into(),
            cloudflare_cookies: true,
            ..Default::default()
        })
        .unwrap();
        assert!(p.is_empty(), "proxy change drops cached clients");
        assert!(p
            .reconfigure(ClientOptions {
                proxy_url: "http://[::1".into(),
                ..Default::default()
            })
            .is_err());
        assert_eq!(p.options().proxy_url, "http://127.0.0.1:3128");
        drop(held);
    }
}
