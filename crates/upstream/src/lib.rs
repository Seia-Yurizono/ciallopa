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

#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    /// Empty = direct (environment proxies are ignored on purpose).
    pub proxy_url: String,
    pub connect_timeout: Option<Duration>,
}
impl From<&Config> for ClientOptions {
    fn from(c: &Config) -> Self {
        Self {
            proxy_url: c.proxy_url.clone(),
            connect_timeout: Some(Duration::from_secs(c.upstream_opts.connect_timeout_secs)),
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
    opts: ClientOptions,
    map: std::sync::Mutex<std::collections::HashMap<String, reqwest::Client>>,
}
impl ClientPool {
    pub fn new(opts: ClientOptions) -> Self {
        Self {
            opts,
            map: Default::default(),
        }
    }
    /// The account's own client, created on first use.
    pub fn get(&self, account: &str) -> Result<reqwest::Client, UpstreamError> {
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = m.get(account) {
            return Ok(c.clone());
        }
        let c = build_reqwest(&self.opts)?;
        m.insert(account.to_string(), c.clone());
        Ok(c)
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

pub mod retry;
pub mod ws;

pub use ws::{close as ws_close, UpstreamWs, WsMessage};

/// Shared upstream handle placed in `AppState`.
#[derive(Clone)]
pub struct UpstreamClient {
    pub base_url: String,
    /// Account-less client (not used for account traffic).
    pub http: reqwest::Client,
    pub opts: ClientOptions,
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
            opts,
        })
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
    /// WebSocket connect mirroring codex-rs `codex-websocket-client` (tokio-tungstenite fork,
    /// tungstenite default rustls connector on the aws-lc-rs provider, permessage-deflate
    /// offered, TCP_NODELAY) through the global egress proxy (http / socks5 / socks5h).
    pub async fn ws_connect(
        &self,
        path: &str,
        headers: http::HeaderMap,
    ) -> Result<(UpstreamWs, http::Response<Option<Vec<u8>>>), UpstreamError> {
        ensure_rustls_crypto_provider();
        ws::connect(&self.ws_url(path), headers, &self.opts).await
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
}
