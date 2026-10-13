//! Upstream HTTP/WS client. **Owner: forwarding worker** (with `cpa-lite-forward`).
//!
//! Construction mirrors codex-rs `http-client/src/client_builder.rs::base_reqwest_builder`
//! and `utils/rustls-provider`: plain `reqwest::Client::builder()`, aws-lc-rs installed as
//! the process-wide rustls provider, `use_rustls_tls()`, native roots, no h2/keepalive
//! tuning (reqwest/hyper defaults). Anything that changes the ClientHello or the h2
//! SETTINGS/WINDOW_UPDATE/pseudo-header order must keep scripts/fp-check.sh green.
use cpa_lite_core::Config;
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

pub mod retry;
pub mod ws;

pub use ws::{close as ws_close, UpstreamWs, WsMessage};

/// Shared upstream handle placed in `AppState`.
#[derive(Clone)]
pub struct UpstreamClient {
    pub base_url: String,
    pub http: reqwest::Client,
    pub opts: ClientOptions,
}
impl UpstreamClient {
    pub fn new(cfg: &Config) -> Result<Self, UpstreamError> {
        let opts = ClientOptions::from(cfg);
        Ok(Self {
            base_url: cfg.upstream.trim_end_matches('/').to_string(),
            http: build_reqwest(&opts)?,
            opts,
        })
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
}
