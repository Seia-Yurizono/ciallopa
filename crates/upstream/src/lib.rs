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
    /// The egress proxy could not be reached or refused the tunnel (WebSocket dialer).
    #[error("egress proxy: {0}")]
    Proxy(String),
    /// Connecting through the egress proxy did not finish within the connect timeout.
    #[error("egress proxy: connect timeout")]
    ProxyTimeout,
    #[error("{0}")]
    Other(String),
}

/// Same as codex-rs `ensure_rustls_crypto_provider` (aws-lc-rs ⇒ X25519MLKEM768 key share).
/// The hybrid group is offered first only with rustls `prefer-post-quantum` (Cargo.toml), as
/// in Codex's dependency graph; JA4 does not show the difference, the ClientHello size does.
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
    /// `false`: IPv4 only for direct connections and for the proxy host. `true`: IPv6 too,
    /// with Happy Eyeballs.
    pub ipv6: bool,
}
impl From<&Config> for ClientOptions {
    fn from(c: &Config) -> Self {
        Self {
            proxy_url: c.effective_proxy(),
            connect_timeout: Some(Duration::from_secs(c.upstream_opts.connect_timeout_secs)),
            cloudflare_cookies: c.upstream_opts.cloudflare_cookies,
            ipv6: c.ipv6,
        }
    }
}

pub fn build_reqwest(opts: &ClientOptions) -> Result<reqwest::Client, UpstreamError> {
    ensure_rustls_crypto_provider();
    let mut b = reqwest::Client::builder().use_rustls_tls();
    // Proxy responses (3xx) must reach the client untouched.
    b = b.redirect(reqwest::redirect::Policy::none()).no_proxy();
    if !opts.proxy_url.is_empty() {
        let u = ciallopa_core::config::normalize_proxy_url(&opts.proxy_url);
        b = b.proxy(reqwest::Proxy::all(&u)?);
    }
    if !opts.ipv6 {
        // hyper-util's connector then only tries IPv4 addresses (target or proxy host).
        b = b.local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
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
    /// Cookie jars preserved across a proxy / timeout rebuild (clients are dropped).
    jars: std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<cookies::CookieJar>>>,
    any_cookie_host: std::sync::atomic::AtomicBool,
}
#[derive(Debug, Clone)]
struct PoolEntry {
    client: reqwest::Client,
    via_proxy: bool,
    jar: Option<std::sync::Arc<cookies::CookieJar>>,
}
impl ClientPool {
    pub fn new(opts: ClientOptions) -> Self {
        Self {
            opts: std::sync::Mutex::new(opts),
            map: Default::default(),
            jars: Default::default(),
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
        self.entry_with(account, build_reqwest)
    }
    fn entry_with(
        &self,
        account: &str,
        mut build: impl FnMut(&ClientOptions) -> Result<reqwest::Client, UpstreamError>,
    ) -> Result<PoolEntry, UpstreamError> {
        loop {
            let opts = {
                let m = self.map.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(e) = m.get(account) {
                    return Ok(e.clone());
                }
                self.options()
            };
            // Build outside the map lock: crypto / resolver initialization may block
            // briefly, while other accounts and configuration reloads must keep serving.
            let built = build(&opts);
            let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(e) = m.get(account) {
                return Ok(e.clone());
            }
            // A reload may have cleared the map while this client was being built.
            // Publish only under the configuration that produced it, including cookies.
            // Holding the map lock also keeps reconfigure from changing these options.
            if self.options() != opts {
                continue;
            }
            let client = built?;
            let jar = if opts.cloudflare_cookies {
                let mut jars = self.jars.lock().unwrap_or_else(|e| e.into_inner());
                Some(
                    jars.entry(account.to_string())
                        .or_insert_with(|| {
                            std::sync::Arc::new(cookies::CookieJar::new(
                                self.any_cookie_host
                                    .load(std::sync::atomic::Ordering::SeqCst),
                            ))
                        })
                        .clone(),
                )
            } else {
                None
            };
            let fresh = PoolEntry {
                client,
                via_proxy: !opts.proxy_url.is_empty(),
                jar,
            };
            m.insert(account.to_string(), fresh.clone());
            return Ok(fresh);
        }
    }
    pub fn options(&self) -> ClientOptions {
        self.opts.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    /// Hot-reload. Proxy or connect-timeout changes drop every cached *client* (rebuilt
    /// lazily on next use; in-flight requests / WebSockets keep the client they already
    /// hold) but **preserve** each account's cookie jar across the rebuild so a proxy
    /// change does not look like a brand-new Cloudflare client. A cookie toggle only
    /// adds empty jars or drops all jars; connections are kept. Fails (and changes
    /// nothing) when a client cannot be built with `new`.
    pub fn reconfigure(&self, new: ClientOptions) -> Result<(), UpstreamError> {
        build_reqwest(&new)?;
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let mut o = self.opts.lock().unwrap_or_else(|e| e.into_inner());
        let mut jars = self.jars.lock().unwrap_or_else(|e| e.into_inner());
        if o.proxy_url != new.proxy_url
            || o.connect_timeout != new.connect_timeout
            || o.ipv6 != new.ipv6
        {
            // Preserve jars (if cookies stay on), drop clients; next `entry()` rebuilds
            // outside this lock.
            if new.cloudflare_cookies {
                for (k, e) in m.drain() {
                    if let Some(j) = e.jar {
                        jars.insert(k, j);
                    }
                }
            } else {
                m.clear();
                jars.clear();
            }
        } else if o.cloudflare_cookies != new.cloudflare_cookies {
            jars.clear();
            if new.cloudflare_cookies {
                let any = self
                    .any_cookie_host
                    .load(std::sync::atomic::Ordering::SeqCst);
                for e in m.values_mut() {
                    e.jar = Some(std::sync::Arc::new(cookies::CookieJar::new(any)));
                }
            } else {
                for e in m.values_mut() {
                    e.jar = None;
                }
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
        let mut jars = self.jars.lock().unwrap_or_else(|e| e.into_inner());
        jars.retain(|k, _| keep(k));
    }
    pub fn remove(&self, account: &str) {
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        m.remove(account);
        self.jars
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(account);
    }
    pub fn len(&self) -> usize {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub mod cookies;
pub mod drain;
pub mod net;
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
    /// Egress generation: bumped (after `opts` is swapped) whenever the effective egress proxy
    /// changes. Long-lived upstream WebSockets watch it to reconnect between turns.
    pub egress: std::sync::Arc<tokio::sync::watch::Sender<u64>>,
    /// Graceful shutdown of open client WebSocket sessions (see [`drain`]).
    pub drain: std::sync::Arc<drain::Drain>,
}
impl UpstreamClient {
    pub fn new(cfg: &Config) -> Result<Self, UpstreamError> {
        let opts = ClientOptions::from(cfg);
        Ok(Self {
            base_url: cfg.upstream.trim_end_matches('/').to_string(),
            http: build_reqwest(&opts)?,
            pools: std::sync::Arc::new(ClientPool::new(opts.clone())),
            opts: std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(opts)),
            egress: std::sync::Arc::new(tokio::sync::watch::Sender::new(0)),
            drain: Default::default(),
        })
    }
    /// Hot-reload of `proxy_url` / `proxy_enabled` / `connect_timeout_secs` /
    /// `cloudflare_cookies`. A change of the effective egress proxy bumps the egress generation.
    pub fn reconfigure(&self, cfg: &Config) -> Result<(), UpstreamError> {
        let o = ClientOptions::from(cfg);
        self.pools.reconfigure(o.clone())?;
        let moved = {
            let cur = self.opts.load();
            cur.proxy_url != o.proxy_url || cur.ipv6 != o.ipv6
        };
        self.opts.store(std::sync::Arc::new(o));
        if moved {
            self.egress.send_modify(|g| *g += 1);
        }
        Ok(())
    }
    /// Current egress generation. Read it *before* connecting: a connection made with an
    /// older proxy then never carries the newer generation.
    pub fn egress_gen(&self) -> u64 {
        *self.egress.borrow()
    }
    /// Notified on every egress generation change.
    pub fn egress_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.egress.subscribe()
    }
    /// The account's own client/connection pool (lazy). Falls back to the shared client only
    /// if building fails, which cannot happen for a config that already built `http`.
    pub fn http_for(&self, account: &str) -> reqwest::Client {
        self.pools
            .get(account)
            .unwrap_or_else(|_| self.http.clone())
    }
    /// The account client and its actual egress, kept together across a configuration
    /// reload. Failure classification must describe this attempt, not the next client's
    /// configuration. Do not fall back to the startup client if rebuilding fails.
    pub fn http_for_attempt(
        &self,
        account: &str,
    ) -> Result<(reqwest::Client, bool), UpstreamError> {
        let entry = self.pools.entry(account)?;
        Ok((entry.client, entry.via_proxy))
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
    #[test]
    fn key_exchange_prefers_x25519mlkem768_like_codex() {
        let groups: Vec<_> = rustls::crypto::aws_lc_rs::default_provider()
            .kx_groups
            .iter()
            .map(|g| g.name())
            .collect();
        assert_eq!(
            groups,
            [
                rustls::NamedGroup::X25519MLKEM768,
                rustls::NamedGroup::X25519,
                rustls::NamedGroup::secp256r1,
                rustls::NamedGroup::secp384r1,
            ]
        );
    }
    use super::*;

    #[test]
    fn proxy_build_error_hides_credentials() {
        let secret = "SuperSecretPass";
        let e = build_reqwest(&ClientOptions {
            proxy_url: format!("http://user:{secret}@[::1"),
            ..Default::default()
        })
        .unwrap_err();
        let s = e.to_string();
        assert!(
            !s.contains(secret) && !s.contains("user:"),
            "proxy build error echoed credentials: {s}"
        );
    }

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
        assert!(p.is_empty(), "proxy change drops clients (rebuilt lazily)");
        assert!(p.jar("a").is_some(), "jar preserved across rebuild");
        assert!(p
            .reconfigure(ClientOptions {
                proxy_url: "http://[::1".into(),
                ..Default::default()
            })
            .is_err());
        assert_eq!(p.options().proxy_url, "http://127.0.0.1:3128");
        drop(held);
    }

    #[tokio::test]
    async fn reconfigure_during_lazy_build_cannot_publish_stale_client() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let pool = ClientPool::new(ClientOptions {
            proxy_url: "http://127.0.0.1:1".into(),
            cloudflare_cookies: true,
            ..Default::default()
        });
        let mut reload = true;
        let entry = pool
            .entry_with("a", |options| {
                // A reload can complete while this account's client is being built
                // outside the map lock. Reproduce that ordering without a timing race.
                if std::mem::take(&mut reload) {
                    pool.reconfigure(ClientOptions::default())?;
                }
                build_reqwest(options)
            })
            .unwrap();
        assert!(
            pool.jar("a").is_none(),
            "cookie storage was disabled by reload"
        );
        assert!(
            !entry.via_proxy,
            "egress metadata follows the rebuilt client"
        );

        // The returned client must also use the new direct route, not the old proxy.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut request = BufReader::new(socket);
            loop {
                let mut line = String::new();
                let n = request.read_line(&mut line).await.unwrap();
                if n == 0 || line == "\r\n" {
                    break;
                }
            }
            let mut socket = request.into_inner();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        });
        let response = entry
            .client
            .get(format!("http://{address}/"))
            .timeout(Duration::from_secs(3))
            .send()
            .await
            .unwrap();
        assert_eq!(response.text().await.unwrap(), "ok");
        server.await.unwrap();
    }
}
