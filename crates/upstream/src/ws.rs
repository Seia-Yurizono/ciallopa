//! Upstream Responses WebSocket (codex-rs `websocket-client/src/dialer.rs` equivalent).
use crate::{ClientOptions, UpstreamError};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{client_async_tls_with_config, MaybeTlsStream, WebSocketStream};
use tungstenite::extensions::compression::deflate::DeflateConfig;
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::protocol::WebSocketConfig;
use tungstenite::proxy::ProxyConfig;

pub use tungstenite::Message as WsMessage;
pub type UpstreamWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Same as codex-rs `responses_websocket::websocket_config` (permessage-deflate default).
pub fn websocket_config() -> WebSocketConfig {
    let mut extensions = ExtensionsConfig::default();
    extensions.permessage_deflate = Some(DeflateConfig::default());
    let mut config = WebSocketConfig::default();
    config.extensions = extensions;
    config
}

fn err(e: tungstenite::Error) -> UpstreamError {
    UpstreamError::Ws(Box::new(e))
}

pub(crate) async fn connect(
    url: &str,
    headers: http::HeaderMap,
    opts: &ClientOptions,
) -> Result<(UpstreamWs, http::Response<Option<Vec<u8>>>), UpstreamError> {
    let mut request = url.into_client_request().map_err(err)?;
    request.headers_mut().extend(headers);
    let uri = request.uri().clone();
    let host = uri
        .host()
        .ok_or_else(|| UpstreamError::Other("ws url without host".into()))?
        .to_string();
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("wss") {
            443
        } else {
            80
        });
    let fut = async {
        let tcp = if opts.proxy_url.is_empty() {
            crate::net::dial(&host, port, opts.ipv6).await?
        } else {
            if opts.proxy_url.starts_with("https://") {
                return Err(UpstreamError::Other(
                    "https:// egress proxy is not supported for websocket; use http:// or socks5h://".into(),
                ));
            }
            let u = ciallopa_core::config::normalize_proxy_url(&opts.proxy_url);
            let proxy = ProxyConfig::parse(&u).map_err(err)?;
            let authority = proxy.authority();
            let (ph, pp) = crate::net::split_authority(&authority)
                .ok_or_else(|| UpstreamError::Proxy(format!("bad proxy address {authority}")))?;
            let tcp = crate::net::dial(ph, pp, opts.ipv6)
                .await
                .map_err(|e| UpstreamError::Proxy(format!("connect to proxy: {e}")))?;
            tcp.set_nodelay(true)?;
            tokio_tungstenite::proxy::connect_via_proxy(tcp, &proxy, &host, port)
                .await
                .map_err(|e| UpstreamError::Proxy(format!("proxy handshake: {e}")))?
        };
        tcp.set_nodelay(true)?;
        client_async_tls_with_config(request, tcp, Some(websocket_config()), None)
            .await
            .map_err(err)
    };
    match opts.connect_timeout {
        Some(t) => tokio::time::timeout(t, fut).await.map_err(|_| {
            if opts.proxy_url.is_empty() {
                UpstreamError::Other("websocket connect timeout".into())
            } else {
                UpstreamError::ProxyTimeout
            }
        })?,
        None => fut.await,
    }
}

/// HTTP status of a failed handshake (e.g. 401/429), with body bytes.
pub fn handshake_status(e: &UpstreamError) -> Option<(u16, http::HeaderMap, Vec<u8>)> {
    match e {
        UpstreamError::Ws(b) => match b.as_ref() {
            tungstenite::Error::Http(r) => Some((
                r.status().as_u16(),
                r.headers().clone(),
                r.body().clone().unwrap_or_default(),
            )),
            _ => None,
        },
        _ => None,
    }
}

/// Close-frame types re-exported for the relay.
pub mod close {
    pub use tungstenite::protocol::frame::coding::CloseCode;
    pub use tungstenite::protocol::CloseFrame;
}
