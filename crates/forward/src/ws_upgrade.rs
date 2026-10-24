//! Client-side WebSocket upgrade (RFC 6455 over HTTP/1.1) with `permessage-deflate`
//! (RFC 7692) negotiation.
//!
//! Codex always offers `permessage-deflate; client_max_window_bits`. axum's
//! `WebSocketUpgrade` never answers `Sec-WebSocket-Extensions`, so this extractor performs
//! the same handshake checks (same rejection statuses and texts), accepts the client's first
//! acceptable offer when `ws_compression` is on, and builds the tungstenite stream with the
//! agreed parameters. Without an acceptable offer the connection is plain.
//!
//! HTTP/2 extended CONNECT (RFC 8441) is not served: the listener speaks HTTP/1.1 only.
use axum::extract::FromRequestParts;
use axum::response::{IntoResponse, Response};
use http::{header, request::Parts, HeaderMap, HeaderValue, Method, StatusCode};
use hyper_util::rt::TokioIo;
use std::future::Future;
use tokio_tungstenite::WebSocketStream;
use tungstenite::extensions::compression::deflate::DeflateConfig;
use tungstenite::handshake::derive_accept_key;
use tungstenite::protocol::{Role, WebSocketConfig};

pub(crate) type DownWs = WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>;

const EXT: &str = "permessage-deflate";
/// Window sizes the deflate backend supports (RFC 7692 allows 8..=15).
const BITS: std::ops::RangeInclusive<u8> = 9..=15;

pub(crate) struct Upgrade {
    key: HeaderValue,
    on_upgrade: hyper::upgrade::OnUpgrade,
    offers: Vec<HeaderValue>,
}

fn rejection(status: StatusCode, body: &'static str) -> Response {
    (status, body).into_response()
}

fn header_contains(h: &HeaderMap, name: header::HeaderName, token: &str) -> bool {
    h.get(name)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.to_ascii_lowercase().contains(token))
}

fn header_eq(h: &HeaderMap, name: header::HeaderName, value: &str) -> bool {
    h.get(name)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(value.as_bytes()))
}

impl<S: Send + Sync> FromRequestParts<S> for Upgrade {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        if parts.method != Method::GET {
            return Err(rejection(
                StatusCode::METHOD_NOT_ALLOWED,
                "Request method must be `GET`",
            ));
        }
        if !header_contains(&parts.headers, header::CONNECTION, "upgrade") {
            return Err(rejection(
                StatusCode::BAD_REQUEST,
                "Connection header did not include 'upgrade'",
            ));
        }
        if !header_eq(&parts.headers, header::UPGRADE, "websocket") {
            return Err(rejection(
                StatusCode::BAD_REQUEST,
                "`Upgrade` header did not include 'websocket'",
            ));
        }
        let Some(key) = parts.headers.get(header::SEC_WEBSOCKET_KEY).cloned() else {
            return Err(rejection(
                StatusCode::BAD_REQUEST,
                "`Sec-WebSocket-Key` header missing",
            ));
        };
        if !header_eq(&parts.headers, header::SEC_WEBSOCKET_VERSION, "13") {
            return Err(rejection(
                StatusCode::BAD_REQUEST,
                "`Sec-WebSocket-Version` header did not include '13'",
            ));
        }
        let Some(on_upgrade) = parts.extensions.remove::<hyper::upgrade::OnUpgrade>() else {
            return Err(rejection(
                StatusCode::UPGRADE_REQUIRED,
                "WebSocket request couldn't be upgraded since no upgrade state was present",
            ));
        };
        let offers = parts
            .headers
            .get_all(header::SEC_WEBSOCKET_EXTENSIONS)
            .iter()
            .cloned()
            .collect();
        Ok(Self {
            key,
            on_upgrade,
            offers,
        })
    }
}

/// Server answer to one `permessage-deflate` offer (its parameters, `;`-split), or `None` to
/// decline it. Mirrors tungstenite's own server negotiation: no_context_takeover requests are
/// honored and echoed, `server_max_window_bits=N` limits our compressor, a valued
/// `client_max_window_bits=N` sizes our decompressor; unknown, duplicate, valueless
/// `server_max_window_bits` or out-of-range parameters decline the offer.
fn accept_offer<'a>(params: impl Iterator<Item = &'a str>) -> Option<(DeflateConfig, String)> {
    let mut cfg = DeflateConfig::default();
    let (mut s_nct, mut c_nct, mut s_bits, mut c_bits) = (false, false, None, None);
    let mut seen: Vec<&str> = Vec::new();
    for p in params {
        let (k, v) = match p.split_once('=') {
            Some((k, v)) => (k.trim(), Some(v.trim().trim_matches('"'))),
            None => (p.trim(), None),
        };
        if seen.contains(&k) {
            return None;
        }
        seen.push(k);
        let bits = |v: Option<&str>| {
            v.and_then(|v| v.parse::<u8>().ok())
                .filter(|b| BITS.contains(b))
        };
        match (k, v) {
            ("server_no_context_takeover", None) => s_nct = true,
            ("client_no_context_takeover", None) => c_nct = true,
            ("server_max_window_bits", v) => s_bits = Some(bits(v)?),
            ("client_max_window_bits", None) => {}
            ("client_max_window_bits", v) => c_bits = Some(bits(v)?),
            _ => return None,
        }
    }
    let mut resp = String::from(EXT);
    if s_nct {
        cfg.server_no_context_takeover = true;
        resp.push_str("; server_no_context_takeover");
    }
    if c_nct {
        cfg.client_no_context_takeover = true;
        resp.push_str("; client_no_context_takeover");
    }
    if let Some(b) = s_bits {
        cfg = cfg.set_max_window_bits(Role::Server, b).ok()?;
        resp.push_str(&format!("; server_max_window_bits={b}"));
    }
    if let Some(b) = c_bits.filter(|b| *b != 15) {
        cfg = cfg.set_max_window_bits(Role::Client, b).ok()?;
        resp.push_str(&format!("; client_max_window_bits={b}"));
    }
    Some((cfg, resp))
}

/// First acceptable `permessage-deflate` offer of `Sec-WebSocket-Extensions` (client
/// preference order) → (server-side config, response header value).
pub(crate) fn negotiate(offers: &[HeaderValue]) -> Option<(DeflateConfig, String)> {
    offers
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .find_map(|offer| {
            let mut it = offer.split(';').map(str::trim);
            (it.next() == Some(EXT)).then_some(())?;
            accept_offer(it.filter(|p| !p.is_empty()))
        })
}

impl Upgrade {
    /// 101 response; `cb` runs with the stream once hyper hands the connection over.
    /// `compression`: accept a `permessage-deflate` offer (`ws_compression`).
    /// `max_message`: largest incoming message (and frame) in bytes, after inflating.
    pub(crate) fn on_upgrade<F, Fut>(self, compression: bool, max_message: usize, cb: F) -> Response
    where
        F: FnOnce(DownWs) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let agreed = if compression {
            negotiate(&self.offers)
        } else {
            None
        };
        let mut cfg = WebSocketConfig::default();
        cfg.extensions.permessage_deflate = agreed.as_ref().map(|(d, _)| *d);
        cfg.max_message_size = Some(max_message);
        cfg.max_frame_size = Some(max_message);
        let on = self.on_upgrade;
        tokio::spawn(async move {
            let Ok(up) = on.await else { return };
            let ws =
                WebSocketStream::from_raw_socket(TokioIo::new(up), Role::Server, Some(cfg)).await;
            cb(ws).await;
        });
        let mut r = Response::new(axum::body::Body::empty());
        *r.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        let h = r.headers_mut();
        h.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
        h.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        if let Ok(v) = HeaderValue::from_str(&derive_accept_key(self.key.as_bytes())) {
            h.insert(header::SEC_WEBSOCKET_ACCEPT, v);
        }
        if let Some(v) = agreed.and_then(|(_, ext)| HeaderValue::from_str(&ext).ok()) {
            h.insert(header::SEC_WEBSOCKET_EXTENSIONS, v);
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn neg(v: &[&'static str]) -> Option<String> {
        let hv: Vec<HeaderValue> = v.iter().map(|s| HeaderValue::from_static(s)).collect();
        negotiate(&hv).map(|x| x.1)
    }

    #[test]
    fn codex_offer() {
        assert_eq!(
            neg(&["permessage-deflate; client_max_window_bits"]).as_deref(),
            Some("permessage-deflate")
        );
        assert_eq!(
            neg(&["permessage-deflate"]).as_deref(),
            Some("permessage-deflate")
        );
    }

    #[test]
    fn params() {
        assert_eq!(
            neg(&["permessage-deflate; server_no_context_takeover; client_no_context_takeover"])
                .as_deref(),
            Some("permessage-deflate; server_no_context_takeover; client_no_context_takeover")
        );
        assert_eq!(
            neg(&["permessage-deflate; server_max_window_bits=10; client_max_window_bits=12"])
                .as_deref(),
            Some("permessage-deflate; server_max_window_bits=10; client_max_window_bits=12")
        );
        assert_eq!(
            neg(&["permessage-deflate; client_max_window_bits=15"]).as_deref(),
            Some("permessage-deflate")
        );
        assert_eq!(
            neg(&["permessage-deflate; server_max_window_bits=\"11\""]).as_deref(),
            Some("permessage-deflate; server_max_window_bits=11")
        );
        let (cfg, _) = negotiate(&[HeaderValue::from_static(
            "permessage-deflate; server_max_window_bits=10; client_no_context_takeover",
        )])
        .unwrap();
        assert_eq!(cfg.server_max_window_bits().get(), 10);
        assert_eq!(cfg.client_max_window_bits().get(), 15);
        assert!(cfg.client_no_context_takeover && !cfg.server_no_context_takeover);
    }

    #[test]
    fn declines_bad_offers_and_falls_back() {
        for bad in [
            "permessage-deflate; server_max_window_bits",
            "permessage-deflate; server_max_window_bits=8",
            "permessage-deflate; client_max_window_bits=16",
            "permessage-deflate; foo",
            "permessage-deflate; server_no_context_takeover=1",
            "permessage-deflate; server_no_context_takeover; server_no_context_takeover",
            "x-webkit-deflate-frame",
        ] {
            assert_eq!(neg(&[bad]), None, "{bad}");
        }
        assert_eq!(neg(&[]), None);
        // Next offer in preference order, also across header lines.
        assert_eq!(
            neg(&["permessage-deflate; foo, permessage-deflate; server_no_context_takeover"])
                .as_deref(),
            Some("permessage-deflate; server_no_context_takeover")
        );
        assert_eq!(
            neg(&["x-foo", "permessage-deflate; client_max_window_bits"]).as_deref(),
            Some("permessage-deflate")
        );
    }
}
