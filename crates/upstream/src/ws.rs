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

/// Largest WebSocket frame payload of a turn sent upstream (codex-rs 0.169.0
/// `responses_websocket::send_message`, `FRAGMENT_SIZE`).
pub const FRAGMENT_SIZE: usize = 1024 * 1024;

/// Send one message upstream the way Codex 0.169.0 does: a text message over
/// [`FRAGMENT_SIZE`] goes out as one message in frames of at most 1 MiB (first `Text`, then
/// `Continue`, `FIN` on the last), each sent and flushed before the next. Splits are on
/// bytes, so a UTF-8 character can straddle two frames. With permessage-deflate the fork
/// compresses the fragments as one message (RSV1 on the first frame only). Anything else
/// (smaller text, binary, control frames) is a single `send`.
pub async fn send_message<S>(socket: &mut S, message: WsMessage) -> Result<(), tungstenite::Error>
where
    S: futures::Sink<WsMessage, Error = tungstenite::Error> + Unpin,
{
    use futures::SinkExt;
    use tungstenite::protocol::frame::coding::Data;
    use tungstenite::protocol::frame::{Fragment, Frame};

    let WsMessage::Text(text) = message else {
        return socket.send(message).await;
    };
    if text.len() <= FRAGMENT_SIZE {
        return socket.send(WsMessage::Text(text)).await;
    }
    let mut data = bytes::Bytes::from(text);
    let mut opcode = Data::Text;
    while !data.is_empty() {
        let payload = data.split_to(data.len().min(FRAGMENT_SIZE));
        let fragment = if data.is_empty() {
            Fragment::Final
        } else {
            Fragment::More
        };
        socket
            .send(WsMessage::Frame(Frame::message_fragment(
                payload, opcode, fragment,
            )))
            .await?;
        opcode = Data::Continue;
    }
    Ok(())
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
                UpstreamError::ConnectTimeout
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tungstenite::protocol::frame::coding::{Data, OpCode};

    const MIB: usize = 1024 * 1024;

    /// Same cases as codex-rs 0.169.0 `large_turn_is_one_text_message_in_bounded_frames`.
    #[tokio::test]
    async fn large_turn_is_one_text_message_in_bounded_frames() {
        let (tx, mut frames) = futures::channel::mpsc::unbounded();
        let mut socket = tx.sink_map_err(|_| tungstenite::Error::ConnectionClosed);

        let at_limit = WsMessage::text("a".repeat(MIB));
        send_message(&mut socket, at_limit.clone()).await.unwrap();
        assert_eq!(frames.next().await, Some(at_limit));
        // Binary is never split.
        let bin = WsMessage::Binary(vec![7u8; 2 * MIB + 1].into());
        send_message(&mut socket, bin.clone()).await.unwrap();
        assert_eq!(frames.next().await, Some(bin));

        let mut text = "a".repeat(MIB - 1);
        text.push('🌱');
        text.push_str(&"b".repeat(MIB));
        send_message(&mut socket, WsMessage::text(text.clone()))
            .await
            .unwrap();
        drop(socket);
        let chunks: Vec<_> = text.as_bytes().chunks(MIB).collect();
        assert_eq!(chunks.len(), 3);
        for (i, expected) in chunks.iter().enumerate() {
            let Some(WsMessage::Frame(frame)) = frames.next().await else {
                panic!("expected a data frame");
            };
            let opcode = if i == 0 { Data::Text } else { Data::Continue };
            assert_eq!(frame.header().opcode, OpCode::Data(opcode));
            assert_eq!(frame.header().is_final, i + 1 == chunks.len());
            assert_eq!(frame.payload(), *expected);
        }
        assert!(frames.next().await.is_none());
    }

    /// Records everything written through it (the client side of the wire).
    struct Rec<S>(S, Arc<Mutex<Vec<u8>>>);
    impl<S: AsyncRead + Unpin> AsyncRead for Rec<S> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }
    impl<S: AsyncWrite + Unpin> AsyncWrite for Rec<S> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let r = Pin::new(&mut self.0).poll_write(cx, buf);
            if let Poll::Ready(Ok(n)) = &r {
                self.1.lock().unwrap().extend_from_slice(&buf[..*n]);
            }
            r
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }

    /// (fin, rsv1, opcode, payload length) of masked client frames after the handshake.
    fn frame_shape(raw: &[u8]) -> Vec<(bool, bool, u8, usize)> {
        let start = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let mut b = &raw[start..];
        let mut out = Vec::new();
        while b.len() >= 2 {
            let (mut len, mut i) = ((b[1] & 0x7f) as usize, 2);
            if len == 126 {
                len = u16::from_be_bytes([b[2], b[3]]) as usize;
                i = 4;
            } else if len == 127 {
                len = u64::from_be_bytes(b[2..10].try_into().unwrap()) as usize;
                i = 10;
            }
            if b[1] & 0x80 != 0 {
                i += 4;
            }
            out.push((b[0] & 0x80 != 0, b[0] & 0x40 != 0, b[0] & 0x0f, len));
            b = &b[(i + len).min(b.len())..];
        }
        out
    }

    /// Our client config and fork against a tungstenite server: the wire shape of a 3 MiB+
    /// turn with and without permessage-deflate, and the server gets it back intact.
    #[tokio::test]
    async fn fragmented_turn_on_the_wire_with_and_without_deflate() {
        for deflate in [false, true] {
            let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = ln.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (s, _) = ln.accept().await.unwrap();
                let mut cfg = WebSocketConfig::default();
                if deflate {
                    cfg.extensions.permessage_deflate = Some(DeflateConfig::default());
                }
                cfg.max_message_size = Some(16 * MIB);
                let mut ws = tokio_tungstenite::accept_async_with_config(s, Some(cfg))
                    .await
                    .unwrap();
                let mut got = Vec::new();
                while let Some(Ok(m)) = ws.next().await {
                    match m {
                        WsMessage::Text(t) => got.push(t.to_string()),
                        WsMessage::Close(_) => break,
                        _ => {}
                    }
                }
                got
            });
            let raw = Arc::new(Mutex::new(Vec::new()));
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut cfg = websocket_config();
            if !deflate {
                cfg.extensions.permessage_deflate = None;
            }
            let (mut ws, resp) = tokio_tungstenite::client_async_with_config(
                format!("ws://{addr}/"),
                Rec(tcp, raw.clone()),
                Some(cfg),
            )
            .await
            .unwrap();
            assert_eq!(
                resp.headers().contains_key("sec-websocket-extensions"),
                deflate
            );
            // JSON-ish, repetitive (compresses well), a 3-byte character on each boundary.
            let mut text = String::new();
            while text.len() < 3 * MIB + 7 {
                let to_boundary = MIB - text.len() % MIB;
                if to_boundary == 1 {
                    text.push('中');
                } else {
                    text.push('x');
                }
            }
            let small = "{\"type\":\"response.create\"}".to_string();
            send_message(&mut ws, WsMessage::text(small.clone()))
                .await
                .unwrap();
            send_message(&mut ws, WsMessage::text(text.clone()))
                .await
                .unwrap();
            ws.close(None).await.unwrap();
            let got = server.await.unwrap();
            assert_eq!(got, vec![small.clone(), text.clone()], "deflate={deflate}");
            let shape: Vec<_> = frame_shape(&raw.lock().unwrap())
                .into_iter()
                .filter(|f| f.2 <= 2)
                .collect();
            let n = text.len().div_ceil(MIB);
            assert_eq!(shape.len(), 1 + n, "deflate={deflate}: {shape:?}");
            assert_eq!((shape[0].0, shape[0].1, shape[0].2), (true, deflate, 1));
            for (i, f) in shape[1..].iter().enumerate() {
                let first = i == 0;
                assert_eq!(f.0, i + 1 == n, "FIN only on the last fragment");
                assert_eq!(f.1, deflate && first, "RSV1 only on the first fragment");
                assert_eq!(f.2, if first { 1 } else { 0 });
                if deflate {
                    assert!(f.3 < MIB / 8, "compressed fragment: {f:?}");
                } else {
                    assert_eq!(f.3, (text.len() - i * MIB).min(MIB));
                }
            }
        }
    }
}
