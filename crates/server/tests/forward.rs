//! Forwarding integration tests against a mock upstream (SSE, retry, 429 failover, 401
//! refresh, idle timeout, models cache, WebSocket relay + quota replay/close, Images API).
#![allow(clippy::unwrap_used)]
use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{
    body::Body,
    extract::State,
    http::HeaderMap,
    response::Response,
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use ciallopa_core::*;
use ciallopa_forward::AppState;
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const KEY: &str = "sk-cpa-0123456789abcdef";

/// Capture structured diagnostic fields without global subscriber state or new test deps.
#[derive(Clone, Default)]
struct StreamWarnings(Arc<Mutex<Vec<serde_json::Value>>>);

impl StreamWarnings {
    fn capture(&self) -> (tracing::Dispatch, tracing::subscriber::DefaultGuard) {
        // With just one registered dispatcher, tracing-core registers a new callsite
        // using the current thread's default. A parallel test without a subscriber can
        // then cache `never` for our shared warning callsite. Keep both the ordinary
        // no-subscriber context and this capture registered, so interests are combined.
        // Both are scoped to this test; no global default or suite-wide lock is needed.
        let ordinary = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let capture = tracing::subscriber::set_default(self.clone());
        (ordinary, capture)
    }
}

impl tracing::Subscriber for StreamWarnings {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.fields().field("terminal_received").is_some()
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(serde_json::Map<String, serde_json::Value>);
        impl tracing::field::Visit for Fields {
            fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
                self.0.insert(f.name().into(), v.into());
            }
            fn record_bool(&mut self, f: &tracing::field::Field, v: bool) {
                self.0.insert(f.name().into(), v.into());
            }
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                self.0.insert(f.name().into(), v.into());
            }
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                self.0.insert(f.name().into(), format!("{v:?}").into());
            }
        }
        let mut fields = Fields(serde_json::Map::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0.into());
    }
}

struct Store {
    ids: Vec<&'static str>,
    refreshed: AtomicUsize,
}
#[async_trait]
impl AccountStore for Store {
    async fn list(&self) -> Vec<Account> {
        self.ids
            .iter()
            .map(|i| Account {
                id: i.to_string(),
                email: None,
                chatgpt_account_id: Some(format!("acct-{i}")),
                plan_type: None,
                priority: 0,
                weight: 1,
                disabled: false,
            })
            .collect()
    }
    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        let gen = if self.refreshed.load(Ordering::SeqCst) > 0 {
            "2"
        } else {
            "1"
        };
        Ok(Credentials {
            account: id.clone(),
            access_token: format!("tok-{id}-{gen}"),
            chatgpt_account_id: Some(format!("acct-{id}")),
        })
    }
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.refreshed.fetch_add(1, Ordering::SeqCst);
        self.credentials(id).await
    }
    async fn reload(&self) -> Result<(), AuthError> {
        Ok(())
    }
}

#[derive(Default)]
struct Rec(Mutex<Vec<(String, Outcome)>>);
impl ciallopa_core::Router for Rec {
    fn select(&self, req: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError> {
        accounts
            .iter()
            .find(|a| !req.exclude.contains(&a.id))
            .map(|a| Selection {
                account: a.id.clone(),
                from_binding: false,
            })
            .ok_or(RouteError::NoAccounts)
    }
    fn report(&self, _: &RouteRequest, a: &AccountId, o: &Outcome) {
        self.0.lock().unwrap().push((a.clone(), o.clone()));
    }
}

type Seen = (Vec<(String, String)>, Bytes);

#[derive(Clone, Default)]
struct Mock {
    hits: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Seen>>>,
    /// (authorization, client peer port) per `/responses` hit: one port = one connection.
    peers: Arc<Mutex<Vec<(String, u16)>>>,
    /// Synchronize fault fixtures with their callers without waiting for arbitrary sleeps.
    ready: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
impl Mock {
    fn record(&self, h: &HeaderMap, b: &Bytes) -> usize {
        let hs = h
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        self.seen.lock().unwrap().push((hs, b.clone()));
        self.hits.fetch_add(1, Ordering::SeqCst)
    }
}

/// Upstream `invalid_encrypted_content` error object / SSE `error` event (real wording).
macro_rules! enc_obj {
    () => {
        r#"{"message":"The encrypted content for item rs_0abc could not be verified. Reason: Encrypted content organization_id did not match the target organization.","type":"invalid_request_error","param":"input","code":"invalid_encrypted_content"}"#
    };
}
macro_rules! enc_error {
    () => {
        concat!(r#"{"type":"error","error":"#, enc_obj!(), "}")
    };
}

fn sse_resp(body: &'static str) -> Response {
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("x-codex-primary-used-percent", "12")
        .body(Body::from(body))
        .unwrap()
}

/// `x-test-noct: 1` mimics chatgpt.com, which sends `/responses` SSE without `Content-Type`.
async fn up_responses(
    st: State<Mock>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    h: HeaderMap,
    b: Bytes,
) -> Response {
    let auth = h
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    st.peers.lock().unwrap().push((auth.clone(), peer.port()));
    let noct = h.contains_key("x-test-noct");
    let cookies = h.contains_key("x-test-cookies");
    let mut r = up_responses_inner(st, h, b).await;
    if cookies {
        let set: &[&str] = if auth.contains("tok-a") {
            &[
                "__cf_bm=A; Path=/; Secure; HttpOnly",
                "__oailb=A-lb; Path=/backend-api; Max-Age=1; Secure",
                "chatgpt_session=evil; Path=/; Secure",
            ]
        } else {
            &["__cf_bm=B; Path=/; Secure; HttpOnly"]
        };
        for c in set {
            r.headers_mut().append("set-cookie", c.parse().unwrap());
        }
    }
    if noct {
        r.headers_mut().remove("content-type");
    }
    r
}

async fn up_responses_inner(State(m): State<Mock>, h: HeaderMap, b: Bytes) -> Response {
    let n = m.record(&h, &b);
    let auth = h
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let mode = h
        .get("x-test-mode")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    const OVER: &str = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
        "event: error\ndata: {\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\",\"message\":\"Our servers are currently overloaded. Please try again later.\",\"sequence_number\":1}\n\n",
        "data: {\"type\":\"response.failed\",\"sequence_number\":2,\"response\":{\"id\":\"r-a\",\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Our servers are currently overloaded.\"}}}\n\n"
    );
    const OK_B: &str = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-b\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-b\"}}\n\n"
    );
    match mode.as_str() {
        mode if mode.starts_with("diag_") => {
            let mode = mode.to_string();
            let stream = futures::stream::unfold(0, move |i| {
                let mode = mode.clone();
                async move {
                    let bytes: &'static [u8] = match i {
                        0 if mode == "diag_unknown" => b"data: {\"type\":\"response.private-do-not-log\",\"value\":\"private-output\"}\n\n",
                        0 if mode.starts_with("diag_boot") => b"data: {\"type\":\"response.created\"}\n\n",
                        0 => b"data: {\"type\":\"response.created\"}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"private-output\"}\n\n",
                        1 => {
                            tokio::time::sleep(Duration::from_millis(120)).await;
                            b": private-heartbeat\n\n"
                        }
                        2 if mode == "diag_boot_idle" => futures::future::pending().await,
                        2 if mode == "diag_success" => b"data: {\"type\":\"response.completed\"}\n\n",
                        _ => return None,
                    };
                    Some((Ok::<_, std::io::Error>(Bytes::from_static(bytes)), i + 1))
                }
            });
            return Response::builder()
                .header("content-type", "text/event-stream")
                .header("x-request-id", "diag-upstream-id")
                .body(Body::from_stream(stream)).unwrap();
        }
        "empty_200" | "empty_204" => return empty_response(&mode),
        "json_body_stall" => {
            let stream = futures::stream::once(async { Ok::<_, std::io::Error>(Bytes::from_static(b"{\"id\":")) })
                .chain(futures::stream::pending());
            return Response::builder().header("content-type", "application/json").body(Body::from_stream(stream)).unwrap();
        }
        "image_failed" => {
            let mut r = sse_resp("data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"image_failed\",\"message\":\"image failed locally in fixture\"},\"usage\":{\"input_tokens\":99,\"output_tokens\":30}}}\n\n");
            r.headers_mut().insert("x-request-id", "image-upstream-id".parse().unwrap());
            return r;
        }
        "image_truncated" => return sse_resp("data: {\"type\":\"response.created\"}\n\n"),
        "image_reset" => {
            let s = futures::stream::once(async { Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.created\"}\n\n")) })
                .chain(futures::stream::once(async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Err(std::io::Error::other("connection reset"))
                }));
            return Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap();
        }
        "event_literal_complete" => return sse_resp(concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"response.completed\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":3}}}\n\n"
        )),
        "event_literal_truncated" => return sse_resp("data: {\"type\":\"response.output_text.delta\",\"delta\":\"response.completed\"}\n\n"),
        "event_metadata_only" => return sse_resp(concat!(
            "data: {\"type\":\"response.metadata\",\"note\":\"synthetic.delta\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":0}}}\n\n"
        )),
        "event_done" => return sse_resp("data: {\"type\":\"response.done\",\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":0}}}\n\n"),
        "event_spaced_error" => return sse_resp("data: { \"type\": \"error\", \"error\": {\"code\":\"bad_request\",\"message\":\"fixture rejected\"}}\n\n"),
        mode @ ("event_large_completed" | "event_large_literal" | "event_large_plain" | "event_large_held" | "event_large_delta_completed") => {
            let mut text = if mode == "event_large_completed" {
                String::from("data: {\"type\":\"response.completed\",\"response\":{\"padding\":\"")
            } else if mode == "event_large_plain" {
                String::from("data: {\"type\":\"response.output_text.delta\",\"delta\":\"ordinary output\",\"padding\":\"")
            } else {
                String::from("data: {\"type\":\"response.output_text.delta\",\"delta\":\"response.completed\",\"padding\":\"")
            };
            text.extend(std::iter::repeat_n('x', 9 * 1024 * 1024));
            if mode == "event_large_completed" {
                text.push_str("\",\"usage\":{\"input_tokens\":7,\"output_tokens\":0}}}\n\n");
            } else {
                text.push_str("\"}\n\n");
            }
            if mode == "event_large_delta_completed" {
                text.push_str("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":0}}}\n\n");
            }
            let body = if mode == "event_large_held" {
                Body::from_stream(futures::stream::once(async move {
                    Ok::<_, std::io::Error>(Bytes::from(text))
                }).chain(futures::stream::pending()))
            } else {
                Body::from(text)
            };
            return Response::builder().header("content-type", "text/event-stream")
                .body(body).unwrap();
        }
        "held_stream" => {
            let stream = futures::stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"held\"}\n\n"))
            }).chain(futures::stream::pending());
            return Response::builder().header("content-type", "text/event-stream")
                .body(Body::from_stream(stream)).unwrap();
        }
        "reload_idle" => {
            // Let the test reload after this request has started, before its headers.
            m.ready.notify_one();
            m.release.notified().await;
            let stream = futures::stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n"))
            }).chain(futures::stream::once(async {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"reload-idle\"}}\n\n"))
            }));
            return Response::builder().header("content-type", "text/event-stream")
                .body(Body::from_stream(stream)).unwrap();
        }
        "header_stall_a" if auth.contains("tok-a") => {
            m.ready.notify_one();
            m.release.notified().await;
            return sse_resp(OK_B);
        }
        "header_stall_a" => return sse_resp(OK_B),
        "preparation_shared" if auth.contains("tok-a") => {
            // A slow response header consumes most of the shared two-second budget.
            tokio::time::sleep(Duration::from_millis(1500)).await;
            return sse_resp("data: {\"type\":\"error\",\"error\":{\"type\":\"usage_limit_reached\",\"resets_in_seconds\":30}}\n\n");
        }
        "preparation_shared" => {
            m.ready.notify_one();
            m.release.notified().await;
            return sse_resp(OK_B);
        }
        "bootstrap_long_quota" if auth.contains("tok-a") => {
            let stream = futures::stream::unfold(0, |n| async move {
                let chunk = match n {
                    0 => b"data: {\"type\":\"response.created\"}\n\n".as_slice(),
                    1 => {
                        // Legitimate streaming/pre-output time is not preparation time.
                        tokio::time::sleep(Duration::from_millis(1200)).await;
                        b"data: {\"type\":\"error\",\"error\":{\"type\":\"usage_limit_reached\",\"resets_in_seconds\":30}}\n\n".as_slice()
                    }
                    _ => return None,
                };
                Some((Ok::<_, std::io::Error>(Bytes::from_static(chunk)), n + 1))
            });
            return Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(stream)).unwrap();
        }
        "bootstrap_long_quota" => return sse_resp(OK_B),
        "json_headers" => {
            m.ready.notify_one();
            m.release.notified().await;
            return Response::builder().header("content-type", "application/json")
                .body(Body::from(r#"{"id":"json-ok","status":"completed"}"#)).unwrap();
        }
        "error_too_large" => {
            return Response::builder().status(503).header("x-request-id", "oversized-upstream")
                .body(Body::from(vec![b'x'; 1024 * 1024 + 1])).unwrap();
        }
        "error_stall" => {
            let stream = futures::stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(b"partial error"))
            }).chain(futures::stream::pending());
            return Response::builder().status(503).header("x-request-id", "stalled-upstream")
                .body(Body::from_stream(stream)).unwrap();
        }
        "retry_then_overload" if n == 0 => {
            return Response::builder().status(503).body(Body::from("busy")).unwrap()
        }
        "retry_then_overload" if n == 1 => {
            let mut r = sse_resp(OVER);
            r.headers_mut().insert("retry-after", "0".parse().unwrap());
            return r;
        }
        "retry_then_overload" => return sse_resp(OK_B),
        // Real upstream shape (HTTP): 200 + SSE `error`, no preamble.
        "enc" => return sse_resp(concat!("event: error\ndata: ", enc_error!(), "\n\n")),
        "lateenc" => {
            return sse_resp(concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-e\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
                "event: error\ndata: ",
                enc_error!(),
                "\n\n"
            ))
        }
        // chatgpt Codex SSE: moderation / queue bookkeeping before any model output, then
        // overload or encrypted rejection (sub2api #8139).
        "metaover" if auth.contains("tok-a") => {
            return sse_resp(concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
                "data: {\"type\":\"response.queued\",\"response\":{\"id\":\"r-a\",\"status\":\"queued\"}}\n\n",
                "data: {\"type\":\"response.metadata\",\"response_id\":\"r-a\",\"metadata\":{\"openai_chatgpt_moderation_metadata\":{\"prompt\":{}}}}\n\n",
                "event: error\ndata: {\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\",\"message\":\"Our servers are currently overloaded. Please try again later.\",\"sequence_number\":1}\n\n",
                "data: {\"type\":\"response.failed\",\"sequence_number\":2,\"response\":{\"id\":\"r-a\",\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Our servers are currently overloaded.\"}}}\n\n"
            ))
        }
        "metaover" => return sse_resp(OK_B),
        // Real chatgpt shape: empty item / part announcements, then an overload (no output yet).
        "emptyadded_over" if auth.contains("tok-a") => {
            return sse_resp(concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
                "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"r-a\"}}\n\n",
                "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"rs_a\",\"type\":\"reasoning\",\"summary\":[]}}\n\n",
                "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"id\":\"msg_a\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n",
                "data: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_a\",\"output_index\":1,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"\"}}\n\n",
                "event: error\ndata: {\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\",\"message\":\"Our servers are currently overloaded. Please try again later.\",\"sequence_number\":1}\n\n",
                "data: {\"type\":\"response.failed\",\"sequence_number\":2,\"response\":{\"id\":\"r-a\",\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Our servers are currently overloaded.\"}}}\n\n"
            ))
        }
        "emptyadded_over" => return sse_resp(OK_B),
        // Tool-call arguments already streamed are real output: never replayed.
        "tooladded_over" => {
            return sse_resp(concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
                "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_a\",\"type\":\"function_call\",\"name\":\"shell\",\"call_id\":\"call_a\",\"arguments\":\"\"}}\n\n",
                "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_a\",\"output_index\":0,\"delta\":\"{\\\"cmd\"}\n\n",
                "event: error\ndata: {\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\",\"message\":\"overloaded\"}\n\n"
            ))
        }
        "repro_cf1010_text" | "repro_cf1010_json" | "repro_cf_challenge" => {
            let (ct, body) = match mode.as_str() {
                "repro_cf1010_text" => ("text/plain; charset=UTF-8", "error code: 1010".to_string()),
                "repro_cf1010_json" => (
                    "application/json",
                    r#"{"cloudflare_error":true,"error_code":1010,"error_name":"browser_signature_banned","detail":"The site owner has blocked access based on your browser's signature.","ray_id":"9a1b2c3d4e5f6789","zone":"chatgpt.com"}"#.to_string(),
                ),
                _ => ("text/html; charset=UTF-8", "<!DOCTYPE html><html><head><title>Just a moment...</title></head><body>cloudflare</body></html>".to_string()),
            };
            let mut r = Response::builder()
                .status(403)
                .header("content-type", ct)
                .header("cf-ray", "9a1b2c3d4e5f6789-SJC")
                .header("server", "cloudflare");
            if mode == "repro_cf_challenge" {
                r = r.header("cf-mitigated", "challenge");
            }
            return r.body(Body::from(body)).unwrap();
        }
        "real403" if auth.contains("tok-a") => {
            return Response::builder()
                .status(403)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"error":{"message":"You do not have access to this workspace.","type":"invalid_request_error","code":"forbidden"}}"#,
                ))
                .unwrap();
        }
        "real403" => return sse_resp(OK_B),
        "metaenc" => {
            return sse_resp(concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-e\"}}\n\n",
                "data: {\"type\":\"response.metadata\",\"response_id\":\"r-e\",\"metadata\":{\"openai_chatgpt_moderation_metadata\":{\"prompt\":{}}}}\n\n",
                "event: error\ndata: ",
                enc_error!(),
                "\n\n"
            ))
        }
        "metaok" => {
            return sse_resp(concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-m\"}}\n\n",
                "data: {\"type\":\"response.queued\",\"response\":{\"id\":\"r-m\",\"status\":\"queued\"}}\n\n",
                "data: {\"type\":\"response.metadata\",\"response_id\":\"r-m\",\"metadata\":{\"openai_chatgpt_moderation_metadata\":{\"prompt\":{\"flagged\":false}}}}\n\n",
                "data: {\"type\":\"keepalive\"}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-m\"}}\n\n"
            ))
        }
        "enc400" => {
            return Response::builder()
                .status(400)
                .header("content-type", "application/json")
                .body(Body::from(concat!(r#"{"error":"#, enc_obj!(), "}")))
                .unwrap()
        }
        "midover" if auth.contains("tok-a") => return sse_resp(OVER),
        "allover" => return sse_resp(OVER),
        "midover" => return sse_resp(OK_B),
        "lateover" => {
            return sse_resp(concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
                "data: {\"type\":\"error\",\"error\":{\"type\":\"service_unavailable_error\",\"code\":\"server_is_overloaded\",\"message\":\"overloaded\"}}\n\n"
            ))
        }
        "over400" if auth.contains("tok-a") => {
            return Response::builder()
                .status(400)
                .header("content-type", "application/json")
                .header("x-retry-metadata", "NO_MORE_RETRY")
                .body(Body::from(r#"{"error":{"message":"Our servers are currently overloaded. Please try again later."}}"#))
                .unwrap()
        }
        "over400" => return sse_resp(OK_B),
        "overonce" if n < 2 => return sse_resp(OVER),
        "overonce" => return sse_resp(OK_B),
        "overnmr" => {
            let mut r = sse_resp(OVER);
            r.headers_mut()
                .insert("x-retry-metadata", "NO_MORE_RETRY".parse().unwrap());
            return r;
        }
        _ => {}
    }
    match mode.as_str() {
        "retry" if n < 1 => Response::builder().status(503).body(Body::from("busy")).unwrap(),
        "deact" | "pay402" | "nf404" | "nfmodel" if auth.contains("tok-a") => {
            let (st, body) = match mode.as_str() {
                "deact" => (402, r#"{"detail":{"code":"deactivated_workspace"}}"#),
                "pay402" => (402, r#"{"detail":"Payment Required"}"#),
                "nf404" => (404, r#"{"detail":"Not Found"}"#),
                _ => (404, r#"{"detail":"The model 'gpt-x' does not exist or is not supported for this account."}"#),
            };
            Response::builder()
                .status(st)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap()
        }
        "deactall" => Response::builder()
            .status(402)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"detail":{"code":"deactivated_workspace"}}"#))
            .unwrap(),
        "down" => Response::builder()
            .status(503)
            .header("content-type", "application/json")
            .header("x-request-id", "req_down")
            .body(Body::from(r#"{"error":{"type":"server_error","code":"server_is_overloaded","message":"up busy"}}"#))
            .unwrap(),
        "limit" if auth.contains("tok-a") => Response::builder()
            .status(429)
            .body(Body::from(r#"{"error":{"type":"usage_limit_reached","resets_in_seconds":30}}"#))
            .unwrap(),
        "midlimit" if auth.contains("tok-a") => sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
            "data: {\"type\":\"error\",\"status\":429,\"error\":{\"type\":\"usage_limit_reached\",\"resets_in_seconds\":45}}\n\n"
        )),
        "midlimit" | "latelimit" if !auth.contains("tok-a") || mode == "latelimit" => sse_resp(if mode == "latelimit" {
            concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"usage_limit_reached\"}}}\n\n"
            )
        } else {
            concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-b\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-b\"}}\n\n"
            )
        }),
        "errbody" => Response::builder()
            .status(400)
            .header("content-type", "application/json")
            .header("cf-ray", "ray-123")
            .header("x-request-id", "req_abc")
            .header("set-cookie", "session=supersecret")
            .header("x-codex-primary-used-percent", "40")
            .body(Body::from(
                r#"{"error":{"message":"bad input from alice@example.com <img src=x onerror=alert(1)>","auth":"Bearer sk-live-SECRETSECRETSECRET"},"refresh_token":"rt_SECRETSECRETSECRET"}"#,
            ))
            .unwrap(),
        "unauth" if auth.ends_with("-1") => Response::builder().status(401).body(Body::empty()).unwrap(),
        "instrlimit" => sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-i\"}}\n\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"},\"instructions\":\"see usage_limit_reached\"}}\n\n"
        )),
        "textlimit" => sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-t\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"grep usage_limit_reached in proxy.rs\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-t\"}}\n\n"
        )),
        // Real upstream shape: preamble and an empty reasoning item first, output later, and the
        // body keeps going briefly after `response.completed` (decoder flush / EOF).
        "slowtail" => {
            let s = futures::stream::unfold(0, |i| async move {
                let b: &'static [u8] = match i {
                    0 => b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-s\"}}\n\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"reasoning\"}}\n\n",
                    1 => {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-s\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n"
                    }
                    2 => {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        b"\n"
                    }
                    _ => return None,
                };
                Some((Ok::<_, std::io::Error>(Bytes::from_static(b)), i + 1))
            });
            Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap()
        }
        "late_request_error" => sse_resp(concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"context_length_exceeded\",\"message\":\"too long\"}}}\n\n"
        )),
        "late_rate_limit" => sse_resp(concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n",
            "event: error\ndata: {\"type\":\"error\",\"status\":429,\"error\":{\"type\":\"rate_limit_error\",\"code\":\"rate_limit_exceeded\",\"message\":\"slow down\"}}\n\n"
        )),
        "late_failed_rate_limit" => sse_resp(concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"Please try again in 11s.\"}}}\n\n"
        )),
        "late_429_nocode" => sse_resp(concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n",
            "event: error\ndata: {\"type\":\"error\",\"status\":429,\"error\":{\"message\":\"slow down\"}}\n\n"
        )),
        mode @ ("delta_then_reset" | "reset_first") => {
            let first = mode == "delta_then_reset";
            let s = futures::stream::unfold(0, move |i| async move {
                match i {
                    0 if first => Some((Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n")), 1)),
                    0 | 1 => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Some((Err(std::io::Error::other("connection reset")), 2))
                    }
                    _ => None,
                }
            });
            Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap()
        }
        "completed_then_reset" => {
            let s = futures::stream::unfold(0, |i| async move {
                match i {
                    0 => Some((Ok::<_, std::io::Error>(Bytes::from_static(COMPLETED_HI)), 1)),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Some((Err(std::io::Error::other("reset after completed")), 2))
                    }
                    _ => None,
                }
            });
            Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap()
        }
        "completed_then_idle" => {
            let s = futures::stream::once(async { Ok::<_, std::io::Error>(Bytes::from_static(COMPLETED_HI)) })
                .chain(futures::stream::pending());
            Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap()
        }
        "idle_after_output" => {
            let s = futures::stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(
                    b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
                ))
            }).chain(futures::stream::pending());
            Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap()
        }
        "booterr" => {
            let s = futures::stream::unfold(0, |i| async move {
                match i {
                    0 => Some((Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.created\"}\n\n")), 1)),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        Some((Err(std::io::Error::other("boom")), 2))
                    }
                    _ => None,
                }
            });
            Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap()
        }
        "idle" => {
            let s = futures::stream::unfold(0, |i| async move {
                if i == 0 {
                    Some((Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.created\"}\n\n")), 1))
                } else {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    None
                }
            });
            Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap()
        }
        "usage" => {
            let mut r = sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-u\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            "event: response.completed\r\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-u\",\"usage\":{\"input_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":100,\"cache_write_tokens\":10},\"output_tokens\":30,\"output_tokens_details\":{\"reasoning_tokens\":12}}}}\r\n\r\n"
            ));
            let reset = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 3600).to_string();
            let h = r.headers_mut();
            h.insert("x-codex-plan-type", "plus".parse().unwrap());
            h.insert("x-codex-primary-window-minutes", "300".parse().unwrap());
            h.insert("x-codex-primary-reset-at", reset.parse().unwrap());
            r
        }
        "meta" => sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-m\",\"model\":\"gpt-5.5\",\"service_tier\":\"auto\",\"reasoning\":{\"effort\":\"low\"}}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-m\",\"model\":\"gpt-5.5-2026-09-30\",\"service_tier\":\"default\",\"reasoning\":{\"effort\":\"medium\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
        )),
        "meta_created_only" => sse_resp(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-c\",\"model\":\"gpt-5.5\",\"service_tier\":\"auto\",\"reasoning\":{\"effort\":\"high\"}}}\n\n",
        ),
        "incomplete" => sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-i\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
            "data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"r-i\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":40,\"output_tokens\":16,\"output_tokens_details\":{\"reasoning_tokens\":4}}}}\n\n"
        )),
        "ts_issue" => {
            let mut r = sse_resp("data: {\"type\":\"response.completed\"}\n\n");
            let ts = if auth.contains("tok-a") { "ts-owned-a" } else { "ts-owned-b" };
            r.headers_mut().insert("x-codex-turn-state", ts.parse().unwrap());
            r
        }
        "compact" => sse_resp(concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-c\"}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"compaction\"}}\n\n",
            "data: {\"type\":\"response.compaction.compacting\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"gAAA\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-c\"}}\n\n"
        )),
        "image" => sse_resp(concat!(
            "data: {\"type\":\"response.image_generation_call.partial_image\",\"partial_image_b64\":\"UA==\",\"partial_image_index\":0}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"image_generation_call\",\"result\":\"SU1H\",\"output_format\":\"png\",\"size\":\"1024x1024\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"created_at\":7,\"output\":[],\"tool_usage\":{\"image_gen\":{\"images\":1}}}}\n\n"
        )),
        "image_incomplete" => sse_resp(concat!(
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"image_generation_call\",\"result\":\"SU1H\",\"output_format\":\"png\"}}\n\n",
            "data: {\"type\":\"response.incomplete\",\"response\":{\"created_at\":7,\"output\":[],\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n"
        )),
        "image_content_filter" => sse_resp(
            "data: {\"type\":\"response.incomplete\",\"response\":{\"created_at\":7,\"output\":[],\"incomplete_details\":{\"reason\":\"content_filter\"}}}\n\n",
        ),
        _ => sse_resp("event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"),
    }
}

async fn up_models(State(m): State<Mock>, h: HeaderMap) -> Response {
    m.record(&h, &Bytes::new());
    if h.get("x-test-mode")
        .is_some_and(|v| v == "models_too_large")
    {
        return Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(vec![b'x'; 8 * 1024 * 1024 + 1]))
            .unwrap();
    }
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(MODELS_BODY))
        .unwrap()
}

/// Upstream catalog shape (service_tiers is an array; `[]` when a model has none).
const MODELS_BODY: &str = r#"{"models":[{"slug":"gpt-5.5","service_tiers":[{"id":"priority","name":"Fast"}]},{"slug":"gpt-6-luna","service_tiers":[]}]}"#;

async fn up_images(State(m): State<Mock>, h: HeaderMap, b: Bytes) -> Response {
    m.record(&h, &b);
    match h
        .get("x-test-mode")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
    {
        mode @ ("empty_200" | "empty_204") => return empty_response(mode),
        "image_header_stall" => {
            m.ready.notify_one();
            m.release.notified().await;
        }
        "image_body_late_error" => {
            let stream = futures::stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(b"{\"data\":["))
            })
            .chain(futures::stream::once(async move {
                m.release.notified().await;
                Err(std::io::Error::other("delayed image body failure"))
            }));
            return Response::builder()
                .header("content-type", "application/json")
                .body(Body::from_stream(stream))
                .unwrap();
        }
        "image_direct_sse" => return sse_resp(IMAGE_DIRECT_SSE),
        mode @ ("image_error_event"
        | "image_error_type"
        | "image_error_after_large"
        | "image_error_words") => {
            let body = image_event_fixture(mode);
            let chunks: Vec<_> = body
                .as_bytes()
                .chunks(8191)
                .map(Bytes::copy_from_slice)
                .collect();
            let stream = futures::stream::iter(chunks.into_iter().map(Ok::<_, std::io::Error>))
                .chain(futures::stream::once(async move {
                    m.release.notified().await;
                    Ok(Bytes::new())
                }));
            return Response::builder()
                .header("content-type", "text/event-stream")
                .header("x-request-id", "images-fixture-request")
                .body(Body::from_stream(stream))
                .unwrap();
        }
        _ => {}
    }
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(r#"{"created":1,"data":[{"b64_json":"X"}]}"#))
        .unwrap()
}

const IMAGE_DIRECT_SSE: &str = "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"b64_json\":\"X\"}\n\n";

fn image_event_fixture(mode: &str) -> String {
    match mode {
        "image_error_event" => "event: error\r\ndata: {\"error\":{\"message\":\"fixture image failure\"}}\r\n\r\n".into(),
        "image_error_type" => "data: {\"type\":\"error\",\"error\":{\"message\":\"fixture image failure\"}}\n\n".into(),
        "image_error_after_large" => format!(
            "event: image_generation.partial_image\ndata: {{\"b64_json\":\"{}\"}}\n\n{}",
            "A".repeat(128 * 1024), image_event_fixture("image_error_event")
        ),
        "image_error_words" => "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"note\":\"error\",\"metadata\":{\"type\":\"error\"},\"b64_json\":\"X\"}\n\n".into(),
        _ => unreachable!(),
    }
}

fn empty_response(mode: &str) -> Response {
    Response::builder()
        .status(if mode == "empty_204" { 204 } else { 200 })
        .header("content-type", "application/json")
        .header("content-length", "0")
        .body(Body::empty())
        .unwrap()
}

async fn up_ws(State(m): State<Mock>, h: HeaderMap, ws: WebSocketUpgrade) -> Response {
    let attempt = m.record(&h, &Bytes::new());
    if let Some(mode @ ("ws_delay_fail" | "ws_retry_delay_fail")) =
        h.get("x-test-mode").and_then(|v| v.to_str().ok())
    {
        m.ready.notify_one();
        m.release.notified().await;
        return Response::builder()
            .status(if mode == "ws_retry_delay_fail" && attempt == 0 {
                503
            } else {
                400
            })
            .header("content-type", "application/json")
            .body(Body::from(r#"{"error":{"code":"handshake_fixture"}}"#))
            .unwrap();
    }
    let auth = h
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let mode = h
        .get("x-test-mode")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if mode == "ws_budget_replay" && !auth.contains("tok-a") {
        m.ready.notify_one();
        m.release.notified().await;
    }
    if mode == "ws_budget_turns" {
        tokio::time::sleep(Duration::from_millis(650)).await;
    }
    if mode == "repro_cf1010_text" {
        return Response::builder()
            .status(403)
            .header("content-type", "text/plain; charset=UTF-8")
            .header("cf-ray", "9a1b2c3d4e5f6789-SJC")
            .body(Body::from("error code: 1010"))
            .unwrap();
    }
    if h.get("x-test-mode").is_some_and(|v| v == "deact") && auth.contains("tok-a") {
        return Response::builder()
            .status(402)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"detail":{"code":"deactivated_workspace"}}"#))
            .unwrap();
    }
    let drop_replay = mode == "ws_replay_drop" && !auth.contains("tok-a");
    let mut r = ws.on_upgrade(move |mut s| async move {
        while let Some(Ok(msg)) = s.recv().await {
            if let Message::Text(t) = msg {
                if drop_replay {
                    return;
                }
                if t.as_str().contains("\"diagnostic_close\"") || t.as_str().contains("\"diagnostic_complete\"") {
                    let _ = s.send(Message::Text(r#"{"type":"private-do-not-log","value":"private-output"}"#.into())).await;
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    let _ = s.send(Message::Ping(Bytes::from_static(b"private-ping"))).await;
                    if t.as_str().contains("\"diagnostic_complete\"") {
                        let _ = s.send(Message::Text(r#"{"type":"response.completed"}"#.into())).await;
                        continue;
                    }
                    let _ = s.send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1000,
                        reason: "private-close-reason".into(),
                    }))).await;
                    return;
                }
                if (t.as_str().contains("\"budget_quota\"") || t.as_str().contains("\"budget_overload\"")) && !auth.contains("tok-c") {
                    let event = if t.as_str().contains("\"budget_overload\"") {
                        r#"{"type":"error","error":{"code":"server_is_overloaded"}}"#
                    } else {
                        r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":45}}"#
                    };
                    let _ = s.send(Message::Text(event.into())).await;
                    continue;
                }
                if t.as_str().contains("\"late_ratelimit_ws\"") || t.as_str().contains("\"lifecycle_ws\"") {
                    let _ = s.send(Message::Text(r#"{"type":"response.output_text.delta","delta":"ok"}"#.into())).await;
                    let event = if t.as_str().contains("\"lifecycle_ws\"") {
                        r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"websocket_connection_limit_reached","message":"limit"}}"#
                    } else {
                        r#"{"type":"error","status":429,"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"slow down"}}"#
                    };
                    let _ = s.send(Message::Text(event.into())).await;
                    continue;
                }
                if t.as_str().contains("\"budget_long\"") || t.as_str().contains("\"budget_next\"") {
                    if auth.contains("tok-a") || (auth.contains("tok-b") && t.as_str().contains("\"budget_next\"")) {
                        let _ = s.send(Message::Text(r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":45}}"#.into())).await;
                    } else {
                        for _ in 0..3 {
                            tokio::time::sleep(Duration::from_millis(450)).await;
                            let _ = s.send(Message::Text(r#"{"type":"response.output_text.delta","delta":"ok"}"#.into())).await;
                        }
                        let _ = s.send(Message::Text(r#"{"type":"response.completed","response":{"id":"budget-long"}}"#.into())).await;
                    }
                    continue;
                }
                if t.as_str().contains("\"emptyadded_over\"") && auth.contains("tok-a") {
                    for f in [
                        r#"{"type":"response.created","response":{"id":"r-a"}}"#,
                        r#"{"type":"response.in_progress","response":{"id":"r-a"}}"#,
                        r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_a","type":"message","status":"in_progress","role":"assistant","content":[]}}"#,
                        r#"{"type":"response.content_part.added","item_id":"msg_a","output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[],"text":""}}"#,
                        r#"{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"overloaded"}}"#,
                    ] {
                        let _ = s.send(Message::Text(f.into())).await;
                    }
                    continue;
                }
                if t.as_str().contains("\"metaover\"") || t.as_str().contains("\"emptyadded_over\"") {
                    if auth.contains("tok-a") {
                        let _ = s.send(Message::Text(r#"{"type":"response.created","response":{"id":"r-a"}}"#.into())).await;
                        let _ = s.send(Message::Text(r#"{"type":"response.queued","response":{"id":"r-a","status":"queued"}}"#.into())).await;
                        let _ = s.send(Message::Text(r#"{"type":"response.metadata","response_id":"r-a","metadata":{"openai_chatgpt_moderation_metadata":{"prompt":{}}}}"#.into())).await;
                        let _ = s.send(Message::Text(r#"{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"overloaded"}}"#.into())).await;
                    } else {
                        for f in [
                            r#"{"type":"response.created","response":{"id":"r-b"}}"#,
                            r#"{"type":"response.output_text.delta","delta":"ok"}"#,
                            r#"{"type":"response.completed","response":{"id":"r-b"}}"#,
                        ] {
                            let _ = s.send(Message::Text(f.into())).await;
                        }
                    }
                    continue;
                }
                if t.as_str().contains("\"metaenc\"") {
                    let _ = s.send(Message::Text(r#"{"type":"response.created","response":{"id":"r-e"}}"#.into())).await;
                    let _ = s.send(Message::Text(r#"{"type":"response.metadata","response_id":"r-e","metadata":{"openai_chatgpt_moderation_metadata":{"prompt":{}}}}"#.into())).await;
                    let _ = s.send(Message::Text(concat!(r#"{"type":"error","status":400,"error":"#, enc_obj!(), "}").into())).await;
                    continue;
                }
                if t.as_str().contains("\"enc\"") {
                    // Real upstream shape (WS): metadata frames, then the error with status 400.
                    let _ = s.send(Message::Text(r#"{"type":"codex.rate_limits","rate_limits":{}}"#.into())).await;
                    let _ = s.send(Message::Text(r#"{"type":"codex.response.metadata","headers":{}}"#.into())).await;
                    let _ = s.send(Message::Text(concat!(r#"{"type":"error","status":400,"error":"#, enc_obj!(), "}").into())).await;
                    continue;
                }
                if t.as_str().contains("\"drop\"") {
                    // Created, then the TCP connection goes away without a close frame.
                    let _ = s.send(Message::Text(r#"{"type":"response.created","response":{"id":"r-d"}}"#.into())).await;
                    return;
                }
                if t.as_str().contains("\"held\"") {
                    let _ = s.send(Message::Text(r#"{"type":"response.output_text.delta","delta":"held"}"#.into())).await;
                    continue;
                }
                if t.as_str().contains("\"generate\":false") {
                    let _ = s.send(Message::Text(r#"{"type":"response.created","response":{"id":"r-w"}}"#.into())).await;
                    let _ = s.send(Message::Text(r#"{"type":"response.completed","response":{"id":"r-w","usage":{"input_tokens":8,"output_tokens":0}}}"#.into())).await;
                } else if t.as_str().contains("\"slow\"") {
                    let _ = s.send(Message::Text(r#"{"type":"codex.response.metadata","headers":{}}"#.into())).await;
                    let _ = s.send(Message::Text(r#"{"type":"response.created","response":{"id":"r-x"}}"#.into())).await;
                    let _ = s.send(Message::Text(r#"{"type":"response.output_item.added","item":{"type":"reasoning"}}"#.into())).await;
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let _ = s.send(Message::Text(r#"{"type":"response.reasoning_summary_text.delta","delta":"x"}"#.into())).await;
                    let _ = s.send(Message::Text(r#"{"type":"response.completed","response":{"id":"r-x","usage":{"input_tokens":5,"output_tokens":1}}}"#.into())).await;
                } else if t.as_str().contains("\"meta\"") {
                    for f in [
                        r#"{"type":"response.created","response":{"id":"r-m","model":"gpt-5.5","service_tier":"auto","reasoning":{"effort":"high"}}}"#,
                        r#"{"type":"response.output_text.delta","delta":"ok"}"#,
                        r#"{"type":"response.completed","response":{"id":"r-m","model":"gpt-5.6-luna","service_tier":"flex","reasoning":{"effort":"high"},"usage":{"input_tokens":5,"output_tokens":2}}}"#,
                    ] {
                        let _ = s.send(Message::Text(f.into())).await;
                    }
                } else if t.as_str().contains("limit") && (auth.contains("tok-a") || t.as_str().contains("prevlimit")) {
                    let _ = s.send(Message::Text(r#"{"type":"response.created","response":{"id":"r-a"}}"#.into())).await;
                    let _ = s.send(Message::Text(r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":45}}"#.into())).await;
                } else if t.as_str().contains("limit") {
                    for f in [
                        r#"{"type":"response.created","response":{"id":"r-b"}}"#,
                        r#"{"type":"response.output_text.delta","delta":"ok"}"#,
                        r#"{"type":"response.completed","response":{"id":"r-b"}}"#,
                    ] {
                        let _ = s.send(Message::Text(f.into())).await;
                    }
                } else {
                    let _ = s.send(Message::Text(format!(r#"{{"type":"response.completed","echo":{t}}}"#).into())).await;
                }
            }
        }
    });
    r.headers_mut().insert(
        "x-request-id",
        if drop_replay {
            "ws-replay-upstream-id"
        } else {
            "ws-diagnostic-upstream-id"
        }
        .parse()
        .unwrap(),
    );
    r.headers_mut()
        .insert("x-codex-turn-state", "ts-1".parse().unwrap());
    r.headers_mut()
        .insert("set-cookie", "__cf_bm=secret".parse().unwrap());
    r.headers_mut()
        .insert("set-cookie2", "__oailb=secret".parse().unwrap());
    r
}

async fn serve(app: Router) -> String {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            ln,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap()
    });
    format!("127.0.0.1:{}", addr.port())
}

const COMPLETED_HI: &[u8] = b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-x\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-x\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n";

struct Env {
    proxy: String,
    mock: Mock,
    rec: Arc<Rec>,
    store: Arc<Store>,
    http: reqwest::Client,
    config: Arc<ciallopa_core::settings::SharedConfig>,
    drain: Arc<ciallopa_upstream::drain::Drain>,
    app: Router,
    state: AppState,
}

async fn env(idle: u64) -> Env {
    env_usage(idle, None).await
}

async fn env_usage(idle: u64, usage: Option<Arc<ciallopa_usage::Recorder>>) -> Env {
    env_cfg(idle, usage, "").await
}

async fn env_cfg(idle: u64, usage: Option<Arc<ciallopa_usage::Recorder>>, extra: &str) -> Env {
    env_cfg_accounts(idle, usage, extra, None).await
}

async fn env_cfg_accounts(
    idle: u64,
    usage: Option<Arc<ciallopa_usage::Recorder>>,
    extra: &str,
    accounts: Option<Arc<dyn AccountStore>>,
) -> Env {
    let mock = Mock::default();
    let up = Router::new()
        .route(
            "/backend-api/codex/responses",
            post(up_responses).get(up_ws),
        )
        .route("/backend-api/codex/models", get(up_models))
        .route("/backend-api/codex/images/generations", post(up_images))
        .route("/backend-api/codex/images/edits", post(up_images))
        .route("/backend-api/wham/usage", get(up_models))
        .with_state(mock.clone());
    let up_addr = serve(up).await;
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n[upstream_opts]\nstream_idle_timeout_secs = {idle}\n{extra}"
    ))
    .unwrap();
    let rec = Arc::new(Rec::default());
    let store = Arc::new(Store {
        ids: vec!["a", "b"],
        refreshed: AtomicUsize::new(0),
    });
    let state = AppState {
        accounts: accounts.unwrap_or_else(|| store.clone()),
        router: rec.clone(),
        upstream: {
            let up = ciallopa_upstream::UpstreamClient::new(&cfg).unwrap();
            // mock upstream is plain http on 127.0.0.1, not an https ChatGPT host
            up.pools.allow_any_cookie_host_for_tests();
            Arc::new(up)
        },
        usage,
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let config = state.config.clone();
    let drain = state.upstream.drain.clone();
    let app = ciallopa::build_app(state.clone());
    let proxy = serve(app.clone()).await;
    Env {
        state,
        proxy,
        mock,
        rec,
        store,
        http: reqwest::Client::new(),
        config,
        drain,
        app,
    }
}

impl Env {
    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .post(format!("http://{}{path}", self.proxy))
            .bearer_auth(KEY)
    }
    fn outcomes(&self) -> Vec<(String, Outcome)> {
        self.rec.0.lock().unwrap().clone()
    }
}

async fn held_response(e: &Env) -> Response {
    use tower::ServiceExt;
    let req = http::Request::post("/v1/responses")
        .header("authorization", format!("Bearer {KEY}"))
        .header("x-test-mode", "held_stream")
        .body(Body::from(r#"{"stream":true,"input":[]}"#))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), e.app.clone().oneshot(req))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn pending_connect_keeps_its_egress_classification_across_reload() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tower::ServiceExt;

    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let proxy = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            assert!(line.starts_with("CONNECT 127.0.0.1:1 "), "{line}");
            loop {
                line.clear();
                socket.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
            socket.get_mut().write_all(
                b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ).await.unwrap();
        });
        let mut cfg = Config::parse(&format!(
            "api_keys = [\"{KEY}\"]\nupstream = \"https://127.0.0.1:1/backend-api\"\nproxy_url = \"http://{address}\""
        )).unwrap();
        let upstream = Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap());
        let config = Arc::new(ciallopa_core::settings::SharedConfig::new(cfg.clone()));
        let rec = Arc::new(Rec::default());
        let dir = tempfile::tempdir().unwrap();
        let usage = usage_recorder(dir.path());
        let app = ciallopa::build_app(AppState {
            accounts: Arc::new(Store { ids: vec!["a", "b"], refreshed: AtomicUsize::new(0) }),
            router: rec.clone(),
            upstream: upstream.clone(),
            usage: Some(usage.clone()),
            config: config.clone(),
            egress: Default::default(),
        });
        let request = http::Request::post("/v1/responses")
            .header("authorization", format!("Bearer {KEY}"))
            .body(Body::from(r#"{"stream":true,"input":[]}"#)).unwrap();
        let response = tokio::spawn(app.oneshot(request));
        ready_rx.await.unwrap();
        // The in-flight CONNECT still uses the old proxy; new attempts now go direct.
        cfg.proxy_enabled = false;
        upstream.reconfigure(&cfg).unwrap();
        config.store(cfg);
        release_tx.send(()).unwrap();
        let response = response.await.unwrap().unwrap();
        assert_eq!(response.status(), 502);
        let body = axum::body::to_bytes(response.into_body(), 8192).await.unwrap();
        let _: serde_json::Value = serde_json::from_slice(&body).unwrap();
        proxy.await.unwrap();
        usage.flush_blocking().unwrap();
        let rows = usage.recent(ciallopa_usage::RecentQuery { limit: 10, ..Default::default() }).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].error.as_deref(), Some("proxy_connect"));
        assert_eq!(rec.0.lock().unwrap().len(), 1, "a proxy rejection must not switch accounts");
    }).await.expect("CONNECT/reload fixture did not finish");
}

#[tokio::test]
async fn configured_body_limit_has_no_hidden_twenty_mib_floor() {
    use tower::ServiceExt;
    let e = env(300).await;
    assert_eq!(e.config.load().max_body_mb, 64);
    let req = http::Request::post("/v1/responses")
        .header("authorization", format!("Bearer {KEY}"))
        .body(Body::from(vec![b' '; 64 * 1024 * 1024 + 1]))
        .unwrap();
    let response = e.app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), 413);
    drop(response);
    assert_eq!(e.drain.inflight(), 0);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
}

fn sized_post(len: usize, chunked: bool) -> http::Request<Body> {
    let pad = "x".repeat(len.saturating_sub(40));
    let raw = format!(r#"{{"stream":true,"input":[],"pad":"{pad}"}}"#);
    let n = raw.len();
    let b = http::Request::post("/v1/responses").header("authorization", format!("Bearer {KEY}"));
    if chunked {
        let bytes = Bytes::from(raw);
        let parts: Vec<Result<Bytes, std::io::Error>> =
            vec![Ok(bytes.slice(..n / 2)), Ok(bytes.slice(n / 2..))];
        b.body(Body::from_stream(futures::stream::iter(parts)))
            .unwrap()
    } else {
        b.header("content-length", n).body(Body::from(raw)).unwrap()
    }
}

async fn busy_json(r: Response) -> serde_json::Value {
    assert_eq!(r.status(), 503);
    assert_eq!(r.headers()["retry-after"], "3");
    assert_eq!(r.headers()["retry-after-ms"], "3000");
    serde_json::from_slice(&axum::body::to_bytes(r.into_body(), 4096).await.unwrap()).unwrap()
}

#[tokio::test]
async fn buffered_body_budget_waits_briefly_then_refuses_without_upstream() {
    use tower::ServiceExt;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_buffered_body_mb = 1;
    e.config.store(cfg);
    let mib = 1024 * 1024u64;
    // Another request's body occupies 900 KiB of the 1 MiB budget.
    let mut other = e.drain.body_permit();
    e.drain
        .reserve_body(&mut other, 900 * 1024, mib, Duration::ZERO)
        .await
        .unwrap();

    let t = std::time::Instant::now();
    let r = e
        .app
        .clone()
        .oneshot(sized_post(200 * 1024, false))
        .await
        .unwrap();
    let waited = t.elapsed();
    let json = busy_json(r).await;
    assert_eq!(json["error"]["type"], "gateway_busy");
    assert_eq!(json["error"]["message"], "Gateway is busy; retry shortly.");
    assert!(
        waited >= Duration::from_millis(900),
        "bounded wait first: {waited:?}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
    assert_eq!(e.drain.inflight(), 0);

    // A chunked body is charged as it arrives: 1.2 MiB can never fit in 1 MiB.
    drop(other);
    let r = e
        .app
        .clone()
        .oneshot(sized_post(1200 * 1024, true))
        .await
        .unwrap();
    assert_eq!(busy_json(r).await["error"]["type"], "gateway_busy");
    assert_eq!(
        e.drain.body_bytes(),
        0,
        "a refused body keeps nothing reserved"
    );

    // Room freed during the wait admits the request; its bytes are returned afterwards.
    let mut other = e.drain.body_permit();
    e.drain
        .reserve_body(&mut other, 900 * 1024, mib, Duration::ZERO)
        .await
        .unwrap();
    let app = e.app.clone();
    let pending = tokio::spawn(async move { app.oneshot(sized_post(200 * 1024, false)).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(other);
    let r = pending.await.unwrap().unwrap();
    assert_eq!(r.status(), 200);
    let _ = axum::body::to_bytes(r.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(e.drain.body_bytes(), 0);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);

    // Chunked bodies within budget pass, and 0 = unlimited.
    let r = e
        .app
        .clone()
        .oneshot(sized_post(300 * 1024, true))
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let mut cfg = (*e.config.load()).clone();
    cfg.max_buffered_body_mb = 0;
    e.config.store(cfg);
    let r = e
        .app
        .clone()
        .oneshot(sized_post(1200 * 1024, false))
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

#[tokio::test]
async fn declared_oversized_body_gets_neutral_json_413() {
    use tower::ServiceExt;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_body_mb = 1;
    e.config.store(cfg);
    let r = e
        .app
        .clone()
        .oneshot(sized_post(1024 * 1024 + 100, false))
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    let json: serde_json::Value =
        serde_json::from_slice(&axum::body::to_bytes(r.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(json["error"]["type"], "invalid_request_error");
    assert_eq!(json["error"]["message"], "Request body is too large.");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
    assert_eq!(e.drain.body_bytes(), 0);
}

#[tokio::test]
async fn admission_holds_http_body_and_hot_limit_keeps_existing_occupancy() {
    use tower::ServiceExt;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 1;
    cfg.api_keys = vec![ciallopa_core::config::ApiKeyEntry::Labeled {
        label: "admin".into(),
        key: KEY.into(),
        admin: true,
    }];
    e.config.store(cfg.clone());
    let first = held_response(&e).await;
    assert_eq!(first.status(), 200);
    assert_eq!(e.drain.inflight(), 1, "headers do not release the slot");

    let unread = Body::from_stream(futures::stream::poll_fn(
        |_| -> std::task::Poll<Option<Result<Bytes, std::io::Error>>> {
            panic!("an overloaded request must be rejected before its body is polled")
        },
    ));
    let req = http::Request::post("/v1/responses")
        .header("authorization", format!("Bearer {KEY}"))
        .body(unread)
        .unwrap();
    let busy = e.app.clone().oneshot(req).await.unwrap();
    assert_eq!(busy.status(), 503);
    assert_eq!(busy.headers()["retry-after"], "3");
    assert_eq!(busy.headers()["retry-after-ms"], "3000");
    let json: serde_json::Value =
        serde_json::from_slice(&axum::body::to_bytes(busy.into_body(), 4096).await.unwrap())
            .unwrap();
    assert!(json.to_string().contains("gateway_busy"));
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);

    for path in ["/healthz", "/admin/v1/config", "/admin/v1/memory"] {
        let r = e
            .app
            .clone()
            .oneshot(
                http::Request::get(path)
                    .header("authorization", format!("Bearer {KEY}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{path} remains available at capacity");
        let body = axum::body::to_bytes(r.into_body(), 100_000).await.unwrap();
        if path.ends_with("config") {
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["runtime"]["inflight"], 1);
            assert_eq!(value["runtime"]["rejected"], 1);
        }
        if path.ends_with("memory") {
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(v["buffered_body_limit_bytes"], 256u64 << 20);
            assert_eq!(v["ws_max_connections"], 24);
            assert_eq!(v["ws_max_message_bytes"], 16u64 << 20);
            assert_eq!(v["now"]["ws_connections"], 0);
            assert_eq!(v["jemalloc"], false, "tests use the system allocator");
            assert!(v["history"].is_array());
            if cfg!(target_os = "linux") {
                assert!(v["now"]["rss_bytes"].as_u64().unwrap() > 0);
            }
        }
    }
    cfg.max_inflight_requests = 0;
    e.config.store(cfg.clone());
    let second = held_response(&e).await;
    assert_eq!(second.status(), 200);
    assert_eq!(e.drain.inflight(), 2, "unlimited still counts");
    cfg.max_inflight_requests = 1;
    e.config.store(cfg);
    assert_eq!(held_response(&e).await.status(), 503);
    drop(first);
    assert_eq!(
        e.drain.inflight(),
        1,
        "hot lowering did not terminate either body"
    );
    assert_eq!(held_response(&e).await.status(), 503);
    drop(second);
    assert_eq!(e.drain.inflight(), 0);
    let again = held_response(&e).await;
    assert_eq!(again.status(), 200);
    drop(again);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
}

#[tokio::test]
async fn idle_websocket_is_a_session_without_an_inflight_slot() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 1;
    e.config.store(cfg);
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, response) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(response.status(), 101);
    // Ping/pong proves the detached relay has started; no turn is active.
    ws.send(M::Ping(Bytes::from_static(b"ready")))
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap(),
        Some(Ok(M::Pong(_)))
    ));
    assert_eq!(e.drain.inflight(), 0, "an idle socket holds no slot");
    assert_eq!(
        e.drain.active(),
        1,
        "but shutdown still waits for its session"
    );
    assert_eq!(held_response(&e).await.status(), 200);
    wait_inflight(&e, 0).await;
    ws.send(M::Close(None)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            // Both the WebSocket implementation and relay can answer the readiness
            // ping. A second pong may already be queued ahead of the close reply.
            match ws.next().await.unwrap().unwrap() {
                M::Close(_) => break,
                M::Pong(_) | M::Ping(_) => continue,
                other => panic!("unexpected frame while closing: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    drop(ws);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(3)).await, 0);
    assert_eq!(e.drain.inflight(), 0);
    assert_eq!(held_response(&e).await.status(), 200);

    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(r#"{"type":"response.create","input":"held"}"#))
        .await
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(output
        .to_text()
        .unwrap()
        .contains("response.output_text.delta"));
    assert_eq!(e.drain.inflight(), 1, "the running turn holds the slot");
    let drain = e.drain.clone();
    let stopped = tokio::spawn(async move {
        drain
            .shutdown(Duration::from_millis(30), Duration::from_secs(2))
            .await
    });
    let close = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(close, M::Close(Some(frame)) if u16::from(frame.code) == 1001));
    drop(ws);
    assert!(stopped.await.unwrap() > 0);
    assert_eq!(e.drain.inflight(), 0);
}

#[tokio::test]
async fn ws_connection_cap_refuses_handshakes_until_one_closes() {
    use tokio_tungstenite::tungstenite::Message as M;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.ws_max_connections = 1;
    e.config.store(cfg.clone());
    let (mut first, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(r.status(), 101);
    assert_eq!(e.drain.ws_connections(), 1);
    // Over the cap: the admission 503 wording and headers, no upstream dial.
    match tokio_tungstenite::connect_async(ws_req(&e)).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => {
            assert_eq!(r.status(), 503);
            assert_eq!(r.headers()["retry-after"], "3");
            assert_eq!(r.headers()["retry-after-ms"], "3000");
            let body: serde_json::Value =
                serde_json::from_slice(r.body().as_deref().unwrap()).unwrap();
            assert_eq!(body["error"]["type"], "gateway_busy");
            assert_eq!(body["error"]["message"], "Gateway is busy; retry shortly.");
        }
        other => panic!("expected 503, got {other:?}"),
    }
    assert_eq!(e.drain.rejected(), 1);
    // HTTP is not limited by the WebSocket cap.
    assert_eq!(held_response(&e).await.status(), 200);
    // 0 = unlimited (hot, next handshake).
    cfg.ws_max_connections = 0;
    e.config.store(cfg.clone());
    let (second, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(e.drain.ws_connections(), 2);
    drop(second);
    cfg.ws_max_connections = 1;
    e.config.store(cfg);
    first.send(M::Close(None)).await.unwrap();
    while let Some(Ok(m)) = first.next().await {
        if matches!(m, M::Close(_)) {
            break;
        }
    }
    drop(first);
    tokio::time::timeout(Duration::from_secs(3), async {
        while e.drain.ws_connections() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (_third, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(r.status(), 101);
}

#[tokio::test]
async fn ws_message_over_the_limit_closes_1009_even_when_compressed() {
    use tokio_tungstenite::tungstenite::{protocol::frame::coding::CloseCode, Message as M};
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.ws_max_message_mb = 1;
    e.config.store(cfg);
    let msg = |len: usize| {
        let head = r#"{"type":"response.create","model":"gpt-5.5","pad":""#;
        M::text(format!("{head}{}\"}}", "x".repeat(len - head.len() - 2)))
    };
    async fn closed_with<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Option<CloseCode>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        loop {
            match tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .unwrap()
            {
                Some(Ok(M::Close(f))) => return f.map(|f| f.code),
                Some(Ok(_)) => continue,
                _ => return None,
            }
        }
    }
    // Plain: just under the limit passes, just over closes with 1009.
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    ws.send(msg(1024 * 1024 - 64)).await.unwrap();
    let got = ws.next().await.unwrap().unwrap();
    assert!(
        got.to_text().unwrap().contains("response.completed"),
        "{got:?}"
    );
    ws.send(msg(1024 * 1024 + 1)).await.unwrap();
    assert_eq!(closed_with(&mut ws).await, Some(CloseCode::Size));
    // Deflated: a 4 MiB message that compresses to a few KiB is still refused by size.
    let (mut ws, _) = tokio_tungstenite::connect_async_with_config(
        ws_req(&e),
        Some(deflate_client(|d| d)),
        false,
    )
    .await
    .unwrap();
    ws.send(msg(4 * 1024 * 1024)).await.unwrap();
    assert_eq!(closed_with(&mut ws).await, Some(CloseCode::Size));
    tokio::time::timeout(Duration::from_secs(3), async {
        while e.drain.ws_connections() > 0 || e.drain.body_bytes() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn admission_force_cancels_headers_and_unpolled_sse_with_shutdown_records() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let held = held_response(&e).await;
    assert_eq!(held.status(), 200);
    let headers = tokio::spawn(
        e.post("/v1/responses")
            .header("x-test-mode", "header_stall_a")
            .body(r#"{"stream":true,"input":[]}"#)
            .send(),
    );
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    assert_eq!(e.drain.inflight(), 2);
    let drain = e.drain.clone();
    let stopped = tokio::spawn(async move {
        drain
            .shutdown(Duration::from_millis(30), Duration::from_millis(100))
            .await
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), headers)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        503
    );
    assert!(stopped.await.unwrap() > 0);
    // No poll of held's body: force must cancel its detached producer on its own.
    let rows = tokio::time::timeout(Duration::from_secs(3), wait_rows(&e, 2))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(row["error"], "shutdown", "{row}");
        assert_eq!(row["err_source"], "local", "{row}");
        assert_eq!(row["account"], "a", "{row}");
    }
    drop(held);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(2)).await, 0);
    e.mock.release.notify_one();
    rec.shutdown();
}

#[tokio::test]
async fn sse_passthrough_headers_and_zstd() {
    let e = env(300).await;
    let raw = br#"{"model":"gpt-5.5","prompt_cache_key":"pk","input":[]}"#;
    let z = zstd::encode_all(&raw[..], 3).unwrap();
    let r = e
        .post("/backend-api/codex/responses")
        .header("originator", "codex_cli_rs")
        .header("chatgpt-account-id", "client-acct")
        .header("x-forwarded-for", "9.9.9.9")
        .header("True-Client-IP", "203.0.113.7")
        .header("CF-Connecting-IP", "203.0.113.7")
        .header("X-Real-IP", "203.0.113.7")
        .header("content-encoding", "zstd")
        .header("session_id", "s1")
        .body(z.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "text/event-stream");
    assert!(r.text().await.unwrap().contains("response.completed"));
    let seen = e.mock.seen.lock().unwrap();
    let (h, b) = &seen[0];
    assert_eq!(
        b.as_ref(),
        z.as_slice(),
        "zstd body forwarded byte-identical"
    );
    let names: Vec<&str> = h.iter().map(|(k, _)| k.as_str()).collect();
    let get = |k: &str| h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    assert_eq!(get("authorization"), Some("Bearer tok-a-1"));
    assert_eq!(get("chatgpt-account-id"), Some("acct-a"));
    assert_eq!(get("content-encoding"), Some("zstd"));
    for k in [
        "x-forwarded-for",
        "true-client-ip",
        "cf-connecting-ip",
        "x-real-ip",
    ] {
        assert!(get(k).is_none(), "{k} forwarded upstream");
    }
    let pos = |k: &str| names.iter().position(|n| *n == k).unwrap();
    assert!(
        pos("originator") < pos("chatgpt-account-id")
            && pos("chatgpt-account-id") < pos("content-encoding")
    );
    assert!(matches!(
        &e.outcomes()[0].1,
        Outcome::Success { quota: Some(_) }
    ));
}

#[tokio::test]
async fn retries_5xx_then_succeeds() {
    let e = env(300).await;
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "retry")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    // One 503, then a failover to another account succeeds.
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    let o = e.outcomes();
    assert!(matches!(o[0].1, Outcome::ServerError { status: 503, .. }));
    assert!(matches!(o[1].1, Outcome::Success { .. }));
    assert_ne!(o[0].0, o[1].0);
}

#[tokio::test]
async fn rate_limit_fails_over() {
    let e = env(300).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "limit")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let o = e.outcomes();
    assert_eq!(o[0].0, "a");
    assert!(
        matches!(o[0].1, Outcome::RateLimited { usage_limit: true, retry_after: Some(d), .. } if d == Duration::from_secs(30))
    );
    assert_eq!(o[1].0, "b");
}

/// Each account has its own connection pool: sequential requests reuse the account's own
/// keep-alive connection, but two accounts never share one.
#[tokio::test]
async fn accounts_use_separate_connections() {
    let e = env(300).await;
    for _ in 0..3 {
        let r = e
            .post("/backend-api/codex/responses")
            .header("x-test-mode", "limit")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        r.bytes().await.unwrap();
    }
    let peers = e.mock.peers.lock().unwrap().clone();
    let ports = |tok: &str| {
        let mut v: Vec<u16> = peers
            .iter()
            .filter(|(a, _)| a.contains(tok))
            .map(|(_, p)| *p)
            .collect();
        v.dedup();
        v
    };
    let (pa, pb) = (ports("tok-a"), ports("tok-b"));
    assert_eq!(peers.len(), 6, "{peers:?}");
    assert_eq!(
        pa.len(),
        1,
        "account a reuses its own connection: {peers:?}"
    );
    assert_eq!(
        pb.len(),
        1,
        "account b reuses its own connection: {peers:?}"
    );
    assert_ne!(pa[0], pb[0], "accounts must not share a connection");
}

fn cookies_seen(e: &Env) -> Vec<(String, Option<String>)> {
    e.mock
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|(h, _)| {
            let g = |k: &str| h.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
            (g("authorization").unwrap_or_default(), g("cookie"))
        })
        .collect()
}

async fn cookie_round(e: &Env, set: bool) {
    let mut rb = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "limit")
        .header("cookie", "client=1; __cf_bm=client")
        .body("{}");
    if set {
        rb = rb.header("x-test-cookies", "1");
    }
    let r = rb.send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers().get("set-cookie").is_none());
    r.bytes().await.unwrap();
}

#[tokio::test]
async fn cloudflare_cookies_disabled_sends_none() {
    let e = env(300).await;
    cookie_round(&e, true).await;
    cookie_round(&e, true).await;
    let seen = cookies_seen(&e);
    assert_eq!(seen.len(), 4);
    assert!(seen.iter().all(|(_, c)| c.is_none()), "{seen:?}");
}

#[tokio::test]
async fn cloudflare_cookies_per_account_allowlist_and_expiry() {
    let e = env_cfg(300, None, "cloudflare_cookies = true").await;
    cookie_round(&e, true).await; // a: 429 + Set-Cookie A, b: Set-Cookie B
    cookie_round(&e, true).await;
    let seen = cookies_seen(&e);
    assert_eq!(seen.len(), 4, "{seen:?}");
    // First round: nothing stored yet, client Cookie never forwarded.
    assert_eq!(seen[0].1, None);
    assert_eq!(seen[1].1, None);
    // Second round: each account replays only its own allowlisted cookies.
    let a = seen[2].1.clone().unwrap();
    assert!(seen[2].0.contains("tok-a"));
    let mut parts: Vec<&str> = a.split("; ").collect();
    parts.sort();
    assert_eq!(parts, vec!["__cf_bm=A", "__oailb=A-lb"]);
    assert!(seen[3].0.contains("tok-b"));
    assert_eq!(seen[3].1.as_deref(), Some("__cf_bm=B"));
    // Expiry: __oailb has Max-Age=1.
    tokio::time::sleep(Duration::from_millis(2100)).await;
    cookie_round(&e, false).await;
    let seen = cookies_seen(&e);
    assert_eq!(seen[4].1.as_deref(), Some("__cf_bm=A"));
    assert_eq!(seen[5].1.as_deref(), Some("__cf_bm=B"));
}

#[tokio::test]
async fn cloudflare_cookies_ws_handshake_shared_with_http() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    for enabled in [false, true] {
        let e = env_cfg(
            300,
            None,
            if enabled {
                "cloudflare_cookies = true"
            } else {
                ""
            },
        )
        .await;
        for _ in 0..2 {
            let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
                .into_client_request()
                .unwrap();
            req.headers_mut()
                .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
            req.headers_mut()
                .insert("cookie", "client=1".parse().unwrap());
            for k in [
                "True-Client-IP",
                "CF-Connecting-IP",
                "X-Forwarded-For",
                "X-Real-IP",
            ] {
                req.headers_mut().insert(k, "203.0.113.7".parse().unwrap());
            }
            let (ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
            assert!(resp.headers().get("set-cookie").is_none());
            drop(ws);
        }
        // WS handshake Set-Cookie (__cf_bm=secret) is replayed on HTTP for the same account.
        let r = e
            .post("/backend-api/codex/responses")
            .body("{}")
            .send()
            .await
            .unwrap();
        r.bytes().await.unwrap();
        let seen = cookies_seen(&e);
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].1, None, "client Cookie never forwarded");
        let want = enabled.then(|| "__cf_bm=secret".to_string());
        assert_eq!(seen[1].1, want, "ws handshake (enabled={enabled})");
        assert_eq!(seen[2].1, want, "http after ws (enabled={enabled})");
        let all = e.mock.seen.lock().unwrap();
        for (h, _) in all.iter() {
            for (k, _) in h.iter() {
                assert!(
                    !matches!(
                        k.as_str(),
                        "true-client-ip" | "cf-connecting-ip" | "x-forwarded-for" | "x-real-ip"
                    ),
                    "{k} forwarded upstream on the ws handshake"
                );
            }
        }
    }
}

#[tokio::test]
async fn unauthorized_forces_refresh_once() {
    let e = env(300).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "unauth")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(e.store.refreshed.load(Ordering::SeqCst), 1);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn idle_timeout_aborts_stream() {
    let e = env(1).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "idle")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let t0 = std::time::Instant::now();
    // A cut-off stream ends with a synthetic `response.failed` so the client retries.
    let b = r.text().await.unwrap();
    assert!(b.contains("event: response.failed"), "{b}");
    assert!(!b.contains("ciallopa"));
    assert!(t0.elapsed() < Duration::from_secs(4));
    assert!(e
        .outcomes()
        .iter()
        .any(|(_, o)| *o == Outcome::Transient { status: None }));
}

#[tokio::test]
async fn stream_diagnostics_distinguish_heartbeat_activity_from_events_without_payloads() {
    let warnings = StreamWarnings::default();
    let _subscriber = warnings.capture();
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(1, Some(usage_recorder(d.path()))).await;
    for (mode, error, last, count) in [
        ("diag_boot_eof", "eof", "response.created", 1),
        ("diag_boot_idle", "idle_timeout", "response.created", 1),
        ("diag_stream_eof", "eof", "response.output_text.delta", 2),
        ("diag_unknown", "eof", "unknown", 1),
    ] {
        warnings.0.lock().unwrap().clear();
        let r = e
            .post("/backend-api/codex/responses")
            .header("x-test-mode", mode)
            .header("thread-id", "diag-thread")
            .header("x-codex-turn-metadata", r#"{"turn_id":"diag-turn"}"#)
            .body(r#"{"input":"private-prompt"}"#)
            .send()
            .await
            .unwrap();
        assert!(r.text().await.unwrap().contains("response.failed"));
        let logs = warnings.0.lock().unwrap().clone();
        assert_eq!(logs.len(), 1, "{mode}: {logs:?}");
        let log = &logs[0];
        assert_eq!(log["transport"], "sse");
        assert_eq!(log["error_class"], error);
        assert_eq!(log["last_event"], last);
        assert_eq!(log["event_count"], count);
        assert_eq!(log["terminal_received"], false);
        assert_eq!(log["observation_limited"], false);
        assert_eq!(log["activity_received"], true);
        assert_eq!(log["account"], "a");
        assert_eq!(log["upstream_request_id"], "diag-upstream-id");
        assert_eq!(log["thread_id"], "diag-thread");
        assert_eq!(log["turn_id"], "diag-turn");
        assert_eq!(log["record_id"].as_str().unwrap().len(), 16);
        assert!(
            log["event_idle_ms"].as_u64().unwrap()
                >= log["activity_idle_ms"].as_u64().unwrap() + 80,
            "{log}"
        );
        assert!(!log.to_string().contains("private-"), "{log}");
    }
    warnings.0.lock().unwrap().clear();
    let text = sse_text(&e, "diag_success").await;
    assert!(text.contains("response.completed"));
    assert!(
        warnings.0.lock().unwrap().is_empty(),
        "successful stream is quiet"
    );

    // The operational warning remains available even if request persistence is disabled.
    let e = env(1).await;
    assert!(sse_text(&e, "diag_boot_eof")
        .await
        .contains("response.failed"));
    let logs = warnings.0.lock().unwrap().clone();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0]["error_class"], "eof");
    assert!(logs[0].get("record_id").is_none());
}

#[tokio::test]
async fn models_cached_and_wham_passthrough() {
    let e = env(300).await;
    for _ in 0..2 {
        let r = e
            .http
            .get(format!(
                "http://{}/backend-api/codex/models?client_version=1",
                e.proxy
            ))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(r.text().await.unwrap(), MODELS_BODY);
    }
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    let r = e
        .http
        .get(format!("http://{}/api/codex/usage", e.proxy))
        .header("x-api-key", KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn bounded_header_wait_can_fail_over_without_cooling_the_account() {
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "response_header_timeout_secs = 1\npreparation_timeout_secs = 3\n",
    )
    .await;
    let request = e
        .post("/v1/responses")
        .header("x-test-mode", "header_stall_a")
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send();
    let request = tokio::spawn(request);
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(4), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.text().await.unwrap().contains("r-b"));
    e.mock.release.notify_waiters();
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    assert!(e
        .outcomes()
        .iter()
        .any(|(_, outcome)| matches!(outcome, Outcome::Transient { status: None })));
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{rows:?}");
    let detail: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(detail["attempts"][0]["code"], "header_timeout", "{detail}");
}

#[tokio::test]
async fn bounded_preparation_budget_is_shared_across_sse_replay() {
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "response_header_timeout_secs = 3\npreparation_timeout_secs = 2\n",
    )
    .await;
    let request = tokio::spawn(
        e.post("/v1/responses")
            .header("x-test-mode", "preparation_shared")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send(),
    );
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    // About half a second remains. Resetting the budget on replay would take two seconds.
    let response = tokio::time::timeout(Duration::from_secs(1), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 504);
    response.bytes().await.unwrap();
    e.mock.release.notify_waiters();
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout", "{rows:?}");
    assert_eq!(rows[0]["account"], "b", "{rows:?}");
}

#[tokio::test]
async fn bounded_preparation_does_not_count_valid_sse_bootstrap_time() {
    let e = env_cfg(
        30,
        None,
        "response_header_timeout_secs = 1\npreparation_timeout_secs = 1\n",
    )
    .await;
    let response = tokio::time::timeout(
        Duration::from_secs(4),
        e.post("/v1/responses")
            .header("x-test-mode", "bootstrap_long_quota")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.text().await.unwrap().contains("r-b"));
    assert_eq!(
        e.mock.hits.load(Ordering::SeqCst),
        2,
        "the long pre-output stream can still replay"
    );
}

#[tokio::test]
async fn bounded_header_wait_preserves_nonstream_json_and_http_timing() {
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "response_header_timeout_secs = 2\npreparation_timeout_secs = 2\n",
    )
    .await;
    let request = tokio::spawn(
        e.post("/v1/responses")
            .header("x-test-mode", "json_headers")
            .body(r#"{"model":"gpt-5.5","stream":false,"input":[]}"#)
            .send(),
    );
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    e.mock.release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.text().await.unwrap(),
        r#"{"id":"json-ok","status":"completed"}"#
    );
    let rows = wait_rows(&e, 1).await;
    assert_eq!(
        rows[0]["transport"], "http",
        "nonstream headers are not SSE output timing: {rows:?}"
    );
    assert!(
        rows[0]["ttft_ms"].is_u64(),
        "legacy HTTP header timing stays available: {rows:?}"
    );
}

#[tokio::test]
async fn bounded_error_body_keeps_upstream_identity_without_forwarding_a_partial_body() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(30, Some(usage_recorder(d.path()))).await;
    let response = e
        .post("/v1/responses")
        .header("x-test-mode", "error_too_large")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    let body = response.text().await.unwrap();
    assert!(body.len() < 1024 && body.contains("Bad gateway"), "{body}");
    assert_eq!(
        e.mock.hits.load(Ordering::SeqCst),
        1,
        "an incomplete body cannot classify a safe failover"
    );
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "error_body_too_large", "{rows:?}");
    assert_eq!(rows[0]["upstream_status"], 503, "{rows:?}");
    let detail: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(
        detail["upstream_request_id"], "oversized-upstream",
        "{detail}"
    );
    assert!(
        detail["message"]
            .as_str()
            .unwrap()
            .contains("body incomplete"),
        "{detail}"
    );
}

#[tokio::test]
async fn bounded_models_body_is_not_cached_when_incomplete() {
    let e = env(30).await;
    for _ in 0..2 {
        let response = e
            .http
            .get(format!("http://{}/v1/models", e.proxy))
            .bearer_auth(KEY)
            .header("x-test-mode", "models_too_large")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 502);
        assert!(response.bytes().await.unwrap().len() < 1024);
    }
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn bounded_preparation_preserves_error_headers_when_it_interrupts_body_read() {
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\n",
    )
    .await;
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        e.post("/v1/responses")
            .header("x-test-mode", "error_stall")
            .body(r#"{"model":"gpt-5.5","input":[]}"#)
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), 504);
    response.bytes().await.unwrap();
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout", "{rows:?}");
    assert_eq!(rows[0]["upstream_status"], 503, "{rows:?}");
    let detail: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(
        detail["upstream_request_id"], "stalled-upstream",
        "{detail}"
    );
    assert!(
        detail["message"]
            .as_str()
            .unwrap()
            .contains("no complete response forwarded"),
        "{detail}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn websocket_relay_and_limit_close() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    req.headers_mut()
        .insert("session_id", "ws-1".parse().unwrap());
    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(resp.headers()["x-codex-turn-state"], "ts-1");
    assert!(resp.headers().get("set-cookie").is_none());
    assert!(resp.headers().get("set-cookie2").is_none());
    ws.send(M::text(r#"{"type":"response.create","n":1}"#))
        .await
        .unwrap();
    let m = ws.next().await.unwrap().unwrap();
    assert!(m.to_text().unwrap().contains("\"echo\""));
    // Quota error after response.created, before output: replayed on account b.
    ws.send(M::text(r#"{"type":"response.create","tag":"limit"}"#))
        .await
        .unwrap();
    let mut got = Vec::new();
    loop {
        let m = ws.next().await.unwrap().unwrap();
        let t = m.to_text().unwrap().to_string();
        assert!(
            !t.contains("usage_limit_reached"),
            "quota frame leaked: {t}"
        );
        got.push(t.clone());
        if t.contains("response.completed") {
            break;
        }
    }
    assert_eq!(got.len(), 3, "{got:?}");
    assert!(got[0].contains("r-b") && got[1].contains("delta"));
    // Incremental request hits a quota error: no frame forwarded, close 1012 for replay.
    ws.send(M::text(
        r#"{"type":"response.create","previous_response_id":"r-b","tag":"prevlimit"}"#,
    ))
    .await
    .unwrap();
    match ws.next().await.unwrap().unwrap() {
        M::Close(Some(f)) => {
            assert_eq!(u16::from(f.code), 1012);
            assert_eq!(f.reason.as_str(), "upstream requires HTTP replay");
        }
        other => panic!("expected close, got {other:?}"),
    }
    let seen = e.mock.seen.lock().unwrap();
    let auth = seen[0]
        .0
        .iter()
        .find(|(k, _)| k == "authorization")
        .unwrap()
        .1
        .clone();
    assert_eq!(auth, "Bearer tok-a-1");
    drop(seen);
    assert!(e.outcomes().iter().any(|(a, o)| a == "a"
        && matches!(
            o,
            Outcome::RateLimited {
                usage_limit: true,
                retry_after: Some(d),
                ..
            } if *d == Duration::from_secs(45)
        )));
}

#[tokio::test]
async fn websocket_messages_share_the_buffered_body_budget() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_buffered_body_mb = 1;
    e.config.store(cfg);
    let mib = 1024 * 1024u64;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let msg = |tag: &str, len: usize| {
        let head = format!(r#"{{"type":"response.create","tag":"{tag}","pad":""#);
        M::text(format!("{head}{}\"}}", "x".repeat(len - head.len() - 2)))
    };
    async fn text(
        ws: &mut (impl futures::Stream<
            Item = Result<
                tokio_tungstenite::tungstenite::Message,
                tokio_tungstenite::tungstenite::Error,
            >,
        > + Unpin),
    ) -> String {
        loop {
            match ws.next().await.unwrap().unwrap() {
                M::Text(t) => return t.as_str().to_string(),
                M::Ping(_) | M::Pong(_) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    // Another request holds 900 KiB of the 1 MiB budget: a 200 KiB message waits about 1 s,
    // then gets the HTTP busy wording as a wrapped 503 error event; it is not forwarded.
    let mut other = e.drain.body_permit();
    e.drain
        .reserve_body(&mut other, 900 * 1024, mib, Duration::ZERO)
        .await
        .unwrap();
    let t = std::time::Instant::now();
    ws.send(msg("refused", 200 * 1024)).await.unwrap();
    let busy: serde_json::Value = serde_json::from_str(&text(&mut ws).await).unwrap();
    assert!(
        t.elapsed() >= Duration::from_millis(900),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(
        busy,
        serde_json::json!({
            "type": "error",
            "status": 503,
            "error": {"type": "gateway_busy", "message": "Gateway is busy; retry shortly."},
            "headers": {"retry-after": "3", "retry-after-ms": "3000"},
        })
    );
    assert_eq!(e.drain.body_bytes(), 900 * 1024);
    assert_eq!(e.drain.ws_bytes(), 0, "the refused message holds nothing");

    // Room again: the connection is still usable and the next reply is for this message.
    drop(other);
    ws.send(msg("next", 200 * 1024)).await.unwrap();
    let echo = text(&mut ws).await;
    assert!(echo.contains("\"tag\":\"next\"") && !echo.contains("refused"));
    // Released once the turn completes.
    for _ in 0..50 {
        if e.drain.body_bytes() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(e.drain.body_bytes(), 0);

    // Held for the running turn (its replay copy stays in memory).
    ws.send(M::text(r#"{"type":"response.create","tag":"held"}"#))
        .await
        .unwrap();
    assert!(text(&mut ws).await.contains("held"));
    assert_eq!(
        e.drain.body_bytes(),
        r#"{"type":"response.create","tag":"held"}"#.len() as u64
    );
    assert_eq!(e.drain.ws_bytes(), e.drain.body_bytes());

    // A message larger than the whole budget is not refused forever: it waits for all of it.
    ws.send(msg("whole", (3 * mib / 2) as usize)).await.unwrap();
    let echo = text(&mut ws).await;
    assert!(echo.contains("\"tag\":\"whole\""), "{}", &echo[..80]);
    ws.close(None).await.unwrap();
    for _ in 0..100 {
        if e.drain.body_bytes() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(e.drain.body_bytes(), 0);
    assert_eq!(e.drain.ws_bytes(), 0);
}

async fn sse_text(e: &Env, mode: &str) -> String {
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", mode)
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    r.text().await.unwrap()
}

#[tokio::test]
async fn sse_quota_before_output_switches_transparently() {
    let e = env(300).await;
    let t = sse_text(&e, "midlimit").await;
    assert!(!t.contains("usage_limit_reached"), "{t}");
    assert!(t.contains("r-b") && t.contains("response.completed"), "{t}");
    assert!(!t.contains("r-a"), "{t}");
    let seen = e.mock.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].1, seen[1].1, "same body replayed");
    drop(seen);
    assert!(e.outcomes().iter().any(|(a, o)| a == "a" && matches!(
        o,
        Outcome::RateLimited { usage_limit: true, retry_after: Some(d), .. } if *d == Duration::from_secs(45)
    )));
}

#[tokio::test]
async fn sse_quota_after_output_is_forwarded_and_cools() {
    let e = env(300).await;
    let t = sse_text(&e, "latelimit").await;
    assert!(
        t.contains("delta") && t.contains("usage_limit_reached"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert!(e.outcomes().iter().any(|(a, o)| a == "a"
        && matches!(
            o,
            Outcome::RateLimited {
                usage_limit: true,
                ..
            }
        )));
}

#[tokio::test]
async fn images_wrap_and_direct() {
    let e = env(300).await;
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image")
        .json(&serde_json::json!({"prompt":"cat","size":"1024x1024"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["data"][0]["b64_json"], "SU1H");
    assert_eq!(v["created"], 7);
    assert_eq!(v["usage"]["images"], 1);
    let sent: serde_json::Value =
        serde_json::from_slice(&e.mock.seen.lock().unwrap()[0].1).unwrap();
    assert_eq!(sent["tools"][0]["type"], "image_generation");
    assert_eq!(sent["tools"][0]["model"], "gpt-image-2");
    assert_eq!(sent["model"], "gpt-6.1-sol");
    assert!(
        !e.mock.seen.lock().unwrap()[0]
            .0
            .iter()
            .any(|(k, _)| k == "content-encoding"),
        "images wrap is never compressed"
    );

    // streaming wrap
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image")
        .json(&serde_json::json!({"prompt":"cat","stream":true,"model":"gpt-image-1"}))
        .send()
        .await
        .unwrap();
    let t = r.text().await.unwrap();
    assert!(
        t.contains("event: image_generation.partial_image")
            && t.contains("event: image_generation.completed"),
        "{t}"
    );

    // direct (gpt-image-2) multipart edit → JSON
    let mp = "--XB\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-image-2\r\n--XB\r\nContent-Disposition: form-data; name=\"prompt\"\r\n\r\nedit it\r\n--XB\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"a.png\"\r\n\r\n\x7fPNGxx\r\n--XB--\r\n";
    let mp = mp.replace('\x7f', "\u{89}");
    let r = e
        .post("/v1/images/edits")
        .header("content-type", "multipart/form-data; boundary=XB")
        .body(mp)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let seen = e.mock.seen.lock().unwrap();
    let j: serde_json::Value = serde_json::from_slice(&seen.last().unwrap().1).unwrap();
    assert_eq!(j["model"], "gpt-image-2");
    assert!(j["images"][0]["image_url"]
        .as_str()
        .unwrap()
        .starts_with("data:"));
    assert_eq!(j["prompt"], "edit it");
    assert!(seen
        .last()
        .unwrap()
        .0
        .iter()
        .any(|(k, v)| k == "originator" && v == "codex_exec"));
}

#[tokio::test]
async fn image_records_wait_for_body_and_direct_sse_is_not_responses() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image_body_late_error")
        .json(&serde_json::json!({"model":"gpt-image-2","prompt":"cat"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        rec.stats().accepted,
        0,
        "headers alone must not record success"
    );
    e.mock.release.notify_one();
    assert!(r.bytes().await.is_err());
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image_direct_sse")
        .json(&serde_json::json!({"model":"gpt-image-2","prompt":"cat","stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.text().await.unwrap(),
        IMAGE_DIRECT_SSE,
        "no fabricated Responses failure at EOF"
    );
    let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 2))
        .await
        .unwrap();
    let failed = rows
        .iter()
        .find(|r| r["error"] == "stream_read_error")
        .unwrap();
    assert_eq!(failed["status"], 200);
    assert_eq!(failed["err_source"], "network");
    assert_eq!(failed["account"], "a");
    let ok = rows.iter().find(|r| r["error"].is_null()).unwrap();
    assert_eq!(ok["transport"], "sse");
    for r in rows {
        assert!(r["ttft_ms"].is_null());
        assert!(r["input_tokens"].is_null() && r["output_tokens"].is_null());
    }
    rec.shutdown();
}

#[tokio::test]
async fn image_direct_errors_are_observed_without_changing_bytes_or_body_lifetime() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for (i, mode) in [
        "image_error_event",
        "image_error_type",
        "image_error_after_large",
        "image_error_words",
    ]
    .into_iter()
    .enumerate()
    {
        let expected = image_event_fixture(mode);
        let r = e
            .post("/v1/images/generations")
            .header("x-test-mode", mode)
            .json(&serde_json::json!({"model":"gpt-image-2","prompt":"cat","stream":true}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let mut body = r.bytes_stream();
        let mut got = Vec::new();
        while got.len() < expected.len() {
            got.extend_from_slice(
                &tokio::time::timeout(Duration::from_secs(3), body.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(
            got,
            expected.as_bytes(),
            "original Images bytes must be delivered before EOF"
        );
        assert_eq!(
            rec.stats().accepted,
            i as u64,
            "observing an error must not finalize a pending Body"
        );
        e.mock.release.notify_one();
        while let Some(chunk) = body.next().await {
            got.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(
            got,
            expected.as_bytes(),
            "must not append a Responses error event"
        );
        let rows = tokio::time::timeout(Duration::from_secs(3), wait_rows(&e, i + 1))
            .await
            .unwrap();
        let row = &rows[0];
        assert_eq!(row["status"], 200);
        assert_eq!(row["transport"], "sse");
        assert_eq!(row["account"], "a");
        assert!(
            row["ttft_ms"].is_null()
                && row["input_tokens"].is_null()
                && row["output_tokens"].is_null()
        );
        if mode == "image_error_words" {
            assert!(
                row["error"].is_null(),
                "nested or literal error text must not classify the event"
            );
        } else {
            assert_eq!(row["error"], "upstream_error_event");
            assert_eq!(row["err_source"], "stream");
            let detail: serde_json::Value =
                detail_of(&e, row["request_id"].as_str().unwrap(), true)
                    .await
                    .json()
                    .await
                    .unwrap();
            assert_eq!(detail["upstream_request_id"], "images-fixture-request");
            assert!(detail["message"]
                .as_str()
                .unwrap()
                .contains("fixture image failure"));
            assert!(!detail["body"].as_str().unwrap().contains(&"A".repeat(100)));
        }
    }
    assert_eq!(
        e.mock.hits.load(Ordering::SeqCst),
        4,
        "no replay of image failures"
    );
    assert!(e
        .outcomes()
        .iter()
        .all(|(_, o)| matches!(o, Outcome::Success { .. })));
    rec.shutdown();
}

#[tokio::test]
async fn image_wrapped_results_record_success_failure_and_truncated_eof() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for (mode, stream) in [
        ("image", false),
        ("image", true),
        ("image_failed", false),
        ("image_failed", true),
        ("image_truncated", true),
    ] {
        let r = e
            .post("/v1/images/generations")
            .header("x-test-mode", mode)
            .json(&serde_json::json!({"model":"gpt-image-1","prompt":"cat","stream":stream}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            r.status().as_u16(),
            if mode == "image_failed" && !stream {
                502
            } else {
                200
            }
        );
        let body = r.text().await.unwrap();
        assert!(if mode == "image" {
            body.contains("SU1H")
        } else {
            body.contains("error")
        });
    }
    let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 5))
        .await
        .unwrap();
    assert_eq!(rows.iter().filter(|r| r["error"].is_null()).count(), 2);
    assert_eq!(
        rows.iter().filter(|r| r["error"] == "image_failed").count(),
        2
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r["error"] == "stream_truncated")
            .count(),
        1
    );
    assert_eq!(rows.iter().filter(|r| r["transport"] == "sse").count(), 3);
    for r in rows {
        assert_eq!(r["account"], "a");
        assert_eq!(r["model"], "gpt-image-1");
        assert!(r["ttft_ms"].is_null());
        assert!(
            r["input_tokens"].is_null() && r["output_tokens"].is_null(),
            "internal text model usage is not image-token usage: {r}"
        );
        if !r["error"].is_null() {
            assert_eq!(r["err_source"], "stream");
        }
        if r["error"] == "image_failed" {
            let detail: serde_json::Value = detail_of(&e, r["request_id"].as_str().unwrap(), true)
                .await
                .json()
                .await
                .unwrap();
            assert_eq!(detail["headers"]["x-request-id"], "image-upstream-id");
        }
    }
    rec.shutdown();
}

#[tokio::test]
async fn empty_http_and_image_bodies_complete_without_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for path in ["/v1/images/generations", "/v1/responses"] {
        for mode in ["empty_200", "empty_204"] {
            let r = e
                .post(path)
                .header("x-test-mode", mode)
                .json(&serde_json::json!({"model":"gpt-image-2","stream":false,"input":[],"prompt":"cat"}))
                .send()
                .await
                .unwrap();
            assert_eq!(
                r.status().as_u16(),
                if mode == "empty_204" { 204 } else { 200 }
            );
            assert!(r.bytes().await.unwrap().is_empty());
        }
    }
    let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 4))
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);
    for row in rows {
        assert!(
            row["error"].is_null(),
            "empty HTTP framing proves EOF: {row}"
        );
        assert_eq!(row["transport"], "http");
    }
    rec.shutdown();
}

#[tokio::test]
async fn cancel_before_headers_keeps_selected_account() {
    use tower::ServiceExt;
    for (path, mode, body) in [
        (
            "/v1/images/generations",
            "image_header_stall",
            r#"{"model":"gpt-image-2","prompt":"cat"}"#,
        ),
        (
            "/v1/responses",
            "header_stall_a",
            r#"{"model":"gpt-5.5","stream":true,"input":[]}"#,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let rec = usage_recorder(dir.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let req = http::Request::post(path)
            .header("authorization", format!("Bearer {KEY}"))
            .header("x-test-mode", mode)
            .body(Body::from(body))
            .unwrap();
        let call = tokio::spawn(e.app.clone().oneshot(req));
        tokio::time::timeout(Duration::from_secs(3), e.mock.ready.notified())
            .await
            .unwrap();
        assert_eq!(rec.stats().accepted, 0);
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 1))
            .await
            .unwrap();
        assert_eq!(rows[0]["error"], "client_closed");
        assert_eq!(rows[0]["err_source"], "client");
        assert_eq!(rows[0]["status"], 0);
        assert_eq!(rows[0]["account"], "a");
        assert_eq!(rows[0]["switches"], 0);
        assert!(rows[0]["first_account"].is_null());
        let detail: serde_json::Value =
            detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
                .await
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
        assert!(
            detail["attempts"].is_null(),
            "no completed HTTP attempt: {detail}"
        );
        assert_eq!(e.drain.inflight(), 0);
        e.mock.release.notify_one();
        rec.shutdown();
    }
}

#[tokio::test]
async fn no_account_record_has_no_selected_account() {
    use tower::ServiceExt;
    let dir = tempfile::tempdir().unwrap();
    let usage = usage_recorder(dir.path());
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://127.0.0.1:9/backend-api\"\n"
    ))
    .unwrap();
    let app = ciallopa::build_app(AppState {
        accounts: Arc::new(Store {
            ids: vec![],
            refreshed: AtomicUsize::new(0),
        }),
        router: Arc::new(Rec::default()),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        config: Arc::new(cfg.into()),
        usage: Some(usage.clone()),
        egress: Default::default(),
    });
    let response = app
        .oneshot(
            http::Request::post("/v1/responses")
                .header("authorization", format!("Bearer {KEY}"))
                .body(Body::from(r#"{"stream":true,"input":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    usage.shutdown();
    let rows = usage
        .recent(ciallopa_usage::RecentQuery {
            limit: 5,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].error.as_deref(), Some("no_account"));
    assert!(rows[0].account.is_none());
    assert!(rows[0].first_account.is_none());
    assert_eq!(rows[0].switches, 0);
}

#[tokio::test]
async fn nonstream_http_body_cancel_and_force_are_not_successes() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for force in [false, true] {
        let r = e
            .post("/v1/responses")
            .header("x-test-mode", "json_body_stall")
            .json(&serde_json::json!({"model":"gpt-5.5","stream":false,"input":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        if force {
            e.drain.force();
        }
        drop(r);
        let n = if force { 2 } else { 1 };
        let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, n))
            .await
            .unwrap();
        let row = rows
            .iter()
            .find(|r| r["error"] == if force { "shutdown" } else { "client_closed" })
            .unwrap();
        assert_eq!(row["transport"], "http");
        assert_eq!(row["status"], 200);
        assert_eq!(row["err_source"], if force { "local" } else { "client" });
    }
    rec.shutdown();
}

/// Codex 0.162 remote compaction v2: a normal `/responses` turn whose input ends with
/// `{"type":"compaction_trigger"}`; reply carries one `compaction` output item.
#[tokio::test]
async fn compaction_v2_passthrough() {
    let e = env(300).await;
    let body = r#"{"model":"gpt-5.5","stream":true,"input":[{"type":"message","role":"user","content":[]},{"type":"compaction_trigger"}],"client_metadata":{"x-codex-turn-metadata":"{\"request_kind\":\"compaction\"}"}}"#;
    for path in ["/v1/responses", "/backend-api/codex/responses"] {
        let r = e
            .post(path)
            .header("x-test-mode", "compact")
            .header("x-codex-beta-features", "remote_compaction_v2")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let t = r.text().await.unwrap();
        assert!(
            t.contains("response.compaction.compacting")
                && t.contains("\"encrypted_content\":\"gAAA\""),
            "{t}"
        );
    }
    let seen = e.mock.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for (h, b) in seen.iter() {
        assert!(h
            .iter()
            .any(|(k, v)| k == "content-encoding" && v == "zstd"));
        assert_eq!(
            zstd::decode_all(b.as_ref()).unwrap(),
            body.as_bytes(),
            "compressed compaction turn decodes to the original body"
        );
        assert!(h
            .iter()
            .any(|(k, v)| k == "x-codex-beta-features" && v == "remote_compaction_v2"));
    }
}

fn usage_recorder(dir: &std::path::Path) -> Arc<ciallopa_usage::Recorder> {
    ciallopa_usage::Recorder::open(&ciallopa_core::config::UsageConfig {
        path: dir.join("usage.db"),
        flush_ms: 5,
        ..Default::default()
    })
    .unwrap()
}

async fn usage_rows(e: &Env) -> Vec<serde_json::Value> {
    // the record is emitted when the stream task ends; poll the API
    for _ in 0..100 {
        let r = e
            .http
            .get(format!("http://{}/usage/v1/records?limit=50", e.proxy))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let v: serde_json::Value = r.json().await.unwrap();
        let rows = v["records"].as_array().unwrap().clone();
        if !rows.is_empty() {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    vec![]
}

async fn wait_rows(e: &Env, n: usize) -> Vec<serde_json::Value> {
    for _ in 0..100 {
        let rows = usage_rows(e).await;
        if rows.len() >= n {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    usage_rows(e).await
}

#[tokio::test]
async fn oversized_sse_event_keeps_its_existing_passthrough_contract() {
    for mode in [
        "event_large_completed",
        "event_large_literal",
        "event_large_plain",
        "event_large_delta_completed",
    ] {
        let d = tempfile::tempdir().unwrap();
        let rec = usage_recorder(d.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let text = e
            .post("/v1/responses")
            .header("x-test-mode", mode)
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        if mode == "event_large_completed" {
            assert!(text.starts_with("data: {\"type\":\"response.completed\""));
            assert!(text.ends_with("\"output_tokens\":0}}}\n\n"));
        } else {
            assert!(text.starts_with("data: {\"type\":\"response.output_text.delta\""));
        }
        assert_eq!(
            text.bytes().filter(|&b| b == b'x').count(),
            9 * 1024 * 1024 + usize::from(mode != "event_large_completed") // output_text
        );
        assert!(!text.contains("stream disconnected before completion"));
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["status"], 200);
        assert_eq!(rows[0]["transport"], "sse");
        if mode == "event_large_delta_completed" {
            // Observation can resume at the next complete event after the oversized one.
            assert!(rows[0]["error"].is_null(), "{rows:?}");
            assert_eq!(rows[0]["input_tokens"], 7);
        } else {
            assert_eq!(
                rows[0]["error"], "observation_incomplete",
                "{mode}: {rows:?}"
            );
            assert_eq!(rows[0]["err_source"], "local");
            assert!(rows[0]["input_tokens"].is_null());
            assert!(rows[0]["output_tokens"].is_null());
        }
        rec.shutdown();
    }
}

#[tokio::test]
async fn oversized_sse_observation_keeps_real_client_cancellation() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let d = tempfile::tempdir().unwrap();
        let rec = usage_recorder(d.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let response = e
            .post("/v1/responses")
            .header("x-test-mode", "event_large_held")
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap();
        let mut body = response.bytes_stream();
        let mut received = 0;
        while received < 9 * 1024 * 1024 {
            received += body.next().await.unwrap().unwrap().len();
        }
        drop(body);
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], "client_closed", "{rows:?}");
        assert_eq!(rows[0]["err_source"], "client");
        rec.shutdown();
    })
    .await
    .expect("cancelled large stream must release its tracker");
}

#[tokio::test]
async fn sse_observation_uses_event_type_not_payload_strings() {
    for (mode, error, output) in [
        ("event_literal_complete", None, Some(3)),
        ("event_literal_truncated", Some("stream_truncated"), None),
        ("event_metadata_only", None, Some(0)),
        ("event_done", None, Some(0)),
        // The upstream code is kept, like an HTTP error body.
        ("event_spaced_error", Some("bad_request"), None),
    ] {
        let d = tempfile::tempdir().unwrap();
        let rec = usage_recorder(d.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let text = e
            .post("/v1/responses")
            .header("x-test-mode", mode)
            .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(
            text.contains("stream disconnected before completion"),
            mode == "event_literal_truncated",
            "{mode}: {text}"
        );
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"].as_str(), error, "{mode}: {}", rows[0]);
        assert_eq!(
            rows[0]["output_tokens"].as_i64(),
            output,
            "{mode}: {}",
            rows[0]
        );
        if matches!(mode, "event_metadata_only" | "event_done") {
            assert!(rows[0]["ttft_ms"].is_null(), "{mode}: {}", rows[0]);
        }
        rec.shutdown();
    }
}

#[tokio::test]
async fn usage_sse_rows() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "usage")
        .body(r#"{"model":"gpt-5.5","reasoning":{"effort":"low"},"input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("r-u"));
    let rows = wait_rows(&e, 1).await;
    let u = &rows[0];
    assert_eq!(u["account"], "a");
    assert_eq!(u["model"], "gpt-5.5");
    assert_eq!(u["reasoning_effort"], "low");
    assert_eq!(u["transport"], "sse");
    assert_eq!(u["endpoint"], "responses");
    assert_eq!(u["status"], 200);
    assert!(u["error"].is_null(), "{u}");
    assert_eq!(u["input_tokens"], 120);
    assert_eq!(u["cached_tokens"], 100);
    assert_eq!(u["output_tokens"], 30);
    assert_eq!(u["reasoning_tokens"], 12);
    assert!(u["ttft_ms"].is_u64());
    assert_eq!(u["switches"], 0);

    // quota before output → transparent switch a→b
    sse_text(&e, "midlimit").await;
    // quota after output → recorded as usage_limit
    sse_text(&e, "latelimit").await;
    // 429 on a → failover to b
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "limit")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    r.text().await.unwrap();
    let rows = wait_rows(&e, 4).await;
    assert_eq!(rows.len(), 4, "{rows:?}");
    let (limit, late, mid) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!(mid["first_account"], "a");
    assert_eq!(mid["account"], "b");
    assert_eq!(mid["switches"], 1);
    assert!(mid["error"].is_null(), "{mid}");
    assert_eq!(late["error"], "usage_limit");
    assert_eq!(limit["account"], "b");
    assert_eq!(limit["switches"], 1);
    // Rescued requests carry their attempt chain; a clean one stores no detail.
    assert_eq!(u["has_detail"], false, "{u}");
    assert_eq!(u["rescued"], false);
    assert!(u["problem"].is_null(), "{u}");
    assert_eq!(mid["rescued"], true, "{mid}");
    assert_eq!(mid["upstream_fault"], true);
    assert_eq!(mid["next"], serde_json::json!(["attempts"]));
    assert_eq!(mid["has_detail"], true, "{mid}");
    let v: serde_json::Value = detail_of(&e, mid["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    let at = v["attempts"].as_array().unwrap();
    assert_eq!(at.len(), 3, "{v}");
    assert_eq!(at[1]["account"], "a");
    assert_eq!(at[1]["kind"], "stream");
    assert_eq!(at[1]["code"], "usage_limit");
    assert!(at[1]["action"].as_str().unwrap().ends_with("switch"), "{v}");
    assert_eq!(at[2]["account"], "b");
    assert_eq!(at[2]["action"], "ok");
    assert_eq!(late["problem"], "error");
    assert_eq!(late["cause"], "quota");
    assert_eq!(
        late["next"],
        serde_json::json!(["refresh_quota", "account"])
    );

    let s: serde_json::Value = e
        .http
        .get(format!("http://{}/usage/v1/summary?range=1h", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(s["summary"]["total"]["count"], 4);
    assert_eq!(s["summary"]["total"]["errors"], 1);
    assert_eq!(s["recorder"]["dropped"], 0);
    // API is behind the key
    let r = e
        .http
        .get(format!("http://{}/usage/v1/summary", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
}

#[tokio::test]
async fn usage_problem_filters_and_counts_api() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    sse_text(&e, "usage").await; // clean
    sse_text(&e, "midlimit").await; // quota before output → rescued on b
    sse_text(&e, "latelimit").await; // quota after output → 报错 / 额度
    assert_eq!(wait_rows(&e, 3).await.len(), 3);
    let get = |q: &str| {
        let (h, url) = (
            e.http.clone(),
            format!("http://{}/usage/v1/records{q}", e.proxy),
        );
        async move {
            let r = h.get(url).bearer_auth(KEY).send().await.unwrap();
            let st = r.status().as_u16();
            (st, r.json::<serde_json::Value>().await.unwrap_or_default())
        }
    };
    let (st, v) = get("?problem=error&cause=quota").await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["total"], 1, "{v}");
    assert_eq!(v["records"][0]["error"], "usage_limit");
    let (_, v) = get("?problem=upstream&cause=rescued").await;
    assert_eq!(v["total"], 1, "{v}");
    assert_eq!(v["records"][0]["account"], "b");
    let (_, v) = get("?problem=upstream").await;
    assert_eq!(
        v["total"], 2,
        "rescued plus the in-stream quota failure: {v}"
    );
    let (_, v) = get("?problem=all").await;
    assert_eq!(v["total"], 3, "{v}");
    let (_, c) = get("/counts?range=1h").await;
    assert_eq!(c["all"], 3, "{c}");
    assert_eq!(c["error"], 1);
    assert_eq!(c["upstream"], 2);
    assert_eq!(c["unfinished"], 0);
    assert_eq!(c["refused"], 0);
    assert_eq!(c["causes"]["error"]["quota"], 1);
    assert_eq!(c["causes"]["upstream"]["rescued"], 1);
    assert_eq!(c["causes"]["upstream"]["unrescued"], 1);
    assert_eq!(c["causes"]["unfinished"]["client_closed"], 0);
    let (_, c) = get("/counts?range=1h&account=b&problem=error").await;
    assert_eq!(c["all"], 1, "problem is ignored, account applies: {c}");
    assert!(c["refused"].is_null(), "{c}");
    for bad in [
        "?problem=slow",
        "?problem=error&cause=rescued",
        "?cause=quota",
        "?problem=refused",
        "/counts?problem=nope",
    ] {
        let (st, v) = get(bad).await;
        assert_eq!(st, 400, "{bad}: {v}");
    }
}

#[tokio::test]
async fn usage_counts_retries() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    sse_text(&e, "retry").await; // one 503, then ok on another account
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["retries"], 1, "{rows:?}");
    assert!(rows[0]["error"].is_null());
}

#[tokio::test]
async fn usage_does_not_recount_retries_after_sse_replay() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // HTTP 503 on a, SSE overload on b, then the one permitted overload replay on b.
    let text = sse_text(&e, "retry_then_overload").await;
    assert!(text.contains("response.completed"), "{text}");
    assert!(!text.contains("server_is_overloaded"), "{text}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["retries"], 1, "{rows:?}");
    assert_eq!(rows[0]["switches"], 1, "{rows:?}");
    assert_eq!(rows[0]["account"], "b", "{rows:?}");
    assert_eq!(rows[0]["first_account"], "a", "{rows:?}");
    assert!(rows[0]["error"].is_null(), "{rows:?}");
    assert_eq!(rows[0]["rescued"], true, "{rows:?}");
    let v: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    let actions: Vec<_> = v["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["action"].as_str().unwrap().to_string())
        .collect();
    assert!(actions.iter().any(|a| a.ends_with("wait+retry")), "{v}");
    assert_eq!(actions.last().unwrap(), "ok", "{v}");
}

#[tokio::test]
async fn usage_ws_turn_rows() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","reasoning":{"effort":"high"}}"#,
    ))
    .await
    .unwrap();
    ws.next().await.unwrap().unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"limit"}"#,
    ))
    .await
    .unwrap();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        if t.contains("response.completed") {
            break;
        }
    }
    ws.send(M::text(
        r#"{"type":"response.create","previous_response_id":"r-b","tag":"prevlimit"}"#,
    ))
    .await
    .unwrap();
    let _ = ws.next().await; // 1012 close
    let rows = wait_rows(&e, 3).await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    let (t3, t2, t1) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!(t1["transport"], "ws");
    assert_eq!(t1["reasoning_effort"], "high");
    assert_eq!(t1["account"], "a");
    assert!(t1["error"].is_null(), "{t1}");
    assert_eq!(t2["first_account"], "a");
    assert_eq!(t2["account"], "b");
    assert_eq!(t2["switches"], 1);
    assert!(t2["error"].is_null(), "{t2}");
    assert!(t2["ttft_ms"].is_u64());
    assert_eq!(t3["error"], "usage_limit");
    // A WS turn replayed on another account records its chain; a plain turn records none.
    assert_eq!(t1["has_detail"], false, "{t1}");
    assert_eq!(t2["rescued"], true, "{t2}");
    assert_eq!(t2["has_detail"], true, "{t2}");
    let v: serde_json::Value = detail_of(&e, t2["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(
        v["attempts"],
        serde_json::json!([
            {"account": "a", "t_ms": v["attempts"][0]["t_ms"], "kind": "stream", "code": "usage_limit", "action": "switch"},
            {"account": "b", "t_ms": v["attempts"][1]["t_ms"], "kind": "stream", "action": "ok"},
        ]),
        "{v}"
    );
}

#[tokio::test]
async fn usage_key_label_instance_and_quota_api() {
    let d = tempfile::tempdir().unwrap();
    let recorder = usage_recorder(d.path());
    recorder.set_prices(
        ciallopa_usage::PriceTable::parse(
            "[[model]]\nmodel=\"gpt-5.5\"\nin=5.0\ncache=0.5\nout=30.0\nfast_x=2.5\n",
        )
        .unwrap(),
    );
    let e = env_usage(300, Some(recorder)).await;
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "usage")
        .header("thread-id", "th-abc")
        .header(
            "x-codex-turn-metadata",
            r#"{"turn_id":"turn-1","thread_id":"th-abc"}"#,
        )
        .body(r#"{"model":"gpt-5.5","service_tier":"priority","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    r.text().await.unwrap();
    let rows = wait_rows(&e, 1).await;
    let u = &rows[0];
    let label = ciallopa_core::config::default_key_label(KEY);
    assert_eq!(u["key"], label.as_str());
    assert_eq!(u["instance"], ciallopa_core::config::hostname().as_str());
    assert_eq!(u["plan"], "plus");
    assert_eq!(u["primary_used_pct"], 12.0);
    assert!(u["primary_reset_at"].is_i64());
    assert_eq!(u["thread_id"], "th-abc");
    assert_eq!(u["turn_id"], "turn-1");
    assert_eq!(u["service_tier"], "priority");
    assert_eq!(u["cache_write_tokens"], 10);
    // (120-100-10)·5 + 100·0.5 + 10·5 (no write price ⇒ input) + 30·30, fast ×2.5
    let want = (10.0 * 5.0 + 100.0 * 0.5 + 10.0 * 5.0 + 30.0 * 30.0) * 2.5 / 1e6;
    assert!(
        (u["cost_usd"].as_f64().unwrap() - want).abs() < 1e-12,
        "{u}"
    );
    assert_eq!(u["price_status"], "ok");
    let get = |path: String| {
        let h = e.http.clone();
        async move {
            h.get(path)
                .bearer_auth(KEY)
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()
        }
    };
    // raw key never stored or returned
    assert!(!std::fs::read(d.path().join("usage.db"))
        .unwrap()
        .windows(KEY.len())
        .any(|w| w == KEY.as_bytes()));
    let all = get(format!("http://{}/usage/v1/records?key={label}", e.proxy)).await;
    assert_eq!(all["records"].as_array().unwrap().len(), 1);
    assert!(!all.to_string().contains(KEY));
    let none = get(format!("http://{}/usage/v1/records?key=other", e.proxy)).await;
    assert!(none["records"].as_array().unwrap().is_empty());
    // window, paging, tier and downgrade filters
    let n = |v: &serde_json::Value| v["records"].as_array().unwrap().len();
    let v = get(format!("http://{}/usage/v1/records?range=1h", e.proxy)).await;
    assert_eq!((n(&v), v["total"].as_u64()), (1, Some(1)), "{v}");
    assert!(v["next_offset"].is_null());
    let v = get(format!("http://{}/usage/v1/records?offset=1", e.proxy)).await;
    assert_eq!((n(&v), v["total"].as_u64()), (0, Some(1)));
    let v = get(format!("http://{}/usage/v1/records?from=1&to=2", e.proxy)).await;
    assert_eq!((n(&v), v["total"].as_u64()), (0, Some(0)));
    let v = get(format!("http://{}/usage/v1/records?tier=fast", e.proxy)).await;
    assert_eq!(n(&v), 1);
    let v = get(format!("http://{}/usage/v1/records?tier=standard", e.proxy)).await;
    assert_eq!(n(&v), 0);
    let v = get(format!("http://{}/usage/v1/records?downgraded=1", e.proxy)).await;
    assert_eq!(n(&v), 0);
    let s = get(format!(
        "http://{}/usage/v1/summary?group=key,instance",
        e.proxy
    ))
    .await;
    assert_eq!(s["summary"]["groups"][0]["group"]["key"], label.as_str());
    assert!((s["summary"]["groups"][0]["cost_usd"].as_f64().unwrap() - want).abs() < 1e-12);
    assert_eq!(s["summary"]["total"]["unknown_price"], 0);
    let s = get(format!(
        "http://{}/usage/v1/summary?group=account,tier",
        e.proxy
    ))
    .await;
    assert_eq!(
        s["summary"]["groups"][0]["group"]["service_tier"],
        "priority"
    );
    let r = e
        .http
        .get(format!("http://{}/usage/v1/summary?group=bogus", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    let q = get(format!("http://{}/usage/v1/quota", e.proxy)).await;
    assert!(q["note"].as_str().unwrap().contains("other clients"));
    let accts = q["accounts"].as_array().unwrap();
    assert_eq!(accts.len(), 2, "{q}");
    let a = accts.iter().find(|x| x["account"] == "a").unwrap();
    assert_eq!(a["needs_relogin"], false);
    assert_eq!(a["quota"]["primary"]["used_pct"], 12.0);
    assert_eq!(
        a["quota"]["primary"]["estimate"]["status"],
        "insufficient_data"
    );
    let b = accts.iter().find(|x| x["account"] == "b").unwrap();
    assert!(b["quota"].is_null());
}

#[tokio::test]
async fn ui_page_csp_and_toggle() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // static page needs no key and contains no data
    let r = e
        .http
        .get(format!("http://{}/ui", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let csp = r.headers()["content-security-policy"]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        csp.contains("default-src 'none'")
            && csp.contains("script-src 'sha256-")
            && csp.contains("connect-src 'self'"),
        "{csp}"
    );
    assert!(!csp.contains("unsafe-inline"));
    assert_eq!(r.headers()["x-content-type-options"], "nosniff");
    assert_eq!(r.headers()["cache-control"], "no-store, no-transform");
    let body = r.text().await.unwrap();
    assert!(body.contains("管理面板") && !body.contains(KEY));
    let r = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
        .get(format!("http://{}/", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["location"], "/ui");
    // APIs the page calls stay behind the key
    let r = e
        .http
        .get(format!("http://{}/usage/v1/quota", e.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    let off = env_cfg(300, None, "[ui]\nenabled = false\n").await;
    let r = off
        .http
        .get(format!("http://{}/ui", off.proxy))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn summary_day_buckets_api() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    sse_text(&e, "usage").await;
    wait_rows(&e, 1).await;
    let get = |q: &str| {
        let (h, u) = (
            e.http.clone(),
            format!("http://{}/usage/v1/summary?{q}", e.proxy),
        );
        async move { h.get(u).bearer_auth(KEY).send().await.unwrap() }
    };
    let v: serde_json::Value = get("range=7d&bucket=day&tz=480&group=account")
        .await
        .json()
        .await
        .unwrap();
    let b = v["summary"]["buckets"].as_array().unwrap();
    assert_eq!(b.len(), 1, "{v}");
    assert_eq!(b[0]["count"], 1);
    assert_eq!(b[0]["input_tokens"], 120);
    assert!(b[0]["day"].as_str().unwrap().len() == 10);
    let v: serde_json::Value = get("range=7d").await.json().await.unwrap();
    assert!(v["summary"].get("buckets").is_none());
    assert_eq!(get("bucket=week").await.status(), 400);
    let v: serde_json::Value = get("range=24h&bucket=hour&tz=0&group=account")
        .await
        .json()
        .await
        .unwrap();
    let b = v["summary"]["buckets"].as_array().unwrap();
    assert_eq!(
        (b.len(), b[0]["day"].as_str().unwrap().len()),
        (1, 13),
        "{v}"
    );
    assert_eq!(get("bucket=day&tz=9999").await.status(), 400);
    // numeric params next to the flattened filters
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let v: serde_json::Value = get(&format!("from={}&to={}", now - 60_000, now + 1000))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(v["summary"]["total"]["count"], 1, "{v}");
}

async fn detail_of(e: &Env, id: &str, auth: bool) -> reqwest::Response {
    let r = e
        .http
        .get(format!("http://{}/usage/v1/records/{id}/detail", e.proxy));
    let r = if auth { r.bearer_auth(KEY) } else { r };
    r.send().await.unwrap()
}

#[tokio::test]
async fn usage_error_detail_capture_and_api() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(1, Some(usage_recorder(d.path()))).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "errbody")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let t = r.text().await.unwrap();
    assert!(t.contains("bad input"), "{t}"); // client still sees the raw upstream body
    sse_text(&e, "latelimit").await; // in-stream response.failed
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "idle")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    let t = r.text().await.unwrap(); // idle timeout (1s) ends the stream with response.failed
    assert!(t.contains("event: response.failed"), "{t}");
    sse_text(&e, "usage").await; // success: no detail
    let rows = wait_rows(&e, 4).await;
    assert_eq!(rows.len(), 4, "{rows:?}");
    let (ok, idle, late, bad) = (&rows[0], &rows[1], &rows[2], &rows[3]);
    assert_eq!(ok["has_detail"], false);
    assert_eq!(bad["status"], 400);
    for r in [idle, late, bad] {
        assert_eq!(r["has_detail"], true, "{r}");
    }
    let id = |r: &serde_json::Value| r["request_id"].as_str().unwrap().to_string();

    // auth required; malformed / unknown ids
    assert_eq!(detail_of(&e, &id(bad), false).await.status(), 401);
    assert_eq!(detail_of(&e, "nothex", true).await.status(), 400);
    assert_eq!(detail_of(&e, &id(ok), true).await.status(), 404);

    let v: serde_json::Value = detail_of(&e, &id(bad), true).await.json().await.unwrap();
    let raw = v.to_string();
    for secret in ["SECRETSECRET", "alice@example.com", "supersecret"] {
        assert!(!raw.contains(secret), "{secret} leaked: {raw}");
    }
    assert_eq!(v["headers"]["cf-ray"], "ray-123");
    assert_eq!(v["headers"]["x-request-id"], "req_abc");
    assert_eq!(v["headers"]["x-codex-primary-used-percent"], "40");
    assert!(v["headers"].get("set-cookie").is_none());
    assert_eq!(v["truncated"], false);
    let body: serde_json::Value = serde_json::from_str(v["body"].as_str().unwrap()).unwrap();
    // Content allowlist drops non-envelope keys (stronger than [REDACTED]).
    assert!(
        body.get("refresh_token").is_none() || body["refresh_token"] == "[REDACTED]",
        "refresh_token must be absent or redacted: {body}"
    );
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("<img src=x"),
        "stored as data, escaped by the UI"
    );

    let v: serde_json::Value = detail_of(&e, &id(late), true).await.json().await.unwrap();
    assert!(
        v["body"].as_str().unwrap().contains("usage_limit_reached"),
        "{v}"
    );
    let v: serde_json::Value = detail_of(&e, &id(idle), true).await.json().await.unwrap();
    assert!(v["message"].as_str().unwrap().contains("idle"), "{v}");
}

fn hdr<'a>(h: &'a [(String, String)], k: &str) -> Option<&'a str> {
    h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

#[tokio::test]
async fn responses_compressed_like_codex_and_retries_reuse_bytes() {
    let e = env(300).await;
    let body = r#"{"model":"gpt-5.5","prompt_cache_key":"pk","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello hello hello"}]}]}"#;
    // 503, then 200 on another account: both attempts carry the same compressed bytes.
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "retry")
        .header("accept-encoding", "gzip, br")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers().get("content-encoding").is_none());
    assert!(r.text().await.unwrap().contains("response.completed"));
    let seen = e.mock.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    for (h, b) in &seen {
        assert_eq!(hdr(h, "content-encoding"), Some("zstd"));
        assert!(
            hdr(h, "accept-encoding").is_none(),
            "accept-encoding stripped"
        );
        assert_eq!(hdr(h, "content-length"), Some(b.len().to_string().as_str()));
        assert_eq!(zstd::decode_all(b.as_ref()).unwrap(), body.as_bytes());
        assert_eq!(b, &seen[0].1, "retries reuse the same bytes");
    }
    assert_eq!(
        seen[0].1.as_ref(),
        zstd::encode_all(body.as_bytes(), 3).unwrap().as_slice(),
        "zstd level 3"
    );
    // Session routing still sees the plaintext body.
    assert!(matches!(
        &e.outcomes().last().unwrap().1,
        Outcome::Success { .. }
    ));
}

#[tokio::test]
async fn compression_skips_compact_models_and_preencoded() {
    let e = env(300).await;
    let r = e
        .post("/v1/responses/compact")
        .header("accept-encoding", "gzip")
        .body(r#"{"model":"m"}"#)
        .send()
        .await
        .unwrap();
    // mock has no /compact route; only the request shape matters
    let _ = r.status();
    let pre = zstd::encode_all(&b"{\"model\":\"m\"}"[..], 19).unwrap();
    e.post("/backend-api/codex/responses")
        .header("content-encoding", "zstd")
        .body(pre.clone())
        .send()
        .await
        .unwrap();
    e.http
        .get(format!("http://{}/v1/models", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    let seen = e.mock.seen.lock().unwrap().clone();
    let (h, b) = &seen[0];
    assert_eq!(
        b.as_ref(),
        pre.as_slice(),
        "pre-encoded body byte-identical"
    );
    assert_eq!(hdr(h, "content-encoding"), Some("zstd"));
    let (h, _) = &seen[1];
    assert!(
        hdr(h, "content-encoding").is_none(),
        "models never compressed"
    );
}

#[tokio::test]
async fn compression_can_be_disabled() {
    let e = env_cfg(300, None, "compress_requests = false").await;
    let body = r#"{"model":"gpt-5.5","input":[]}"#;
    e.post("/v1/responses")
        .header("accept-encoding", "gzip")
        .body(body)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let seen = e.mock.seen.lock().unwrap().clone();
    let (h, b) = &seen[0];
    assert_eq!(b.as_ref(), body.as_bytes());
    assert!(hdr(h, "content-encoding").is_none());
    assert!(hdr(h, "accept-encoding").is_none());
}

async fn noct_post(e: &Env, path: &str, mode: &str, body: &str, accept: bool) -> (String, String) {
    let mut rb = e
        .post(path)
        .header("x-test-mode", mode)
        .header("x-test-noct", "1");
    if accept {
        rb = rb.header("accept", "text/event-stream");
    }
    let r = rb.body(body.to_string()).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let ct = r
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    (ct, r.text().await.unwrap())
}

#[tokio::test]
async fn sse_without_content_type_usage_and_failover() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let stream = r#"{"model":"gpt-5.5","stream":true,"input":[]}"#;
    // usage tokens: stream:true hint (v1) and Accept hint (backend-api, no stream field)
    let (ct, t) = noct_post(&e, "/v1/responses", "usage", stream, false).await;
    assert_eq!(ct, "text/event-stream", "content-type added downstream");
    assert!(t.contains("r-u"));
    let (_, t) = noct_post(
        &e,
        "/backend-api/codex/responses",
        "usage",
        r#"{"model":"gpt-5.5","input":[]}"#,
        true,
    )
    .await;
    assert!(t.contains("r-u"));
    let rows = wait_rows(&e, 2).await;
    for u in &rows {
        assert_eq!(u["input_tokens"], 120, "{u}");
        assert_eq!(u["output_tokens"], 30, "{u}");
        assert!(u["ttft_ms"].is_u64(), "{u}");
    }
    // pre-output usage_limit_reached replays on the next account, both paths
    for path in ["/v1/responses", "/backend-api/codex/responses"] {
        let before = e.mock.seen.lock().unwrap().len();
        let (_, t) = noct_post(&e, path, "midlimit", stream, false).await;
        assert!(!t.contains("usage_limit_reached"), "{path}: {t}");
        assert!(t.contains("r-b") && !t.contains("r-a"), "{path}: {t}");
        assert_eq!(e.mock.seen.lock().unwrap().len(), before + 2, "{path}");
    }
    // compaction turn without content-type
    let body = r#"{"model":"gpt-5.5","stream":true,"input":[{"type":"compaction_trigger"}]}"#;
    let (ct, t) = noct_post(&e, "/v1/responses", "compact", body, false).await;
    assert_eq!(ct, "text/event-stream");
    assert!(t.contains("\"encrypted_content\":\"gAAA\""), "{t}");
}

#[tokio::test]
async fn no_content_type_without_stream_hint_stays_passthrough() {
    let e = env(300).await;
    let (ct, t) = noct_post(
        &e,
        "/v1/responses",
        "midlimit",
        r#"{"model":"gpt-5.5","input":[]}"#,
        false,
    )
    .await;
    assert!(ct.is_empty(), "no hint: headers untouched");
    assert!(t.contains("usage_limit_reached"), "{t}");
}

#[tokio::test]
async fn usage_captures_response_model_effort_and_tier() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // SSE: Fast requested (legacy "fast"), downgraded; no request effort; dated snapshot.
    let r = e
        .post("/v1/responses")
        .header("x-test-mode", "meta")
        .body(r#"{"model":"gpt-5.5","service_tier":"fast","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    r.text().await.unwrap();
    // created-only stream: auto never becomes the served tier
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "meta_created_only")
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    r.text().await.unwrap();
    // WS turn: real mismatch + flex
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","service_tier":"flex","reasoning":{"effort":"high"},"tag":"meta"}"#,
    ))
    .await
    .unwrap();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        if t.contains("response.completed") {
            break;
        }
    }
    drop(ws);
    let rows = wait_rows(&e, 3).await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    let (w, created, sse) = (&rows[0], &rows[1], &rows[2]);
    assert_eq!(sse["requested_service_tier"], "priority");
    assert_eq!(sse["service_tier"], "default");
    assert_eq!(sse["response_model"], "gpt-5.5-2026-09-30");
    assert_eq!(
        sse["response_effort"], "medium",
        "completed wins over created"
    );
    assert!(sse["reasoning_effort"].is_null());
    assert_eq!(sse["model_mismatch"], false);
    assert_eq!(created["response_model"], "gpt-5.5");
    assert_eq!(created["response_effort"], "high");
    assert!(created["service_tier"].is_null(), "auto ignored: {created}");
    assert!(created["requested_service_tier"].is_null());
    assert_eq!(w["transport"], "ws");
    assert_eq!(w["requested_service_tier"], "flex");
    assert_eq!(w["service_tier"], "flex");
    assert_eq!(w["response_model"], "gpt-5.6-luna");
    assert_eq!(w["model_mismatch"], true);
    // summary grouped by response model
    let v: serde_json::Value = e
        .http
        .get(format!(
            "http://{}/usage/v1/summary?range=1h&group=response_model",
            e.proxy
        ))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let gs = v["summary"]["groups"].as_array().unwrap();
    assert!(
        gs.iter()
            .any(|g| g["group"]["response_model"] == "gpt-5.6-luna"),
        "{v}"
    );
}

fn turn_states(e: &Env) -> Vec<(String, Option<String>)> {
    e.mock
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|(h, _)| {
            (
                hdr(h, "authorization").unwrap_or("").to_string(),
                hdr(h, "x-codex-turn-state").map(str::to_string),
            )
        })
        .collect()
}

/// Item 6: a turn state issued by account a never reaches account b on failover; an unknown
/// one is kept on the first attempt only; the owner keeps receiving its own value.
#[tokio::test]
async fn turn_state_not_carried_across_accounts() {
    let e = env(300).await;
    // a issues ts-owned-a
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "ts_issue")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["x-codex-turn-state"], "ts-owned-a");
    // same turn: a is limited → failover to b must not carry a's turn state
    for ts in ["ts-owned-a", "ts-never-seen"] {
        e.mock.seen.lock().unwrap().clear();
        let r = e
            .post("/backend-api/codex/responses")
            .header("x-test-mode", "limit")
            .header("x-codex-turn-state", ts)
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let seen = turn_states(&e);
        assert_eq!(seen.len(), 2, "{seen:?}");
        assert!(seen[0].0.contains("tok-a"));
        assert_eq!(seen[0].1.as_deref(), Some(ts), "first attempt keeps it");
        assert!(seen[1].0.contains("tok-b"));
        assert_eq!(seen[1].1, None, "{ts} leaked to account b");
    }
}

/// Item 3: response.incomplete passes through, tokens recorded, flagged, not an error, no cooldown.
#[tokio::test]
async fn incomplete_passthrough_recorded_no_cooldown() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let body = sse_text(&e, "incomplete").await;
    assert!(body.contains("response.incomplete") && body.contains("max_output_tokens"));
    let rows = wait_rows(&e, 1).await;
    let r = &rows[0];
    assert_eq!(r["incomplete"], "max_output_tokens");
    assert!(r["error"].is_null(), "{r}");
    assert_eq!(r["input_tokens"], 40);
    assert_eq!(r["output_tokens"], 16);
    assert_eq!(r["reasoning_tokens"], 4);
    let o = e.outcomes();
    assert!(
        o.iter().all(|(_, o)| matches!(o, Outcome::Success { .. })),
        "{o:?}"
    );
    // a still serves the next request (no cooldown / switch)
    let _ = sse_text(&e, "").await;
    assert!(e.outcomes().iter().all(|(a, _)| a == "a"));
}

/// Item 4: a client close is answered with a close frame (closing handshake completes).
#[tokio::test]
async fn websocket_client_close_gets_close_frame() {
    use tokio_tungstenite::tungstenite::{
        client::IntoClientRequest,
        protocol::{frame::coding::CloseCode, CloseFrame},
        Message as M,
    };
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(r#"{"type":"response.create","n":1}"#))
        .await
        .unwrap();
    let _ = ws.next().await.unwrap().unwrap();
    ws.send(M::Close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "bye".into(),
    })))
    .await
    .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .expect("no close reply");
    match got {
        Some(Ok(M::Close(_))) => {}
        other => panic!("expected close frame, got {other:?}"),
    }
}

/// Item 1: the models catalog is passed through byte-for-byte (never rewritten, so ciallopa
/// cannot introduce `service_tiers: null`) on both client paths.
#[tokio::test]
async fn models_catalog_passthrough_no_null_service_tiers() {
    let e = env(300).await;
    for p in [
        "/v1/models",
        "/backend-api/codex/models?client_version=0.164.0",
    ] {
        let r = e
            .http
            .get(format!("http://{}{p}", e.proxy))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        let t = r.text().await.unwrap();
        assert_eq!(t, MODELS_BODY, "{p}");
        let v: serde_json::Value = serde_json::from_str(&t).unwrap();
        for m in v["models"].as_array().unwrap() {
            assert!(m["service_tiers"].is_array(), "{m}");
        }
        assert!(!t.contains("\"service_tiers\":null"));
    }
}

#[tokio::test]
async fn usage_limit_text_in_output_is_not_a_quota_error() {
    // H1 / L3: only `error` / `response.failed` events count, never raw bytes.
    let e = env(300).await;
    let t = sse_text(&e, "textlimit").await;
    assert!(t.contains("usage_limit_reached") && t.contains("response.completed"));
    let o = e.outcomes();
    assert!(
        !o.iter()
            .any(|(_, o)| matches!(o, Outcome::RateLimited { .. })),
        "{o:?}"
    );
}

#[tokio::test]
async fn bootstrap_transport_error_reaches_client() {
    // M2: an upstream error after `response.created` must not look like a clean EOF.
    let e = env(300).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "booterr")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let b = r.text().await.unwrap();
    assert!(
        b.contains("event: response.failed"),
        "client must see the stream error: {b}"
    );
    assert!(e
        .outcomes()
        .iter()
        .any(|(_, o)| *o == Outcome::Transient { status: None }));
}

#[tokio::test]
async fn bootstrap_idle_timeout_not_doubled() {
    // L1: idle=2s must fail after ~2s, not ~4s.
    let e = env(2).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "idle")
        .body("{}")
        .send()
        .await
        .unwrap();
    let t0 = std::time::Instant::now();
    assert!(r.text().await.unwrap().contains("response.failed"));
    let el = t0.elapsed();
    assert!(el < Duration::from_millis(3500), "took {el:?}");
}

#[tokio::test]
async fn response_failed_instructions_mention_is_not_quota() {
    let e = env(300).await;
    let t = sse_text(&e, "instrlimit").await;
    assert!(
        t.contains("server_error") && t.contains("usage_limit_reached"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1, "must not failover");
    assert!(
        !e.outcomes()
            .iter()
            .any(|(_, o)| matches!(o, Outcome::RateLimited { .. })),
        "{:?}",
        e.outcomes()
    );
}

struct RefreshFail;
#[async_trait]
impl AccountStore for RefreshFail {
    async fn list(&self) -> Vec<Account> {
        vec![Account {
            id: "a".into(),
            email: None,
            chatgpt_account_id: Some("acc-a".into()),
            plan_type: None,
            priority: 0,
            weight: 1,
            disabled: false,
        }]
    }
    async fn credentials(&self, _: &AccountId) -> Result<Credentials, AuthError> {
        Err(AuthError::Refresh("timeout".into()))
    }
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.credentials(id).await
    }
    async fn reload(&self) -> Result<(), AuthError> {
        Ok(())
    }
}

#[tokio::test]
async fn http_refresh_error_must_not_authfail() {
    let mock = Mock::default();
    let up = Router::new()
        .route("/backend-api/codex/responses", post(up_responses))
        .with_state(mock.clone());
    let up_addr = serve(up).await;
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n[upstream_opts]\nstream_idle_timeout_secs = 30\n"
    ))
    .unwrap();
    let rec = Arc::new(Rec::default());
    let state = AppState {
        accounts: Arc::new(RefreshFail),
        router: rec.clone(),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg).unwrap()),
        usage: None,
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let proxy = serve(ciallopa::build_app(state)).await;
    let r = reqwest::Client::new()
        .post(format!("http://{proxy}/backend-api/codex/responses"))
        .bearer_auth(KEY)
        .body("{}")
        .send()
        .await
        .unwrap();
    assert!(r.status().as_u16() >= 400 || r.status().as_u16() == 503 || r.status().as_u16() == 429);
    let o = rec.0.lock().unwrap().clone();
    assert!(
        !o.iter().any(|(_, x)| matches!(x, Outcome::AuthFailed)),
        "{o:?}"
    );
}

#[tokio::test]
async fn upstream_5xx_passed_through_with_provenance() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = e
        .post("/backend-api/codex/responses")
        .header("x-test-mode", "down")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    // Never rewritten into a local 429: the upstream status, body and request id pass through.
    assert_eq!(r.status(), 503);
    assert_eq!(r.headers()["x-request-id"], "req_down");
    assert!(r.headers().keys().all(|k| !k.as_str().contains("ciallopa")));
    let t = r.text().await.unwrap();
    assert!(t.contains("up busy"), "{t}");
    // One failover to another account, never the same account twice (max_attempts = 1).
    // Each account once, then one wait-and-retry on the last usable account.
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    let rows = wait_rows(&e, 1).await;
    let row = &rows[0];
    assert_eq!(row["err_source"], "upstream", "{row}");
    assert_eq!(row["error"], "server_is_overloaded");
    assert_eq!(row["upstream_status"], 503);
    let id = row["request_id"].as_str().unwrap().to_string();
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    assert_eq!(v["upstream_request_id"], "req_down");
    let at = v["attempts"].as_array().unwrap();
    assert_eq!(at.len(), 3, "{v}");
    assert_eq!(at[0]["action"], "switch");
    assert_eq!(at[1]["action"], "wait+retry");
    assert_eq!(at[2]["action"], "stop");
    assert_ne!(at[0]["account"], at[1]["account"]);
    assert_eq!(at[1]["account"], at[2]["account"]);
}

#[tokio::test]
async fn retried_success_keeps_attempt_chain() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    sse_text(&e, "retry").await;
    let rows = wait_rows(&e, 1).await;
    let row = &rows[0];
    assert!(row["error"].is_null(), "{row}");
    assert!(row["err_source"].is_null());
    assert_eq!(row["has_detail"], true);
    let id = row["request_id"].as_str().unwrap().to_string();
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    let at = v["attempts"].as_array().unwrap();
    assert_eq!(at.len(), 2, "{v}");
    assert_eq!(at[0]["action"], "switch");
    assert_eq!(at[1]["action"], "ok");
}

#[tokio::test]
async fn sse_overload_before_output_switches() {
    let e = env(300).await;
    let t = sse_text(&e, "midover").await;
    assert!(!t.contains("overloaded"), "{t}");
    assert!(
        t.contains("r-b") && t.contains("response.completed") && !t.contains("r-a"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    let o = e.outcomes();
    assert!(o
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { cool: true, .. })));
    assert!(!o
        .iter()
        .any(|(_, o)| matches!(o, Outcome::RateLimited { .. } | Outcome::AuthFailed)));
}

#[tokio::test]
async fn sse_overload_everywhere_passes_through_verbatim() {
    let e = env(300).await;
    let t = sse_text(&e, "allover").await;
    assert!(
        t.contains("server_is_overloaded") && t.contains("response.failed"),
        "{t}"
    );
    assert!(t.contains("\"r-a\""), "{t}");
    assert_eq!(
        e.mock.hits.load(Ordering::SeqCst),
        3,
        "each account once, plus one wait-and-retry"
    );
    let o = e.outcomes();
    // The last usable account is never cooled.
    assert!(o
        .iter()
        .any(|(a, o)| a == "b" && matches!(o, Outcome::Overloaded { cool: false, .. })));
    // Once per attempt on b: the passed-through event is not counted again by the stream.
    assert_eq!(
        o.iter()
            .filter(|(a, o)| a == "b" && matches!(o, Outcome::Overloaded { .. }))
            .count(),
        2,
        "{o:?}"
    );
}

#[tokio::test]
async fn sse_overload_after_output_is_forwarded() {
    let e = env(300).await;
    let t = sse_text(&e, "lateover").await;
    assert!(
        t.contains("delta") && t.contains("server_is_overloaded"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_400_overload_message_switches_despite_no_more_retry() {
    let e = env(300).await;
    let t = sse_text(&e, "over400").await;
    assert!(t.contains("r-b"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })));
}

#[tokio::test]
async fn sse_overload_last_account_waits_and_retries_once() {
    let e = env_cfg(300, None, "overload_retry_wait_secs = 1\n").await;
    let t0 = std::time::Instant::now();
    let t = sse_text(&e, "overonce").await;
    assert!(t.contains("r-b") && !t.contains("overloaded"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    assert!(t0.elapsed() >= std::time::Duration::from_millis(600));
}

#[tokio::test]
async fn sse_overload_no_more_retry_skips_wait() {
    let e = env_cfg(300, None, "overload_retry_wait_secs = 1\n").await;
    let t = sse_text(&e, "overnmr").await;
    assert!(t.contains("server_is_overloaded"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn sse_overload_wait_retry_disabled() {
    let e = env_cfg(300, None, "overload_retry_wait_secs = 0\n").await;
    let t = sse_text(&e, "allover").await;
    assert!(t.contains("server_is_overloaded"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
}

/// Codex drops the connection as soon as it has `response.completed`; the upstream body still
/// has a tail. That is a normal end, not `client_closed`. TTFT is the first output event.
#[tokio::test]
async fn sse_close_after_completed_is_not_an_error() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut r = e
        .post("/v1/responses")
        .header("x-test-mode", "slowtail")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    let mut got = Vec::new();
    while let Some(c) = r.chunk().await.unwrap() {
        got.extend_from_slice(&c);
        if String::from_utf8_lossy(&got).contains("response.completed") {
            break;
        }
    }
    drop(r);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let rows = wait_rows(&e, 1).await;
    let u = &rows[0];
    assert!(u["error"].is_null(), "{u}");
    assert!(u["err_source"].is_null(), "{u}");
    assert!(u["ttft_ms"].as_u64().unwrap() >= 250, "{u}");
}

#[tokio::test]
async fn sse_disconnect_while_upstream_idle_records_client_close_promptly() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut response = e
        .post("/v1/responses")
        .header("x-test-mode", "idle_after_output")
        .body(r#"{"model":"gpt-5.5","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.chunk().await.unwrap().is_some());
    drop(response);

    // The upstream never yields another chunk; client closure must finish the tracker
    // well before the configured 300 s upstream idle timeout.
    let rows = tokio::time::timeout(Duration::from_secs(5), usage_rows(&e))
        .await
        .expect("client disconnect releases the tracker promptly");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["error"], "client_closed", "{rows:?}");
    assert_eq!(rows[0]["err_source"], "client", "{rows:?}");
}

#[tokio::test]
async fn http_idle_snapshot_survives_reload_before_headers() {
    let e = env(10).await;
    for existing in [true, false] {
        let request = e
            .post("/v1/responses")
            .header("x-test-mode", "reload_idle")
            .body(r#"{"stream":true,"input":[]}"#);
        let response =
            tokio::spawn(async move { request.send().await.unwrap().text().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(3), e.mock.ready.notified())
            .await
            .expect("upstream reached the header barrier");
        if existing {
            let mut cfg = (*e.config.load()).clone();
            cfg.upstream_opts.stream_idle_timeout_secs = 1;
            e.config.store(cfg);
        }
        e.mock.release.notify_one();
        let body = tokio::time::timeout(Duration::from_secs(5), response)
            .await
            .unwrap()
            .unwrap();
        if existing {
            assert!(
                body.contains("response.completed"),
                "in-flight request changed its timeout: {body}"
            );
            assert!(!body.contains("response.failed"), "{body}");
        } else {
            assert!(
                body.contains("response.failed"),
                "next request did not use the new timeout: {body}"
            );
            assert!(!body.contains("response.completed"), "{body}");
        }
    }
}

/// WS: metadata frames are not the first token; `generate:false` prewarm is labelled and kept
/// out of request counts while its tokens still count.
#[tokio::test]
async fn ws_ttft_and_prewarm() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    for body in [
        r#"{"type":"response.create","model":"gpt-5.5","generate":false}"#,
        r#"{"type":"response.create","model":"gpt-5.5","tag":"slow"}"#,
    ] {
        ws.send(M::text(body)).await.unwrap();
        loop {
            let t = ws
                .next()
                .await
                .unwrap()
                .unwrap()
                .to_text()
                .unwrap()
                .to_string();
            if t.contains("response.completed") {
                break;
            }
        }
    }
    drop(ws);
    let rows = wait_rows(&e, 2).await;
    let (turn, warm) = (&rows[0], &rows[1]);
    assert_eq!(warm["endpoint"], "prewarm", "{warm}");
    assert_eq!(turn["endpoint"], "responses", "{turn}");
    assert!(turn["error"].is_null(), "{turn}");
    assert!(turn["ttft_ms"].as_u64().unwrap() >= 250, "{turn}");
    let s: serde_json::Value = e
        .http
        .get(format!("http://{}/usage/v1/summary?range=1h", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(s["summary"]["total"]["count"], 1, "{s}");
    assert_eq!(s["summary"]["total"]["input_tokens"], 13, "{s}");
}

// ---- client-leg permessage-deflate (ws_compression) ----

fn ws_req(e: &Env) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    req
}

/// Client config offering permessage-deflate (Codex: `DeflateConfig::default()`).
fn deflate_client(
    f: impl FnOnce(
        tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig,
    )
        -> tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig,
) -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    use tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig;
    let mut c = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    c.extensions.permessage_deflate = Some(f(DeflateConfig::default()));
    c
}

/// Sends `n` large, repetitive turns and checks each echo comes back intact.
async fn ws_roundtrips<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>, n: usize)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::Message as M;
    for i in 0..n {
        let input: String = (0..4000)
            .map(|k| format!("{{\"role\":\"user\",\"n\":{},\"t\":\"turn {i}\"}},", k % 97))
            .collect();
        let frame = serde_json::json!({"type":"response.create","model":"gpt-5.5","input":input})
            .to_string();
        ws.send(M::text(frame.clone())).await.unwrap();
        let got = ws.next().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(got.to_text().unwrap()).unwrap();
        assert_eq!(v["type"], "response.completed");
        assert_eq!(
            v["echo"],
            serde_json::from_str::<serde_json::Value>(&frame).unwrap()
        );
    }
}

fn ext(r: &http::Response<Option<Vec<u8>>>) -> Option<String> {
    r.headers()
        .get("sec-websocket-extensions")
        .map(|v| v.to_str().unwrap().to_string())
}

#[tokio::test]
async fn ws_deflate_negotiated_with_codex_offer() {
    let e = env(300).await;
    let (mut ws, resp) = tokio_tungstenite::connect_async_with_config(
        ws_req(&e),
        Some(deflate_client(|d| d)),
        false,
    )
    .await
    .unwrap();
    assert_eq!(ext(&resp).as_deref(), Some("permessage-deflate"));
    // No ciallopa-specific headers on the handshake.
    assert!(resp
        .headers()
        .keys()
        .all(|k| !k.as_str().contains("ciallopa")));
    ws_roundtrips(&mut ws, 3).await;
}

#[tokio::test]
async fn ws_deflate_params() {
    use tokio_tungstenite::tungstenite::protocol::Role;
    let e = env(300).await;
    let cfg = deflate_client(|d| {
        d.set_no_context_takeover(Role::Server, true)
            .set_no_context_takeover(Role::Client, true)
            .set_max_window_bits(Role::Server, 10)
            .unwrap()
            .set_max_window_bits(Role::Client, 12)
            .unwrap()
    });
    let (mut ws, resp) = tokio_tungstenite::connect_async_with_config(ws_req(&e), Some(cfg), false)
        .await
        .unwrap();
    assert_eq!(
        ext(&resp).as_deref(),
        Some("permessage-deflate; server_no_context_takeover; client_no_context_takeover; server_max_window_bits=10; client_max_window_bits=12")
    );
    ws_roundtrips(&mut ws, 3).await;
    // Context takeover on, smaller windows only.
    let cfg = deflate_client(|d| d.set_max_window_bits(Role::Server, 9).unwrap());
    let (mut ws, resp) = tokio_tungstenite::connect_async_with_config(ws_req(&e), Some(cfg), false)
        .await
        .unwrap();
    assert_eq!(
        ext(&resp).as_deref(),
        Some("permessage-deflate; server_max_window_bits=9")
    );
    ws_roundtrips(&mut ws, 2).await;
}

#[tokio::test]
async fn ws_plain_without_offer_or_when_disabled() {
    let e = env(300).await;
    // No offer → plain.
    let (mut ws, resp) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(ext(&resp), None);
    ws_roundtrips(&mut ws, 1).await;
    // Unacceptable offer → plain.
    let mut req = ws_req(&e);
    req.headers_mut().insert(
        "sec-websocket-extensions",
        "permessage-deflate; server_max_window_bits=8, x-webkit-deflate-frame"
            .parse()
            .unwrap(),
    );
    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(ext(&resp), None);
    ws_roundtrips(&mut ws, 1).await;
    // ws_compression = false applies to the next handshake (hot).
    let mut c = (*e.config.load()).clone();
    c.ws_compression = false;
    e.config.store(c.clone());
    let (mut ws, resp) = tokio_tungstenite::connect_async_with_config(
        ws_req(&e),
        Some(deflate_client(|d| d)),
        false,
    )
    .await
    .unwrap();
    assert_eq!(ext(&resp), None);
    ws_roundtrips(&mut ws, 1).await;
    c.ws_compression = true;
    e.config.store(c);
    let (_ws, resp) = tokio_tungstenite::connect_async_with_config(
        ws_req(&e),
        Some(deflate_client(|d| d)),
        false,
    )
    .await
    .unwrap();
    assert_eq!(ext(&resp).as_deref(), Some("permessage-deflate"));
}

/// The server really compresses (RSV1) once negotiated, and accepts an uncompressed client
/// frame on a deflate connection (RFC 7692 §6).
#[tokio::test]
async fn ws_deflate_wire_frames_compressed() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let e = env(300).await;
    let mut s = tokio::net::TcpStream::connect(&e.proxy).await.unwrap();
    s.write_all(format!(
        "GET /v1/responses HTTP/1.1\r\nHost: {}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\nAuthorization: Bearer {KEY}\r\n\r\n",
        e.proxy
    ).as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let mut b = [0u8; 1024];
        let n = s.read(&mut b).await.unwrap();
        assert!(n > 0);
        buf.extend_from_slice(&b[..n]);
    }
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
    assert!(head.starts_with("http/1.1 101"), "{head}");
    assert!(
        head.contains("sec-websocket-accept: s3pplmbitxaq9kygzzhzrbk+xoo="),
        "{head}"
    );
    assert!(
        head.contains("sec-websocket-extensions: permessage-deflate\r\n"),
        "{head}"
    );
    // Masked, uncompressed text frame.
    let payload: String = format!(
        r#"{{"type":"response.create","input":"{}"}}"#,
        "abcabcabc ".repeat(200)
    );
    let key = [1u8, 2, 3, 4];
    let mut f = vec![0x81u8, 0x80 | 126];
    f.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    f.extend_from_slice(&key);
    f.extend(payload.bytes().enumerate().map(|(i, b)| b ^ key[i % 4]));
    s.write_all(&f).await.unwrap();
    let mut rest = buf[end..].to_vec();
    while rest.len() < 4 {
        let mut b = [0u8; 4096];
        let n = s.read(&mut b).await.unwrap();
        assert!(n > 0);
        rest.extend_from_slice(&b[..n]);
    }
    assert_eq!(rest[0] & 0x0f, 1, "text frame");
    assert_eq!(rest[0] & 0x40, 0x40, "RSV1 (compressed) set");
    let len = match rest[1] & 0x7f {
        126 => u16::from_be_bytes([rest[2], rest[3]]) as usize,
        n => n as usize,
    };
    assert!(
        len < payload.len() / 4,
        "compressed {len} vs {}",
        payload.len()
    );
}

#[tokio::test]
async fn ws_preparation_budget_cancels_a_credential_lock_wait() {
    use ciallopa_auth::file::{write_auth_file, FileLock};
    use tokio_tungstenite::tungstenite::Error;

    let d = tempfile::tempdir().unwrap();
    let auth_dir = d.path().join("auths");
    write_auth_file(
        &auth_dir,
        "acct",
        &serde_json::json!({
            "type":"codex", "access_token":"old", "refresh_token":"fixture",
            "expired":"2000-01-01T00:00:00Z"
        }),
    )
    .unwrap();
    let accounts = Arc::new(
        ciallopa_auth::FileAccountStore::open_with(
            auth_dir.clone(),
            ciallopa_auth::OAuthClient::with_issuer("http://127.0.0.1:1", reqwest::Client::new()),
        )
        .await
        .unwrap(),
    );
    let lock = FileLock::acquire(&auth_dir, "acct").unwrap();
    let e = env_cfg_accounts(
        30,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\n",
        Some(accounts),
    )
    .await;
    let mut request = tokio::spawn(tokio_tungstenite::connect_async(ws_req(&e)));
    let result = tokio::time::timeout(Duration::from_secs(2), &mut request).await;
    if result.is_err() {
        request.abort();
    }
    assert!(
        result.is_ok(),
        "WS preparation must cancel the credential lock wait"
    );
    let Err(Error::Http(response)) = result.unwrap().unwrap() else {
        panic!("expected an HTTP preparation timeout");
    };
    assert_eq!(response.status(), 504);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout", "{rows:?}");
    assert_eq!(rows[0]["err_source"], "network", "{rows:?}");
    assert!(rows[0]["duration_ms"].as_u64().unwrap() >= 1000);
    assert_eq!(rows[0]["account"], "acct", "{rows:?}");
    assert_eq!(rows[0]["switches"], 0);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
    assert!(
        e.outcomes().is_empty(),
        "budget exhaustion does not cool an account"
    );
    assert_eq!(e.drain.wait_idle(Duration::from_secs(1)).await, 0);
    assert_eq!(e.drain.inflight(), 0);

    // The timed-out refresh must release its in-process lock too. A later request can
    // re-read the fresh token another process wrote, without reaching the OAuth endpoint.
    write_auth_file(
        &auth_dir,
        "acct",
        &serde_json::json!({
            "type":"codex", "access_token":"tok-a-1", "refresh_token":"fixture",
            "expired":"2999-01-01T00:00:00Z"
        }),
    )
    .unwrap();
    drop(lock);
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(2),
        tokio_tungstenite::connect_async(ws_req(&e)),
    )
    .await
    .unwrap()
    .unwrap();
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_preparation_budget_is_shared_across_handshake_retries() {
    use tokio_tungstenite::tungstenite::Error;
    let d = tempfile::tempdir().unwrap();
    let e = env_cfg(
        30,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\nconnect_timeout_secs = 5\n",
    )
    .await;
    let mut req = ws_req(&e);
    req.headers_mut()
        .insert("x-test-mode", "ws_retry_delay_fail".parse().unwrap());
    let mut request = tokio::spawn(tokio_tungstenite::connect_async(req));
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(650)).await;
    e.mock.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    // A fresh one-second budget for the second attempt would miss this bound.
    let result = tokio::time::timeout(Duration::from_millis(700), &mut request).await;
    e.mock.release.notify_waiters();
    if result.is_err() {
        request.abort();
    }
    assert!(
        result.is_ok(),
        "the second handshake must use only the remaining budget"
    );
    let Err(Error::Http(response)) = result.unwrap().unwrap() else {
        panic!("expected a preparation timeout");
    };
    assert_eq!(response.status(), 504);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout", "{rows:?}");
    assert_eq!(rows[0]["err_source"], "network", "{rows:?}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    assert_eq!(rows[0]["account"], "b", "{rows:?}");
    assert_eq!(rows[0]["first_account"], "a", "{rows:?}");
    assert_eq!(rows[0]["switches"], 1);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(1)).await, 0);
}

#[tokio::test]
async fn ws_preparation_timeout_keeps_the_candidate_waiting_for_credentials() {
    struct WaitingCredentials {
        store: Store,
        selected: tokio::sync::Notify,
    }
    #[async_trait]
    impl AccountStore for WaitingCredentials {
        async fn list(&self) -> Vec<Account> {
            self.store.list().await
        }
        async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
            if id == "b" {
                self.selected.notify_one();
                std::future::pending().await
            } else {
                self.store.credentials(id).await
            }
        }
        async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
            self.credentials(id).await
        }
        async fn reload(&self) -> Result<(), AuthError> {
            Ok(())
        }
    }
    let d = tempfile::tempdir().unwrap();
    let accounts = Arc::new(WaitingCredentials {
        store: Store {
            ids: vec!["a", "b"],
            refreshed: AtomicUsize::new(0),
        },
        selected: tokio::sync::Notify::new(),
    });
    let e = env_cfg_accounts(
        30,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\n",
        Some(accounts.clone()),
    )
    .await;
    let mut req = ws_req(&e);
    req.headers_mut()
        .insert("x-test-mode", "ws_retry_delay_fail".parse().unwrap());
    let request = tokio::spawn(tokio_tungstenite::connect_async(req));
    tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
        .await
        .unwrap();
    e.mock.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), accounts.selected.notified())
        .await
        .unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "preparation_timeout");
    assert_eq!(rows[0]["account"], "b");
    assert_eq!(rows[0]["first_account"], "a");
    assert_eq!(rows[0]["switches"], 1);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_eq!(e.drain.inflight(), 0);
}

#[tokio::test]
async fn ws_preparation_budget_is_shared_across_turn_replays() {
    use tokio_tungstenite::tungstenite::Message as M;
    for tag in ["budget_quota", "budget_overload"] {
        let d = tempfile::tempdir().unwrap();
        let accounts = Arc::new(Store {
            ids: vec!["a", "b", "c"],
            refreshed: AtomicUsize::new(0),
        });
        let e = env_cfg_accounts(
            30,
            Some(usage_recorder(d.path())),
            "preparation_timeout_secs = 1\nconnect_timeout_secs = 5\n",
            Some(accounts),
        )
        .await;
        let mut req = ws_req(&e);
        req.headers_mut()
            .insert("x-test-mode", "ws_budget_replay".parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        // Existing connections retain the preparation policy captured at their handshake.
        let mut cfg = (*e.config.load()).clone();
        cfg.upstream_opts.preparation_timeout_secs = 30;
        e.config.store(cfg);
        ws.send(M::text(format!(
            r#"{{"type":"response.create","tag":"{tag}"}}"#
        )))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(650)).await;
        e.mock.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_millis(700), ws.next()).await;
        e.mock.release.notify_waiters();
        assert!(
            result.is_ok(),
            "{tag}: every redial must share the turn's remaining budget"
        );
        assert!(
            matches!(result.unwrap(), Some(Ok(M::Close(Some(frame)))) if u16::from(frame.code) == 1012)
        );
        drop(ws);
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows.len(), 1, "{tag}: {rows:?}");
        assert_eq!(rows[0]["error"], "preparation_timeout", "{tag}: {rows:?}");
        assert_eq!(rows[0]["err_source"], "network", "{tag}: {rows:?}");
        assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
        assert_eq!(e.drain.wait_idle(Duration::from_secs(3)).await, 0);
    }
}

#[tokio::test]
async fn ws_preparation_budget_resets_each_turn_and_excludes_generation() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let accounts = Arc::new(Store {
        ids: vec!["a", "b", "c"],
        refreshed: AtomicUsize::new(0),
    });
    let e = env_cfg_accounts(
        1,
        Some(usage_recorder(d.path())),
        "preparation_timeout_secs = 1\n",
        Some(accounts),
    )
    .await;
    let mut req = ws_req(&e);
    req.headers_mut()
        .insert("x-test-mode", "ws_budget_turns".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    for tag in ["budget_long", "budget_next"] {
        ws.send(M::text(format!(
            r#"{{"type":"response.create","tag":"{tag}"}}"#
        )))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(4), async {
            let mut deltas = 0;
            loop {
                let message = ws.next().await.unwrap().unwrap();
                let text = message.to_text().expect("a valid long turn must not close");
                if text.contains("response.output_text.delta") {
                    deltas += 1;
                }
                if text.contains("response.completed") {
                    break;
                }
            }
            assert_eq!(deltas, 3);
        })
        .await
        .unwrap();
    }
    let rows = wait_rows(&e, 2).await;
    assert_eq!(rows.len(), 2, "successful handshakes are not business rows");
    for row in rows {
        assert!(row["error"].is_null(), "{row}");
        assert!(row["duration_ms"].as_u64().unwrap() > 1900, "{row}");
    }
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 3);
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn ws_failed_handshake_records_the_wait_before_failure() {
    for (mode, attempts) in [("ws_delay_fail", 1), ("ws_retry_delay_fail", 2)] {
        tokio::time::timeout(Duration::from_secs(5), async {
            let d = tempfile::tempdir().unwrap();
            let rec = usage_recorder(d.path());
            let e = env_usage(300, Some(rec.clone())).await;
            let mut req = ws_req(&e);
            req.headers_mut()
                .insert("x-test-mode", mode.parse().unwrap());
            let before_ms = ciallopa_usage::now_ms();
            let request = tokio::spawn(tokio_tungstenite::connect_async(req));
            let mut first_ready_ms = None;
            for _ in 0..attempts {
                e.mock.ready.notified().await;
                first_ready_ms.get_or_insert_with(ciallopa_usage::now_ms);
                tokio::time::sleep(Duration::from_millis(200)).await;
                e.mock.release.notify_one();
            }
            assert!(request.await.unwrap().is_err());
            let rows = wait_rows(&e, 1).await;
            assert_eq!(rows.len(), 1, "{mode}: {rows:?}");
            let row = &rows[0];
            assert_eq!(row["status"], 400, "{mode}: {row}");
            assert_eq!(row["transport"], "ws");
            assert_eq!(row["error"], "handshake_fixture");
            assert_eq!(row["account"], if attempts == 1 { "a" } else { "b" });
            assert_eq!(row["switches"], attempts - 1);
            if attempts == 1 {
                assert!(row["first_account"].is_null());
            } else {
                assert_eq!(row["first_account"], "a");
            }
            assert!(
                row["duration_ms"].as_u64().unwrap() >= 200 * attempts,
                "handshake preparation/retries must count: {mode}: {row}"
            );
            let started = row["ts_ms"].as_i64().unwrap();
            assert!(
                (before_ms..=first_ready_ms.unwrap()).contains(&started),
                "failed handshake belongs to its start window: {mode}: {row}"
            );
            assert_eq!(e.mock.hits.load(Ordering::SeqCst), attempts as usize);
            rec.shutdown();
        })
        .await
        .expect("handshake fixture must finish");
    }
}

#[tokio::test]
async fn ws_cancelled_handshake_keeps_selected_account() {
    use tokio::io::AsyncWriteExt;
    for (mode, attempts) in [("ws_delay_fail", 1), ("ws_retry_delay_fail", 2)] {
        let d = tempfile::tempdir().unwrap();
        let rec = usage_recorder(d.path());
        let e = env_usage(300, Some(rec.clone())).await;
        let mut client = tokio::net::TcpStream::connect(&e.proxy).await.unwrap();
        client.write_all(format!(
            "GET /v1/responses HTTP/1.1\r\nHost: {}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAuthorization: Bearer {KEY}\r\nx-test-mode: {mode}\r\n\r\n",
            e.proxy
        ).as_bytes()).await.unwrap();
        for attempt in 0..attempts {
            tokio::time::timeout(Duration::from_secs(2), e.mock.ready.notified())
                .await
                .expect("upstream handshake started");
            if attempt + 1 < attempts {
                e.mock.release.notify_one();
            }
        }
        assert_eq!(rec.stats().accepted, 0);
        client.shutdown().await.unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(3), wait_rows(&e, 1))
            .await
            .expect("a real client FIN cancels a pending handshake");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["status"], 0);
        assert_eq!(rows[0]["error"], "client_closed");
        assert_eq!(rows[0]["err_source"], "client");
        assert_eq!(rows[0]["account"], if attempts == 1 { "a" } else { "b" });
        assert_eq!(rows[0]["switches"], attempts - 1);
        if attempts == 2 {
            assert_eq!(rows[0]["first_account"], "a");
        }
        assert_eq!(e.drain.inflight(), 0);
        e.mock.release.notify_one();
        rec.shutdown();
    }
}

#[tokio::test]
async fn ws_unroutable_handshake_has_no_candidate_account() {
    let d = tempfile::tempdir().unwrap();
    let accounts = Arc::new(Store {
        ids: vec![],
        refreshed: AtomicUsize::new(0),
    });
    let e = env_cfg_accounts(30, Some(usage_recorder(d.path())), "", Some(accounts)).await;
    assert!(tokio_tungstenite::connect_async(ws_req(&e)).await.is_err());
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["error"], "no_account");
    assert!(rows[0]["account"].is_null());
    assert!(rows[0]["first_account"].is_null());
    assert_eq!(rows[0]["switches"], 0);
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ws_handshake_rejections_unchanged() {
    let e = env(300).await;
    let r = e
        .http
        .get(format!("http://{}/v1/responses", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(
        r.text().await.unwrap(),
        "Connection header did not include 'upgrade'"
    );
}

#[tokio::test]
async fn ws_upstream_transport_error_mid_turn_recorded_with_cause() {
    use tokio_tungstenite::tungstenite::Message as M;
    let warnings = StreamWarnings::default();
    let _subscriber = warnings.capture();
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"drop","client_metadata":{"thread_id":"diag-ws-thread","x-codex-turn-metadata":"{\"turn_id\":\"diag-ws-turn\"}"}}"#,
    ))
    .await
    .unwrap();
    let mut close = None;
    while let Some(Ok(m)) = ws.next().await {
        if let M::Close(f) = m {
            close = f;
            break;
        }
    }
    let f = close.expect("close frame");
    assert_eq!(u16::from(f.code), 1011);
    let rows = wait_rows(&e, 1).await;
    let r = &rows[0];
    // TCP gone without a close frame: an EOF, not a close frame or a read error.
    assert_eq!(r["error"], "upstream_ws_eof", "{r}");
    assert_eq!(r["err_source"], "network", "{r}");
    let id = r["request_id"].as_str().unwrap().to_string();
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    let msg = v["message"].as_str().unwrap_or_default();
    assert!(
        msg.starts_with("upstream websocket ended without a close frame (") && msg.len() > 50,
        "{v}"
    );
    let logs = warnings.0.lock().unwrap().clone();
    assert_eq!(logs.len(), 1, "{logs:?}");
    let log = &logs[0];
    assert_eq!(log["transport"], "ws");
    assert_eq!(log["error_class"], "eof");
    assert_eq!(log["last_event"], "response.created");
    assert_eq!(log["event_count"], 1);
    assert_eq!(log["terminal_received"], false);
    assert_eq!(log["record_id"], id);
    assert_eq!(log["upstream_request_id"], "ws-diagnostic-upstream-id");
    assert_eq!(log["thread_id"], "diag-ws-thread");
    assert_eq!(log["turn_id"], "diag-ws-turn");
    assert!(
        log.get("error").is_none(),
        "no raw transport error in process log"
    );
}

#[tokio::test]
async fn ws_replayed_attempt_diagnostics_use_current_handshake_and_fresh_progress() {
    use tokio_tungstenite::tungstenite::Message as M;
    let warnings = StreamWarnings::default();
    let _subscriber = warnings.capture();
    tokio::time::timeout(Duration::from_secs(8), async {
        for tag in ["limit", "metaover"] {
            warnings.0.lock().unwrap().clear();
            let d = tempfile::tempdir().unwrap();
            let e = env_usage(30, Some(usage_recorder(d.path()))).await;
            let mut req = ws_req(&e);
            req.headers_mut().insert("x-test-mode", "ws_replay_drop".parse().unwrap());
            let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
            ws.send(M::text(format!(r#"{{"type":"response.create","tag":"{tag}","client_metadata":{{"thread_id":"replay-thread","x-codex-turn-metadata":"{{\"turn_id\":\"replay-turn\"}}"}}}}"#))).await.unwrap();
            assert!(matches!(ws.next().await, Some(Ok(M::Close(Some(f)))) if u16::from(f.code) == 1011));
            drop(ws);
            let rows = wait_rows(&e, 1).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["first_account"], "a", "{tag}: {rows:?}");
            assert_eq!(rows[0]["account"], "b");
            assert_eq!(rows[0]["switches"], 1);
            assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
            let logs = warnings.0.lock().unwrap().clone();
            assert_eq!(logs.len(), 1, "{tag}: {logs:?}");
            let log = &logs[0];
            assert_eq!(log["transport"], "ws");
            assert_eq!(log["error_class"], "eof");
            assert_eq!(log["account"], "b");
            assert_eq!(log["upstream_request_id"], "ws-replay-upstream-id");
            assert_eq!(log["record_id"], rows[0]["request_id"]);
            assert_eq!(log["thread_id"], "replay-thread");
            assert_eq!(log["turn_id"], "replay-turn");
            assert_eq!(log["event_count"], 0);
            assert_eq!(log["terminal_received"], false);
            assert_eq!(log["activity_received"], false);
            assert!(log["last_event"].is_null());
        }
    }).await.expect("both replay diagnostic fixtures must finish");
}

#[tokio::test]
async fn ws_stream_diagnostics_count_ping_as_activity_and_hide_unknown_event_types() {
    use tokio_tungstenite::tungstenite::Message as M;
    let warnings = StreamWarnings::default();
    let _subscriber = warnings.capture();
    let e = env(1).await;
    for mode in ["diagnostic_close", "diagnostic_complete"] {
        warnings.0.lock().unwrap().clear();
        let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
        ws.send(M::text(format!(
            r#"{{"type":"response.create","tag":"{mode}"}}"#
        )))
        .await
        .unwrap();
        while let Some(Ok(m)) = ws.next().await {
            if matches!(m, M::Close(_)) {
                break;
            }
            if let M::Text(t) = m {
                if t.contains("response.completed") {
                    ws.close(None).await.unwrap();
                    break;
                }
            }
        }
        let logs = warnings.0.lock().unwrap().clone();
        if mode == "diagnostic_complete" {
            assert!(logs.is_empty(), "completed WS turn is quiet: {logs:?}");
            continue;
        }
        assert_eq!(logs.len(), 1, "{logs:?}");
        let log = &logs[0];
        assert_eq!(log["error_class"], "close_frame");
        assert_eq!(log["last_event"], "unknown");
        assert_eq!(log["event_count"], 1);
        assert_eq!(log["activity_received"], true);
        assert_eq!(log["terminal_received"], false);
        assert!(
            log["event_idle_ms"].as_u64().unwrap()
                >= log["activity_idle_ms"].as_u64().unwrap() + 80,
            "{log}"
        );
        assert!(!log.to_string().contains("private-"), "{log}");
    }
}

#[tokio::test]
async fn ws_upstream_close_frame_mid_turn_is_upstream_ws_close() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","tag":"diagnostic_close"}"#,
    ))
    .await
    .unwrap();
    let mut code = None;
    while let Some(Ok(m)) = ws.next().await {
        if let M::Close(f) = m {
            code = f.map(|f| u16::from(f.code));
            break;
        }
    }
    // The upstream close frame is relayed as is.
    assert_eq!(code, Some(1000));
    let rows = wait_rows(&e, 1).await;
    let r = &rows[0];
    assert_eq!(r["error"], "upstream_ws_close", "{r}");
    assert_eq!(r["err_source"], "stream", "{r}");
    let id = r["request_id"].as_str().unwrap().to_string();
    let v: serde_json::Value = detail_of(&e, &id, true).await.json().await.unwrap();
    let msg = v["message"].as_str().unwrap_or_default();
    assert!(
        msg.starts_with("upstream websocket closed mid-turn (code 1000)"),
        "{v}"
    );
}

async fn mode_post(e: &Env, mode: &str) -> reqwest::Response {
    e.post("/backend-api/codex/responses")
        .header("x-test-mode", mode)
        .body(r#"{"model":"gpt-x","input":[]}"#)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn payment_required_402_fails_over_and_cools() {
    let e = env(300).await;
    // deactivated_workspace: long account cooldown, transparent switch to b.
    let r = mode_post(&e, "deact").await;
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("response.completed"));
    let o = e.outcomes();
    assert_eq!(o[0].0, "a");
    assert!(matches!(
        o[0].1,
        Outcome::PaymentRequired { deactivated: true, cooldown } if cooldown == Duration::from_secs(86_400)
    ));
    assert_eq!(o[1].0, "b");
    assert!(matches!(o[1].1, Outcome::Success { .. }));
    // Any other 402: shorter account cooldown, also switches.
    let r = mode_post(&e, "pay402").await;
    assert_eq!(r.status(), 200);
    let o = e.outcomes();
    assert!(matches!(
        o[2].1,
        Outcome::PaymentRequired { deactivated: false, cooldown } if cooldown == Duration::from_secs(1_800)
    ));
    assert_eq!(o[3].0, "b");
    // Hot reload: the next request uses the new duration.
    let mut c = (*e.config.load()).clone();
    c.upstream_opts.deactivated_cooldown_secs = 120;
    e.config.store(c);
    assert_eq!(mode_post(&e, "deact").await.status(), 200);
    assert!(matches!(
        e.outcomes()[4].1,
        Outcome::PaymentRequired { deactivated: true, cooldown } if cooldown == Duration::from_secs(120)
    ));
}

#[tokio::test]
async fn payment_required_402_on_every_account_passes_upstream_error() {
    let e = env(300).await;
    let r = mode_post(&e, "deactall").await;
    assert_eq!(r.status(), 402);
    let body = r.text().await.unwrap();
    assert_eq!(body, r#"{"detail":{"code":"deactivated_workspace"}}"#);
    let o = e.outcomes();
    assert_eq!(o.len(), 2);
    assert!(o.iter().all(|(_, o)| matches!(
        o,
        Outcome::PaymentRequired {
            deactivated: true,
            ..
        }
    )));
}

#[tokio::test]
async fn not_found_404_short_unless_model_unsupported() {
    let e = env(300).await;
    assert_eq!(mode_post(&e, "nf404").await.status(), 200);
    let o = e.outcomes();
    assert!(matches!(
        o[0].1,
        Outcome::NotFound { retry_after: None, cooldown } if cooldown == Duration::from_secs(600)
    ));
    assert_eq!(o[1].0, "b");
    assert_eq!(mode_post(&e, "nfmodel").await.status(), 200);
    let o = e.outcomes();
    assert!(matches!(
        o[2].1,
        Outcome::ModelUnsupported { retry_after: None }
    ));
    assert_eq!(o[3].0, "b");
}

#[tokio::test]
async fn websocket_handshake_402_fails_over() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    req.headers_mut()
        .insert("x-test-mode", "deact".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(r#"{"type":"response.create","n":1}"#))
        .await
        .unwrap();
    let m = ws.next().await.unwrap().unwrap();
    assert!(m.to_text().unwrap().contains("\"echo\""));
    let o = e.outcomes();
    assert_eq!(o[0].0, "a");
    assert!(matches!(
        o[0].1,
        Outcome::PaymentRequired {
            deactivated: true,
            ..
        }
    ));
    let auths: Vec<String> = e
        .mock
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(h, _)| {
            h.iter()
                .find(|(k, _)| k == "authorization")
                .map(|(_, v)| v.clone())
        })
        .collect();
    assert_eq!(auths, ["Bearer tok-a-1", "Bearer tok-b-1"]);
}

/// Text the client sees for an encrypted-content rejection.
const ENC_HINT: &str = "Start a new session to continue.";

fn assert_no_account_outcome(e: &Env) {
    let o = e.outcomes();
    assert!(
        o.iter()
            .all(|(_, o)| matches!(o, Outcome::Success { .. } | Outcome::ClientError { .. })),
        "{o:?}"
    );
}

async fn post_mode(e: &Env, mode: &str) -> reqwest::Response {
    e.post("/backend-api/codex/responses")
        .header("x-test-mode", mode)
        .body(r#"{"model":"gpt-5.5","input":[],"stream":true}"#)
        .send()
        .await
        .unwrap()
}

/// HTTP: a 200 + SSE `error` rejection before output becomes an upstream-shaped 400 (Codex
/// shows it once instead of retrying the stream), with no retry, switch or cooldown.
#[tokio::test]
async fn encrypted_rejection_sse_becomes_400() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = post_mode(&e, "enc").await;
    assert_eq!(r.status(), 400);
    assert_eq!(r.headers()["content-type"], "application/json");
    let v: serde_json::Value = r.json().await.unwrap();
    let err = &v["error"];
    assert_eq!(err["code"], "invalid_encrypted_content");
    assert_eq!(err["type"], "invalid_request_error");
    assert_eq!(err["param"], "input");
    let msg = err["message"].as_str().unwrap();
    assert!(
        msg.contains("encrypted reasoning content") && msg.contains(ENC_HINT),
        "{msg}"
    );
    assert!(
        !msg.to_ascii_lowercase().contains("ciallopa")
            && !msg.to_ascii_lowercase().contains("proxy")
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1, "no retry / switch");
    assert_no_account_outcome(&e);
    // The account keeps serving.
    let _ = sse_text(&e, "").await;
    assert!(e.mock.seen.lock().unwrap()[1]
        .0
        .iter()
        .any(|(k, v)| k == "authorization" && v.contains("tok-a")));
    let rows = wait_rows(&e, 2).await;
    let row = &rows[1];
    assert_eq!(row["status"], 400, "{row}");
    assert_eq!(row["error"], "invalid_encrypted_content", "{row}");
    assert_eq!(row["err_source"], "upstream", "{row}");
    assert_eq!(row["switches"], 0, "{row}");
    let dv: serde_json::Value = detail_of(&e, row["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert!(
        dv["message"]
            .as_str()
            .unwrap()
            .contains("organization_id did not match"),
        "{dv}"
    );
    assert!(dv["body"].as_str().unwrap().contains("rs_0abc"), "{dv}");
}

/// HTTP: a plain upstream 400 rejection gets the same clear message.
#[tokio::test]
async fn encrypted_rejection_http_400_clear_message() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = post_mode(&e, "enc400").await;
    assert_eq!(r.status(), 400);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["error"]["code"], "invalid_encrypted_content");
    assert!(v["error"]["message"].as_str().unwrap().contains(ENC_HINT));
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_no_account_outcome(&e);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "invalid_encrypted_content");
    assert_eq!(rows[0]["err_source"], "upstream");
    // Other 400 bodies still pass through verbatim.
    let r = post_mode(&e, "errbody").await;
    assert_eq!(r.status(), 400);
    assert!(r.text().await.unwrap().contains("bad input"));
}

/// HTTP: after output the rejection ends the stream as `response.failed` with the clear message
/// (the upstream wording stays out of the client stream, but in the log detail).
#[tokio::test]
async fn encrypted_rejection_after_output_is_clear_failed_event() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let t = sse_text(&e, "lateenc").await;
    assert!(t.contains("\"delta\":\"hi\""), "{t}");
    assert!(t.contains("event: response.failed"), "{t}");
    assert!(
        t.contains("invalid_encrypted_content") && t.contains(ENC_HINT),
        "{t}"
    );
    assert!(
        !t.contains("could not be verified") && !t.contains("event: error"),
        "{t}"
    );
    assert!(t.ends_with("\n\n"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_no_account_outcome(&e);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["status"], 200);
    assert_eq!(rows[0]["error"], "invalid_encrypted_content");
    assert_eq!(rows[0]["err_source"], "upstream");
    let dv: serde_json::Value = detail_of(&e, rows[0]["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert!(
        dv["message"]
            .as_str()
            .unwrap()
            .contains("could not be verified"),
        "{dv}"
    );
}

/// WebSocket: the error frame keeps its shape and `status: 400` (fatal for Codex), with the
/// clear message; no redial, no account outcome, and the session stays usable.
#[tokio::test]
async fn encrypted_rejection_ws_clear_message() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"enc"}"#,
    ))
    .await
    .unwrap();
    let mut frames = Vec::new();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        let done = t.contains("\"type\":\"error\"");
        frames.push(t);
        if done {
            break;
        }
    }
    assert_eq!(frames.len(), 3, "{frames:?}");
    assert!(
        frames[0].contains("codex.rate_limits") && frames[1].contains("codex.response.metadata")
    );
    let v: serde_json::Value = serde_json::from_str(&frames[2]).unwrap();
    assert_eq!(v["type"], "error");
    assert_eq!(v["status"], 400);
    assert_eq!(v["error"]["code"], "invalid_encrypted_content");
    assert_eq!(v["error"]["type"], "invalid_request_error");
    assert_eq!(v["error"]["param"], "input");
    assert!(
        v["error"]["message"].as_str().unwrap().contains(ENC_HINT),
        "{v}"
    );
    // Same connection, next turn works.
    ws.send(M::text(r#"{"type":"response.create","n":2}"#))
        .await
        .unwrap();
    let t = ws
        .next()
        .await
        .unwrap()
        .unwrap()
        .to_text()
        .unwrap()
        .to_string();
    assert!(t.contains("response.completed"), "{t}");
    assert_eq!(
        e.mock.hits.load(Ordering::SeqCst),
        1,
        "one upstream connection"
    );
    assert_no_account_outcome(&e);
    let rows = wait_rows(&e, 2).await;
    let row = &rows[1];
    assert_eq!(row["transport"], "ws");
    assert_eq!(row["error"], "invalid_encrypted_content", "{row}");
    assert_eq!(row["err_source"], "upstream", "{row}");
    let dv: serde_json::Value = detail_of(&e, row["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    assert!(
        dv["message"]
            .as_str()
            .unwrap()
            .contains("organization_id did not match"),
        "{dv}"
    );
}

/// Bookkeeping events (`response.queued` / `response.metadata` / `keepalive`) before an
/// in-stream overload must not commit output: switch account and succeed (sub2api #8139).
#[tokio::test]
async fn sse_metadata_before_overload_still_switches() {
    let e = env(300).await;
    let t = sse_text(&e, "metaover").await;
    assert!(
        !t.contains("overloaded") && !t.contains("openai_chatgpt_moderation_metadata"),
        "first-account preamble+error must be discarded on switch: {t}"
    );
    assert!(
        t.contains("r-b") && t.contains("response.completed") && !t.contains("r-a"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { cool: true, .. })));
}

/// Same bookkeeping before `invalid_encrypted_content` must still become HTTP 400 (not
/// `response.failed` after a false "output started").
#[tokio::test]
async fn sse_metadata_before_encrypted_still_400() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let r = post_mode(&e, "metaenc").await;
    assert_eq!(r.status(), 400, "expected HTTP 400, not a 200 stream");
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["error"]["code"], "invalid_encrypted_content");
    assert!(
        v["error"]["message"].as_str().unwrap().contains(ENC_HINT),
        "{v}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    assert_no_account_outcome(&e);
}

/// After we commit, the same bookkeeping events are forwarded to the client unchanged.
#[tokio::test]
async fn sse_metadata_forwarded_once_output_commits() {
    let e = env(300).await;
    let t = sse_text(&e, "metaok").await;
    assert!(t.contains("response.queued"), "{t}");
    assert!(t.contains("openai_chatgpt_moderation_metadata"), "{t}");
    assert!(t.contains("\"type\":\"keepalive\""), "{t}");
    assert!(
        t.contains("\"delta\":\"hi\"") && t.contains("response.completed"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
}

/// WS: `response.metadata` / `response.queued` before overload still allow a pre-output replay.
#[tokio::test]
async fn ws_metadata_before_overload_still_switches() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"metaover"}"#,
    ))
    .await
    .unwrap();
    let mut got = Vec::new();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        assert!(!t.contains("overloaded"), "overload leaked: {t}");
        assert!(
            !t.contains("openai_chatgpt_moderation_metadata"),
            "discarded account preamble leaked: {t}"
        );
        got.push(t.clone());
        if t.contains("response.completed") {
            break;
        }
    }
    assert!(
        got.iter().any(|t| t.contains("r-b")) && got.iter().any(|t| t.contains("delta")),
        "{got:?}"
    );
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })));
}

/// WS: an overload replayed on another account is recorded as rescued with its chain.
#[tokio::test]
async fn ws_overload_switch_records_attempt_chain() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"metaover"}"#,
    ))
    .await
    .unwrap();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        if t.contains("response.completed") {
            break;
        }
    }
    drop(ws);
    let rows = wait_rows(&e, 1).await;
    let row = &rows[0];
    assert!(row["error"].is_null(), "{row}");
    assert_eq!(row["rescued"], true, "{row}");
    assert_eq!(row["upstream_fault"], true, "{row}");
    let v: serde_json::Value = detail_of(&e, row["request_id"].as_str().unwrap(), true)
        .await
        .json()
        .await
        .unwrap();
    let at = v["attempts"].as_array().unwrap();
    assert_eq!(at.len(), 2, "{v}");
    assert_eq!(at[0]["account"], "a");
    assert_eq!(at[0]["code"], "server_is_overloaded");
    assert_eq!(at[0]["action"], "switch");
    assert_eq!(at[1]["account"], row["account"]);
    assert_eq!(at[1]["action"], "ok");
}

/// WS: metadata then encrypted rejection keeps the clear error frame (status 400).
#[tokio::test]
async fn ws_metadata_before_encrypted_clear_message() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"metaenc"}"#,
    ))
    .await
    .unwrap();
    let mut frames = Vec::new();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        let done = t.contains("\"type\":\"error\"");
        frames.push(t);
        if done {
            break;
        }
    }
    assert!(
        frames.iter().any(|t| t.contains("response.metadata")),
        "committed metadata should reach the client: {frames:?}"
    );
    let err = frames
        .iter()
        .find(|t| t.contains("\"type\":\"error\""))
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(err).unwrap();
    assert_eq!(v["status"], 400);
    assert_eq!(v["error"]["code"], "invalid_encrypted_content");
    assert!(
        v["error"]["message"].as_str().unwrap().contains(ENC_HINT),
        "{v}"
    );
    assert_no_account_outcome(&e);
}

// Regression (CF 1010): a Cloudflare block page is an egress block, not an auth failure. Before:
// both accounts AuthFailed (30 min cooldown each), the raw page went to the client.
#[tokio::test]
async fn cloudflare_block_is_egress_not_auth() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for (i, mode) in [
        "repro_cf1010_text",
        "repro_cf1010_json",
        "repro_cf_challenge",
    ]
    .into_iter()
    .enumerate()
    {
        let r = mode_post(&e, mode).await;
        assert_eq!(r.status(), 502, "{mode}");
        let body = r.text().await.unwrap();
        let low = body.to_ascii_lowercase();
        assert!(
            !low.contains("cloudflare") && !low.contains("1010") && !low.contains("ciallopa"),
            "{mode}: {body}"
        );
        assert!(body.contains("Bad gateway"), "{mode}: {body}");
        // One upstream hit: the egress is shared, so no switch to b.
        assert_eq!(e.mock.hits.load(Ordering::SeqCst), i + 1, "{mode}");
        let o = e.outcomes();
        assert_eq!(o.len(), i + 1, "{mode}");
        assert!(matches!(o[i].1, Outcome::EgressBlocked), "{mode}: {o:?}");
    }
    assert!(!e
        .outcomes()
        .iter()
        .any(|(_, o)| matches!(o, Outcome::AuthFailed)));
    let rows = wait_rows(&e, 3).await;
    let codes: Vec<&str> = rows.iter().filter_map(|r| r["error"].as_str()).collect();
    assert!(
        codes.contains(&"cf_blocked") && codes.contains(&"challenge"),
        "{rows:?}"
    );
    for r in &rows {
        assert_eq!(r["err_source"], "network", "{r}");
        assert_eq!(r["status"], 502, "{r}");
    }
    let q: serde_json::Value = e
        .http
        .get(format!("http://{}/usage/v1/quota", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let b = &q["egress_block"];
    assert_eq!(b["active"], true, "{q}");
    assert_eq!(b["count"], 3, "{q}");
    assert_eq!(b["code"], "challenge", "{q}");
    assert_eq!(b["cf_ray"], "9a1b2c3d4e5f6789-SJC", "{q}");
    // A normal answer ends the episode on the overview.
    assert_eq!(mode_post(&e, "").await.status(), 200);
    let q: serde_json::Value = e
        .http
        .get(format!("http://{}/usage/v1/quota", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(q["egress_block"]["active"], false, "{q}");
    rec.shutdown();
}

#[tokio::test]
async fn real_403_still_auth_failed_and_switches() {
    let e = env(300).await;
    let r = mode_post(&e, "real403").await;
    assert_eq!(r.status(), 200);
    let o = e.outcomes();
    assert_eq!(o[0].0, "a");
    assert!(matches!(o[0].1, Outcome::AuthFailed), "{o:?}");
    assert_eq!(o[1].0, "b");
    assert!(matches!(o[1].1, Outcome::Success { .. }));
}

#[tokio::test]
async fn websocket_handshake_cloudflare_block_is_egress() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    req.headers_mut()
        .insert("x-test-mode", "repro_cf1010_text".parse().unwrap());
    let err = tokio_tungstenite::connect_async(req).await.unwrap_err();
    match err {
        tokio_tungstenite::tungstenite::Error::Http(r) => {
            assert_eq!(r.status(), 502);
            let b = String::from_utf8_lossy(r.body().as_deref().unwrap_or_default())
                .to_ascii_lowercase();
            assert!(!b.contains("1010") && !b.contains("cloudflare"), "{b}");
        }
        other => panic!("{other:?}"),
    }
    let o = e.outcomes();
    assert_eq!(o.len(), 1, "{o:?}");
    assert!(matches!(o[0].1, Outcome::EgressBlocked));
}

/// Regression: chatgpt announces an empty message item / text part before an overload.
/// Those announcements are not output: the attempt must switch with nothing duplicated.
#[tokio::test]
async fn sse_empty_added_before_overload_switches() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let t = sse_text(&e, "emptyadded_over").await;
    assert!(!t.contains("overloaded"), "{t}");
    assert!(
        !t.contains("r-a") && !t.contains("msg_a") && !t.contains("rs_a"),
        "{t}"
    );
    assert_eq!(t.matches("response.created").count(), 1, "{t}");
    assert!(t.contains("r-b") && t.contains("response.completed"), "{t}");
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 2);
    let o = e.outcomes();
    assert!(
        o.iter()
            .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { cool: true, .. })),
        "{o:?}"
    );
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{}", rows[0]);
    assert_eq!(rows[0]["account"], "b", "{}", rows[0]);
}

/// Streamed tool-call arguments are real output: the overload after them is forwarded, never
/// replayed, and the request is recorded as a failure (not a success) on the account.
#[tokio::test]
async fn sse_overload_after_tool_call_arguments_is_a_failure() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let t = sse_text(&e, "tooladded_over").await;
    assert!(
        t.contains("fc_a") && t.contains("server_is_overloaded"),
        "{t}"
    );
    assert_eq!(e.mock.hits.load(Ordering::SeqCst), 1);
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "server_is_overloaded", "{}", rows[0]);
    assert_eq!(rows[0]["err_source"], "stream", "{}", rows[0]);
    let o = e.outcomes();
    assert!(
        o.iter()
            .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })),
        "a late overload still counts against the account: {o:?}"
    );
}

/// Late overload (after real text) is recorded as a failure as well.
#[tokio::test]
async fn sse_overload_after_output_is_recorded_as_failure() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    let t = sse_text(&e, "lateover").await;
    assert!(
        t.contains("delta") && t.contains("server_is_overloaded"),
        "{t}"
    );
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "server_is_overloaded", "{}", rows[0]);
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })));
}

/// WS: the same empty announcements before an overload still allow the pre-output replay.
#[tokio::test]
async fn ws_empty_added_before_overload_switches() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let e = env(300).await;
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(
        r#"{"type":"response.create","model":"gpt-5.5","tag":"emptyadded_over"}"#,
    ))
    .await
    .unwrap();
    let mut got = Vec::new();
    loop {
        let t = ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .to_string();
        assert!(!t.contains("overloaded"), "overload leaked: {t}");
        assert!(
            !t.contains("msg_a") && !t.contains("r-a"),
            "discarded preamble leaked: {t}"
        );
        got.push(t.clone());
        if t.contains("response.completed") {
            break;
        }
    }
    assert_eq!(
        got.iter()
            .filter(|t| t.contains("response.created"))
            .count(),
        1,
        "{got:?}"
    );
    assert!(got.iter().any(|t| t.contains("r-b")), "{got:?}");
    assert!(e
        .outcomes()
        .iter()
        .any(|(a, o)| a == "a" && matches!(o, Outcome::Overloaded { .. })));
}

/// Running WS turns fill `max_inflight_requests`; new turns are then refused, but model
/// lists and quota polls are exempt.
#[tokio::test]
async fn running_turns_fill_inflight_but_models_and_quota_polls_are_exempt() {
    use tokio_tungstenite::tungstenite::Message as M;
    use tower::ServiceExt;
    let e = env(300).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 2;
    e.config.store(cfg);
    let mut idle = Vec::new();
    for _ in 0..2 {
        let (mut ws, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
        assert_eq!(r.status(), 101);
        ws.send(M::text(r#"{"type":"response.create","input":"held"}"#))
            .await
            .unwrap();
        assert!(ws_text(&mut ws)
            .await
            .contains("response.output_text.delta"));
        idle.push(ws);
    }
    assert_eq!(e.drain.inflight(), 2, "two running turns hold both slots");
    assert_eq!(
        held_response(&e).await.status(),
        503,
        "a new turn is refused"
    );
    let before = e.drain.rejected();
    for path in [
        "/v1/models",
        "/backend-api/codex/models",
        "/backend-api/wham/usage",
        "/api/codex/usage",
    ] {
        let r = e
            .app
            .clone()
            .oneshot(
                http::Request::get(path)
                    .header("authorization", format!("Bearer {KEY}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{path} is exempt from the inflight cap");
        let _ = axum::body::to_bytes(r.into_body(), 1 << 20).await;
    }
    assert_eq!(e.drain.rejected(), before, "exempt polls are never refused");
    assert_eq!(e.drain.inflight(), 2, "exempt polls take no slot");
    drop(idle);
}

#[tokio::test]
async fn refusals_are_stored_per_minute_and_served_by_the_usage_api() {
    use tower::ServiceExt;
    let d = tempfile::tempdir().unwrap();
    let rec = usage_recorder(d.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 1;
    e.config.store(cfg);
    let held = held_response(&e).await;
    assert_eq!(held.status(), 200, "the first turn holds the only slot");
    for _ in 0..3 {
        let r = e
            .app
            .clone()
            .oneshot(
                http::Request::post("/v1/responses?trace=abc")
                    .header("authorization", format!("Bearer {KEY}"))
                    .header("user-agent", "codex_cli_rs/0.99 test")
                    .body(Body::from(r#"{"stream":true,"input":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), 503);
    }
    assert_eq!(e.drain.rejected_by(), [3, 0, 0, 0]);
    let r2 = rec.clone();
    tokio::task::spawn_blocking(move || r2.flush_blocking())
        .await
        .unwrap()
        .unwrap();
    let get = |path: String| {
        let h = e.http.clone();
        async move {
            h.get(path)
                .bearer_auth(KEY)
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()
        }
    };
    let v = get(format!("http://{}/usage/v1/refusals?range=1h", e.proxy)).await;
    assert_eq!(v["totals"]["count"], 3, "{v}");
    assert_eq!(v["totals"]["inflight"], 3);
    let rows = v["rows"].as_array().unwrap();
    // One group per minute (two if the requests straddle a minute boundary).
    assert!(matches!(rows.len(), 1 | 2), "{v}");
    let n: u64 = rows.iter().map(|r| r["count"].as_u64().unwrap()).sum();
    assert_eq!(n, 3);
    let row = &rows[0];
    assert_eq!(row["reason"], "inflight");
    assert_eq!(row["transport"], "http");
    assert_eq!(row["route"], "/v1/responses", "query stripped");
    assert_eq!(row["ua"], "codex_cli_rs/0.99 test");
    assert_eq!(row["limit"], 1);
    assert_eq!(row["max_inflight"], 1);
    assert!(row["key"].is_string());
    let label = row["key"].as_str().unwrap().to_string();
    let none = get(format!(
        "http://{}/usage/v1/refusals?range=1h&reason=ws",
        e.proxy
    ))
    .await;
    assert_eq!(none["totals"]["count"], 0);
    let other = get(format!(
        "http://{}/usage/v1/refusals?range=1h&key=nobody",
        e.proxy
    ))
    .await;
    assert_eq!(other["rows"].as_array().unwrap().len(), 0);
    let s = get(format!(
        "http://{}/usage/v1/summary?range=1h&key={label}",
        e.proxy
    ))
    .await;
    assert_eq!(s["refusals"]["count"], 3, "{s}");
    let s = get(format!(
        "http://{}/usage/v1/summary?range=1h&account=x",
        e.proxy
    ))
    .await;
    assert!(
        s["refusals"].is_null(),
        "no refusal count under an account filter"
    );
    // The request list's chip counts carry the same refusals (本地拒绝) and their reasons.
    let c = get(format!(
        "http://{}/usage/v1/records/counts?range=1h&key={label}",
        e.proxy
    ))
    .await;
    assert_eq!(c["refused"], 3, "{c}");
    assert_eq!(c["causes"]["refused"]["inflight"], 3, "{c}");
    assert_eq!(c["causes"]["refused"]["ws"], 0, "{c}");
    let c = get(format!(
        "http://{}/usage/v1/records/counts?range=1h&account=x",
        e.proxy
    ))
    .await;
    assert!(c["refused"].is_null(), "{c}");
    assert!(c["causes"].get("refused").is_none(), "{c}");
    drop(held);
}

/// Next text frame, skipping pings and pongs (bounded).
async fn ws_text<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::Message as M;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                M::Text(t) => return t.as_str().to_string(),
                M::Ping(_) | M::Pong(_) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await
    .unwrap()
}

async fn wait_inflight(e: &Env, n: usize) {
    for _ in 0..200 {
        if e.drain.inflight() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("inflight stayed {} (want {n})", e.drain.inflight());
}

/// `max_inflight_requests` counts running work only: an idle WebSocket holds no slot (it is
/// bounded by `ws_max_connections`), a WS turn holds one from its client request to its
/// terminal event, and a turn that finds the cap full gets the `gateway_busy` frame and is
/// recorded as an inflight refusal over ws.
#[tokio::test]
async fn idle_websockets_hold_no_inflight_slot_only_running_turns_do() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let rec = usage_recorder(d.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 1;
    e.config.store(cfg);
    let mut idle = Vec::new();
    for _ in 0..2 {
        let (mut ws, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
        assert_eq!(r.status(), 101);
        ws.send(M::Ping(Bytes::from_static(b"ready")))
            .await
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
        idle.push(ws);
    }
    wait_inflight(&e, 0).await;
    assert_eq!(e.drain.ws_connections(), 2);
    assert_eq!(
        e.drain.active(),
        2,
        "idle sessions are still waited for on shutdown"
    );
    let held = held_response(&e).await;
    assert_eq!(held.status(), 200, "idle sockets leave the slot free");
    assert_eq!(e.drain.inflight(), 1);

    // The cap is full: a WS turn is refused with the busy frame, the socket stays open.
    let ws = &mut idle[0];
    ws.send(M::text(r#"{"type":"response.create","input":"plain"}"#))
        .await
        .unwrap();
    let busy = ws_text(ws).await;
    assert!(busy.contains("gateway_busy"), "{busy}");
    assert_eq!(e.drain.rejected_by(), [1, 0, 0, 0]);
    drop(held);
    wait_inflight(&e, 0).await;

    // A completed turn releases its slot at the terminal event.
    ws.send(M::text(r#"{"type":"response.create","input":"plain"}"#))
        .await
        .unwrap();
    assert!(ws_text(ws).await.contains("response.completed"));
    wait_inflight(&e, 0).await;

    // A running turn holds one slot and keeps HTTP out.
    ws.send(M::text(r#"{"type":"response.create","input":"held"}"#))
        .await
        .unwrap();
    assert!(ws_text(ws).await.contains("response.output_text.delta"));
    assert_eq!(e.drain.inflight(), 1, "a running WS turn holds a slot");
    assert_eq!(held_response(&e).await.status(), 503);
    // Closing mid-turn releases it.
    let mut ws = idle.remove(0);
    ws.send(M::Close(None)).await.unwrap();
    drop(ws);
    wait_inflight(&e, 0).await;
    assert_eq!(held_response(&e).await.status(), 200);
    drop(idle);
    assert_eq!(e.drain.wait_idle(Duration::from_secs(3)).await, 0);

    let r2 = rec.clone();
    tokio::task::spawn_blocking(move || r2.flush_blocking())
        .await
        .unwrap()
        .unwrap();
    let v = e
        .http
        .get(format!("http://{}/usage/v1/refusals?range=1h", e.proxy))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["totals"]["inflight"], 2, "{v}");
    let rows = v["rows"].as_array().unwrap();
    let by = |t: &str| -> u64 {
        rows.iter()
            .filter(|r| r["reason"] == "inflight" && r["transport"] == t)
            .map(|r| r["count"].as_u64().unwrap())
            .sum()
    };
    assert_eq!((by("ws"), by("http")), (1, 1), "{v}");
}

/// The sampler stores current occupancy, exact peaks between samples and the limits in
/// effect; `/usage/v1/occupancy` serves them per minute and instance.
#[tokio::test]
async fn occupancy_samples_are_stored_and_served_by_the_usage_api() {
    use tokio_tungstenite::tungstenite::Message as M;
    let d = tempfile::tempdir().unwrap();
    let rec = usage_recorder(d.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let mut cfg = (*e.config.load()).clone();
    cfg.max_inflight_requests = 4;
    cfg.ws_max_connections = 3;
    e.config.store(cfg);
    let (mut ws, r) = tokio_tungstenite::connect_async(ws_req(&e)).await.unwrap();
    assert_eq!(r.status(), 101);
    ws.send(M::Ping(Bytes::from_static(b"ready")))
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
    // A burst between samples: two turns start and one ends before the sample.
    let a = held_response(&e).await;
    let b = held_response(&e).await;
    assert_eq!((a.status().as_u16(), b.status().as_u16()), (200, 200));
    drop(b);
    wait_inflight(&e, 1).await;
    let s = ciallopa_forward::occupancy::sample(&e.state).unwrap();
    assert_eq!(
        (s.inflight, s.peak_inflight),
        (1, 2),
        "the burst peak is kept"
    );
    assert_eq!((s.ws, s.peak_ws), (1, 1));
    assert_eq!((s.limit_inflight, s.limit_ws), (4, 3));
    rec.occupancy(s);
    ciallopa_forward::occupancy::record(&e.state);
    drop(a);
    drop(ws);
    let r2 = rec.clone();
    tokio::task::spawn_blocking(move || r2.flush_blocking())
        .await
        .unwrap()
        .unwrap();
    let get = |q: &str| {
        e.http
            .get(format!("http://{}/usage/v1/occupancy?{q}", e.proxy))
            .bearer_auth(KEY)
            .send()
    };
    let v = get("range=1h")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["step_min"], 1, "{v}");
    let rows = v["rows"].as_array().unwrap();
    // One minute (two if the samples straddle a boundary).
    assert!(matches!(rows.len(), 1 | 2), "{v}");
    let n: u64 = rows.iter().map(|r| r["samples"].as_u64().unwrap()).sum();
    assert_eq!(n, 2);
    let peak = rows
        .iter()
        .map(|r| r["inflight_peak"].as_u64().unwrap())
        .max();
    assert_eq!(peak, Some(2), "{v}");
    let row = &rows[0];
    assert_eq!(row["ws_peak"], 1);
    assert_eq!(row["max_inflight_requests"], 4);
    assert_eq!(row["ws_max_connections"], 3);
    assert!(row["instance"].is_string(), "{v}");
    let v = get("range=1h&instance=nowhere")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["rows"].as_array().unwrap().len(), 0);
    let v = get("range=7d&step=1m")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["step_min"], 7, "a week is at most 1440 buckets");
    assert_eq!(get("step=90s").await.unwrap().status(), 400);
    assert_eq!(get("range=bogus").await.unwrap().status(), 400);
}

/// 429 quota errors count as failures, and the summary tells their reason apart from other
/// upstream, network/stream and local failures (`failure_classes`), with `group=error` /
/// `group=err_source` for the codes behind them. Cancellations are not failures.
#[tokio::test]
async fn summary_breaks_failures_down_by_class_error_code_and_source() {
    let d = tempfile::tempdir().unwrap();
    let rec = usage_recorder(d.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let now = ciallopa_usage::now_ms() - 1_000;
    let row = |status: u16, error: Option<&str>, src: Option<&'static str>, up: Option<u16>| {
        let mut r = ciallopa_usage::Record::new(
            rec.ids.next_at(now),
            ciallopa_usage::Transport::Sse,
            "responses",
        );
        r.account = Some("a".into());
        r.model = Some("m".into());
        r.status = status;
        r.error = error.map(Into::into);
        r.err_source = src;
        r.upstream_status = up;
        assert!(rec.record(r));
    };
    // One row in the previous hour (ids are time-ordered, so it goes first).
    let mut old = ciallopa_usage::Record::new(
        rec.ids.next_at(now - 5_400_000),
        ciallopa_usage::Transport::Sse,
        "responses",
    );
    old.status = 500;
    old.error = Some("server_error".into());
    old.err_source = Some("upstream");
    old.upstream_status = Some(500);
    assert!(rec.record(old));
    row(200, None, None, None);
    row(
        429,
        Some("usage_limit_reached"),
        Some("upstream"),
        Some(429),
    );
    row(200, Some("usage_limit"), Some("stream"), None); // mid-stream quota
    row(429, Some("all_quota_exhausted"), Some("local"), None);
    row(
        429,
        Some("rate_limit_exceeded"),
        Some("upstream"),
        Some(429),
    );
    row(500, Some("server_error"), Some("upstream"), Some(500));
    row(502, Some("connect_timeout"), Some("network"), None);
    row(200, Some("stream_truncated"), Some("stream"), None);
    row(503, Some("all_cooling"), Some("local"), None);
    row(499, Some("client_closed"), Some("client"), None);
    let r2 = rec.clone();
    tokio::task::spawn_blocking(move || r2.flush_blocking())
        .await
        .unwrap()
        .unwrap();
    let get = |q: &str| {
        e.http
            .get(format!("http://{}/usage/v1/summary?range=1h&{q}", e.proxy))
            .bearer_auth(KEY)
            .send()
    };
    let r = get("group=error,err_source").await.unwrap();
    assert_eq!(r.status(), 200, "error / err_source are summary groups");
    let v = r.json::<serde_json::Value>().await.unwrap();
    let t = &v["summary"]["total"];
    assert_eq!(
        (
            t["count"].as_u64(),
            t["failures"].as_u64(),
            t["cancelled"].as_u64()
        ),
        (Some(10), Some(8), Some(1)),
        "{t}"
    );
    assert_eq!(
        t["failure_classes"],
        serde_json::json!({"quota": 3, "rate_limit": 1, "upstream": 1, "network": 1, "stream": 1, "local": 1}),
        "{t}"
    );
    let groups = v["summary"]["groups"].as_array().unwrap();
    let g = groups
        .iter()
        .find(|g| g["group"]["error"] == "all_quota_exhausted")
        .unwrap();
    assert_eq!(g["group"]["err_source"], "local");
    assert_eq!(g["failure_classes"], serde_json::json!({"quota": 1}));
    let ok = groups
        .iter()
        .find(|g| g["group"]["error"].is_null())
        .unwrap();
    assert_eq!(
        (ok["count"].as_u64(), ok["failures"].as_u64()),
        (Some(1), Some(0))
    );
    let v = get("group=err_source")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let up = v["summary"]["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["group"]["err_source"] == "upstream")
        .unwrap()
        .clone();
    assert_eq!(up["failures"], 3, "{up}");
    assert_eq!(
        up["failure_classes"],
        serde_json::json!({"quota": 1, "rate_limit": 1, "upstream": 1})
    );
    // Upstream failure rate: upstream-sourced failures (incl. upstream 429s) / requests.
    assert_eq!(t["upstream_failures"], 3, "{t}");
    assert_eq!(t["upstream_failure_rate"].as_f64(), Some(0.3));

    // Previous period: same length right before `from`, total only.
    let v = get("compare=1&bucket=hour&tz=0")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let p = &v["previous"];
    assert_eq!(p["total"]["count"], 1, "{p}");
    assert_eq!(p["total"]["upstream_failures"], 1);
    assert_eq!(
        p["to"].as_i64().unwrap() - p["from"].as_i64().unwrap(),
        3_600_000
    );
    assert!(p["buckets"].is_array(), "{p}");
    assert!(v["summary"]["buckets"][0]["duration_p90_ms"].is_number());
    let v = get("")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(v["previous"].is_null(), "compare is opt-in");

    // Records: failure_class filter and per-row class.
    let rec_get = |q: &str| {
        e.http
            .get(format!("http://{}/usage/v1/records?range=1h&{q}", e.proxy))
            .bearer_auth(KEY)
            .send()
    };
    let v = rec_get("failure_class=quota")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["total"], 3, "{v}");
    let rows = v["records"].as_array().unwrap();
    assert!(rows.iter().all(|r| r["failure_class"] == "quota"));
    let r = rec_get("failure_class=bogus").await.unwrap();
    assert_eq!(r.status(), 400);
    let v = rec_get("failure_class=network&outcome=completed")
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(v["total"], 0, "a conflicting outcome matches nothing");
}

async fn read_all(e: &Env, mode: &str) {
    let mut r = e
        .post("/v1/responses")
        .header("x-test-mode", mode)
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    while let Ok(Some(_)) = r.chunk().await {}
}

#[tokio::test]
async fn read_error_after_completed_is_a_normal_end() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    read_all(&e, "completed_then_reset").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{}", rows[0]);
    assert!(rows[0]["err_source"].is_null(), "{}", rows[0]);
    assert_eq!(rows[0]["output_tokens"], 1);
    assert!(
        e.outcomes()
            .iter()
            .all(|(_, o)| matches!(o, Outcome::Success { .. })),
        "{:?}",
        e.outcomes()
    );
}

#[tokio::test]
async fn idle_after_completed_is_a_normal_end() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(1, Some(usage_recorder(d.path()))).await;
    read_all(&e, "completed_then_idle").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{}", rows[0]);
    assert!(rows[0]["err_source"].is_null(), "{}", rows[0]);
    assert!(
        e.outcomes()
            .iter()
            .all(|(_, o)| matches!(o, Outcome::Success { .. })),
        "{:?}",
        e.outcomes()
    );
}

#[tokio::test]
async fn image_response_incomplete_is_recorded_not_truncated() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // An incomplete response that still carries an image returns it.
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image_incomplete")
        .json(&serde_json::json!({"prompt":"cat"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["data"][0]["b64_json"], "SU1H");
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{}", rows[0]);
    assert_eq!(rows[0]["incomplete"], "max_output_tokens", "{}", rows[0]);
    // Filtered without an image: the client gets an error, logged as content_filter.
    for stream in [false, true] {
        let r = e
            .post("/v1/images/generations")
            .header("x-test-mode", "image_content_filter")
            .json(&serde_json::json!({"prompt":"cat","stream":stream}))
            .send()
            .await
            .unwrap();
        if !stream {
            assert_eq!(r.status(), 502);
        }
        r.bytes().await.unwrap();
    }
    let rows = wait_rows(&e, 3).await;
    for row in &rows[..2] {
        assert_eq!(row["error"], "content_filter", "{row}");
        assert_eq!(row["err_source"], "stream", "{row}");
        assert_eq!(row["incomplete"], "content_filter", "{row}");
    }
}

async fn ws_turn(e: &Env, tag: &str) {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(M::text(format!(
        r#"{{"type":"response.create","model":"gpt-5.5","tag":"{tag}"}}"#
    )))
    .await
    .unwrap();
    while let Ok(Some(Ok(m))) = tokio::time::timeout(Duration::from_secs(2), ws.next()).await {
        if m.is_close() || m.to_text().is_ok_and(|t| t.contains("\"error\"")) {
            break;
        }
    }
    let _ = ws.close(None).await;
}

fn no_account_penalty(e: &Env) {
    assert!(
        e.outcomes().iter().all(|(_, o)| matches!(
            o,
            Outcome::Success { .. } | Outcome::Transient { status: None }
        )),
        "{:?}",
        e.outcomes()
    );
}

#[tokio::test]
async fn stream_request_error_records_its_code_without_account_penalty() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    read_all(&e, "late_request_error").await;
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "context_length_exceeded", "{}", rows[0]);
    assert_eq!(rows[0]["err_source"], "stream", "{}", rows[0]);
    assert_eq!(rows[0]["cause"], "request", "{}", rows[0]);
    assert_eq!(rows[0]["upstream_fault"], false, "{}", rows[0]);
    no_account_penalty(&e);
}

#[tokio::test]
async fn stream_rate_limit_without_quota_code_is_not_usage_limit() {
    for (mode, code) in [
        ("late_rate_limit", "rate_limit_exceeded"),
        ("late_429_nocode", "rate_limit_exceeded"),
        ("late_failed_rate_limit", "rate_limit_exceeded"),
    ] {
        let d = tempfile::tempdir().unwrap();
        let e = env_usage(300, Some(usage_recorder(d.path()))).await;
        read_all(&e, mode).await;
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], code, "{mode}: {}", rows[0]);
        assert_eq!(
            rows[0]["failure_class"], "rate_limit",
            "{mode}: {}",
            rows[0]
        );
        assert!(
            e.outcomes().iter().all(|(_, o)| !matches!(
                o,
                Outcome::RateLimited {
                    usage_limit: true,
                    ..
                }
            )),
            "{mode}: {:?}",
            e.outcomes()
        );
        // The model is cooled (rate limit), not left untouched.
        assert!(
            e.outcomes().iter().any(|(_, o)| matches!(
                o,
                Outcome::RateLimited {
                    usage_limit: false,
                    ..
                }
            )),
            "{mode}: {:?}",
            e.outcomes()
        );
    }
}

#[tokio::test]
async fn ws_rate_limit_and_lifecycle_errors_keep_their_codes() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    ws_turn(&e, "late_ratelimit_ws").await;
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "rate_limit_exceeded", "{}", rows[0]);
    assert!(
        e.outcomes().iter().all(|(_, o)| !matches!(
            o,
            Outcome::RateLimited {
                usage_limit: true,
                ..
            }
        )),
        "{:?}",
        e.outcomes()
    );
    ws_turn(&e, "lifecycle_ws").await;
    let rows = wait_rows(&e, 2).await;
    assert_eq!(
        rows[0]["error"], "websocket_connection_limit_reached",
        "{}",
        rows[0]
    );
    assert_eq!(rows[0]["outcome"], "cancelled", "{}", rows[0]);
}

#[tokio::test]
async fn stream_read_error_is_network_not_truncated() {
    for mode in ["delta_then_reset", "reset_first"] {
        let d = tempfile::tempdir().unwrap();
        let e = env_usage(300, Some(usage_recorder(d.path()))).await;
        read_all(&e, mode).await;
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], "stream_read_error", "{mode}: {}", rows[0]);
        assert_eq!(rows[0]["err_source"], "network", "{mode}: {}", rows[0]);
        assert_eq!(rows[0]["failure_class"], "network", "{mode}: {}", rows[0]);
    }
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    read_all(&e, "event_literal_truncated").await;
    let rows = wait_rows(&e, 1).await;
    assert_eq!(rows[0]["error"], "stream_truncated", "{}", rows[0]);
    assert_eq!(rows[0]["err_source"], "stream", "{}", rows[0]);
}

#[tokio::test]
async fn image_non_stream_truncation_is_bad_gateway() {
    for (mode, code, source) in [
        ("image_truncated", "stream_truncated", "stream"),
        ("image_reset", "stream_read_error", "network"),
    ] {
        let d = tempfile::tempdir().unwrap();
        let e = env_usage(300, Some(usage_recorder(d.path()))).await;
        let r = e
            .post("/v1/images/generations")
            .header("x-test-mode", mode)
            .json(&serde_json::json!({"model":"gpt-image-1","prompt":"cat"}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), 502, "{mode}");
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], code, "{mode}: {}", rows[0]);
        assert_eq!(rows[0]["err_source"], source, "{mode}: {}", rows[0]);
    }
}
