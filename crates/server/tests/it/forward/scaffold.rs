use super::*;

pub(super) const KEY: &str = "sk-cpa-0123456789abcdef";

/// Capture structured diagnostic fields without global subscriber state or new test deps.
#[derive(Clone, Default)]
pub(super) struct StreamWarnings(pub(super) Arc<Mutex<Vec<serde_json::Value>>>);

impl StreamWarnings {
    pub(super) fn capture(&self) -> (tracing::Dispatch, tracing::subscriber::DefaultGuard) {
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

pub(super) type Seen = (Vec<(String, String)>, Bytes);

#[derive(Clone, Default)]
pub(super) struct Mock {
    pub(super) hits: Arc<AtomicUsize>,
    pub(super) seen: Arc<Mutex<Vec<Seen>>>,
    /// (authorization, client peer port) per `/responses` hit: one port = one connection.
    pub(super) peers: Arc<Mutex<Vec<(String, u16)>>>,
    /// Synchronize fault fixtures with their callers without waiting for arbitrary sleeps.
    pub(super) ready: Arc<tokio::sync::Notify>,
    pub(super) release: Arc<tokio::sync::Notify>,
}
impl Mock {
    pub(super) fn record(&self, h: &HeaderMap, b: &Bytes) -> usize {
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

/// Upstream 400 body and WS error frame (with its own `headers`) used by the request-id tests.
pub(super) const REQID_BODY: &str =
    r#"{"error":{"type":"invalid_request_error","code":"bad_input","message":"nope"}}"#;
pub(super) const WS_ERROR_FRAME: &str = r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"bad_input","message":"nope"},"headers":{"retry-after":"7","x-request-id":"up-ws-req"}}"#;

pub(super) fn sse_resp(body: &'static str) -> Response {
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("x-codex-primary-used-percent", "12")
        .body(Body::from(body))
        .unwrap()
}

pub(super) fn gzip_bytes(body: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(body).unwrap();
    encoder.finish().unwrap()
}

/// Controlled response encoding, independent of the request's negotiation. Existing mocks
/// stay identity unless a test explicitly opts in, including tests of a misbehaving upstream.
pub(super) async fn gzip_fixture(response: Response, mode: &str) -> Response {
    let (mut parts, body) = response.into_parts();
    parts.headers.remove("content-length");
    parts
        .headers
        .insert("content-encoding", "gzip".parse().unwrap());
    let plain = axum::body::to_bytes(body, 16 * 1024 * 1024).await.unwrap();
    let mut encoded = gzip_bytes(&plain);
    if mode == "bad_crc" {
        let crc = encoded.len() - 8;
        encoded[crc] ^= 1;
    }
    parts
        .headers
        .insert("content-length", encoded.len().to_string().parse().unwrap());
    let body = Body::from(encoded);
    Response::from_parts(parts, body)
}

/// `x-test-noct: 1` mimics chatgpt.com, which sends `/responses` SSE without `Content-Type`.
pub(super) async fn up_responses(
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
    let gzip = h
        .get("x-test-gzip")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let encoding = h.get("x-test-response-encoding").cloned();
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
    if let Some(mode) = gzip {
        r = gzip_fixture(r, &mode).await;
    }
    if let Some(encoding) = encoding {
        r.headers_mut().insert("content-encoding", encoding);
    }
    r
}

pub(super) async fn up_responses_inner(State(m): State<Mock>, h: HeaderMap, b: Bytes) -> Response {
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
        "retry" if n < 1 => return Response::builder().status(503).body(Body::from("busy")).unwrap(),
        "gzip_flush_held" | "gzip_late_truncated" | "gzip_terminal_bad_crc" => {
            let terminal = mode == "gzip_flush_held";
            let terminal_before_trailer = mode.starts_with("gzip_terminal_");
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.flush().unwrap();
            let header = Bytes::from(std::mem::take(encoder.get_mut()));
            encoder.write_all("data: {\"type\":\"response.created\"}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"你好\"}\n\n".as_bytes()).unwrap();
            if terminal_before_trailer {
                encoder.write_all(b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":2}}}\n\n").unwrap();
            }
            encoder.flush().unwrap();
            let output = Bytes::from(std::mem::take(encoder.get_mut()));
            let ready = m.ready.clone();
            let release = m.release.clone();
            let chunks = futures::stream::iter([Ok::<_, std::io::Error>(header), Ok(output)])
                .chain(futures::stream::once(async move {
                    ready.notify_one();
                    release.notified().await;
                    if terminal {
                        encoder.write_all(b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":2}}}\n\n").unwrap();
                    }
                    let mut tail = encoder.finish().unwrap();
                    if mode == "gzip_terminal_bad_crc" {
                        let crc = tail.len() - 8;
                        tail[crc] ^= 1;
                    } else if !terminal {
                        tail.truncate(tail.len() - 8);
                    }
                    Ok(Bytes::from(tail))
                }));
            return Response::builder().header("content-type", "text/event-stream")
                .header("content-encoding", "gzip")
                .header("etag", "\"compressed-entity\"")
                .header("content-md5", "fixture-compressed-digest")
                .body(Body::from_stream(chunks)).unwrap();
        }
        "gzip_json" => return Response::builder().header("content-type", "application/json")
            .body(Body::from(r#"{"id":"plain-json","status":"completed"}"#)).unwrap(),
        "gzip_bootstrap_only" => return sse_resp("data: {\"type\":\"response.created\"}\n\n"),
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
                            tokio::time::sleep(millis(120)).await;
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
        "image_many_items" => {
            let item = r#"data: {"type":"response.output_item.done","item":{"type":"image_generation_call","result":"YQ=="}}"#;
            let body = format!("{item}\n\n").repeat(17) + "data: {\"type\":\"response.completed\"}\n\n";
            return Response::builder().header("content-type", "text/event-stream").body(Body::from(body)).unwrap();
        }
        "image_large_output" => {
            // Both upstream events fit individually. Joining the image with terminal usage exceeds O.
            let image = "A".repeat(11 << 20);
            let usage = "u".repeat(10 << 20);
            let item = format!(r#"data: {{"type":"response.output_item.done","item":{{"type":"image_generation_call","result":"{image}"}}}}"#);
            let terminal = format!(r#"data: {{"type":"response.completed","response":{{"tool_usage":{{"image_gen":{{"detail":"{usage}"}}}}}}}}"#);
            return Response::builder().header("content-type", "text/event-stream")
                .body(Body::from(format!("{item}\n\n{terminal}\n\n"))).unwrap();
        }
        "image_truncated" => return sse_resp("data: {\"type\":\"response.created\"}\n\n"),
        "image_reset" => {
            let s = futures::stream::once(async { Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.created\"}\n\n")) })
                .chain(futures::stream::once(async {
                    tokio::time::sleep(millis(50)).await;
                    Err(std::io::Error::other("connection reset"))
                }));
            return Response::builder().header("content-type", "text/event-stream").body(Body::from_stream(s)).unwrap();
        }
        "event_literal_truncated" => return sse_resp("data: {\"type\":\"response.output_text.delta\",\"delta\":\"response.completed\"}\n\n"),
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
        "burst_held" => {
            let stream = futures::stream::iter((1..=3).map(|i| {
                Ok::<_, std::io::Error>(Bytes::from(format!(
                    "data: {{\"type\":\"response.output_text.delta\",\"delta\":\"d{i}\"}}\n\n"
                )))
            }))
            .chain(futures::stream::pending());
            return Response::builder().header("content-type", "text/event-stream")
                .body(Body::from_stream(stream)).unwrap();
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
                tokio::time::sleep(millis(1500)).await;
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
        "gzip_retry_reload" if auth.contains("tok-a") => {
            m.ready.notify_one();
            m.release.notified().await;
            return Response::builder().status(503).body(Body::from("retry on another account")).unwrap();
        }
        "gzip_retry_reload" => return sse_resp(OK_B),
        "preparation_shared" if auth.contains("tok-a") => {
            // A slow response header consumes most of the shared two-second budget.
            tokio::time::sleep(millis(1500)).await;
            return sse_resp("data: {\"type\":\"error\",\"error\":{\"type\":\"usage_limit_reached\",\"resets_in_seconds\":30}}\n\n");
        }
        "preparation_shared" => {
            m.ready.notify_one();
            m.release.notified().await;
            return sse_resp(OK_B);
        }
        "bootstrap_long_quota" if auth.contains("tok-a") => {
            let delay = millis(1200);
            let stream = futures::stream::unfold(0, move |n| async move {
                let chunk = match n {
                    0 => b"data: {\"type\":\"response.created\"}\n\n".as_slice(),
                    1 => {
                        // Legitimate streaming/pre-output time is not preparation time.
                        tokio::time::sleep(delay).await;
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
        "reqid_400" => {
            return Response::builder()
                .status(400)
                .header("content-type", "application/json")
                .header("x-request-id", "up-req")
                .header("x-oai-request-id", "up-oai")
                .body(Body::from(REQID_BODY))
                .unwrap()
        }
        "enc400" => {
            return Response::builder()
                .status(400)
                .header("content-type", "application/json")
                .body(Body::from(concat!(r#"{"error":"#, enc_obj!(), "}")))
                .unwrap()
        }
        // Codex 0.166 `policy_capacity_unavailable` and the upstream's per-turn capacity limit
        // (`turn_rate_limit_exceeded`, "Codex is at capacity"), both before any output.
        "pre_turnrl" if auth.contains("tok-a") => {
            return sse_resp(concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r-a\"}}\n\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"r-a\",\"error\":{\"code\":\"turn_rate_limit_exceeded\",\"message\":\"Codex is at capacity, please try again later\"}}}\n\n"
            ))
        }
        "pre_policycap" | "pre_turnrl" => return sse_resp(OK_B),
        "http_turnrl" | "http_turnrl_all" if auth.contains("tok-a") || mode == "http_turnrl_all" => {
            return Response::builder()
                .status(429)
                .header("content-type", "application/json")
                .header("retry-after", "30")
                .body(Body::from(r#"{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}"#))
                .unwrap()
        }
        "http_turnrl" => return sse_resp(OK_B),
        "allover" => return sse_resp(OVER),
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
        _ => {}
    }
    match mode.as_str() {
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
                        tokio::time::sleep(millis(300)).await;
                        b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-s\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n"
                    }
                    2 => {
                        tokio::time::sleep(millis(300)).await;
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
        "late_turn_rate_limit" => sse_resp(concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n",
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"turn_rate_limit_exceeded\",\"message\":\"Codex is at capacity, please try again later\"}}\n\n"
        )),
        mode @ ("delta_then_reset" | "reset_first") => {
            let first = mode == "delta_then_reset";
            let s = futures::stream::unfold(0, move |i| async move {
                match i {
                    0 if first => Some((Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n")), 1)),
                    0 | 1 => {
                        tokio::time::sleep(millis(50)).await;
                        Some((Err(std::io::Error::other("connection reset")), 2))
                    }
                    _ => None,
                }
            });
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
        "idle" => {
            let s = futures::stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"type\":\"response.created\"}\n\n"))
            }).chain(futures::stream::pending());
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

pub(super) async fn up_models(State(m): State<Mock>, h: HeaderMap) -> Response {
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

pub(super) async fn up_compact(State(m): State<Mock>, h: HeaderMap, b: Bytes) -> Response {
    m.record(&h, &b);
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(r#"{"id":"compact-ok"}"#))
        .unwrap()
}

/// Upstream catalog shape (service_tiers is an array; `[]` when a model has none).
pub(super) const MODELS_BODY: &str = r#"{"models":[{"slug":"gpt-5.5","service_tiers":[{"id":"priority","name":"Fast"}]},{"slug":"gpt-6-luna","service_tiers":[]}]}"#;

pub(super) async fn up_images(State(m): State<Mock>, h: HeaderMap, b: Bytes) -> Response {
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

pub(super) const IMAGE_DIRECT_SSE: &str = "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"b64_json\":\"X\"}\n\n";

pub(super) fn image_event_fixture(mode: &str) -> String {
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

pub(super) fn empty_response(mode: &str) -> Response {
    Response::builder()
        .status(if mode == "empty_204" { 204 } else { 200 })
        .header("content-type", "application/json")
        .header("content-length", "0")
        .body(Body::empty())
        .unwrap()
}

pub(super) async fn up_ws(State(m): State<Mock>, h: HeaderMap, ws: WebSocketUpgrade) -> Response {
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
        tokio::time::sleep(millis(650)).await;
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
    let burst_sync = (m.ready.clone(), m.release.clone());
    // Upstream size limit fixtures: `ws_up_limit` reads with a 1 MiB limit (like the real
    // 16 MiB one) and answers an oversized message with Close 1009, then drops the connection
    // with the rest unread; `ws_close_1009` reads the message and answers Close 1009.
    let up_limit = mode == "ws_up_limit";
    let close_1009 = mode == "ws_close_1009";
    let ws = if up_limit {
        ws.max_message_size(1 << 20).max_frame_size(1 << 20)
    } else {
        ws
    };
    // `ws_maxmsg:<v>[|<v>...]`: the 101 carries x-codex-websocket-max-message-bytes values.
    let max_hdr: Vec<String> = mode
        .strip_prefix("ws_maxmsg:")
        .map(|v| v.split('|').map(str::to_string).collect())
        .unwrap_or_default();
    let mut r = ws.on_upgrade(move |mut s| async move {
        if up_limit || close_1009 {
            let first = s.recv().await;
            if up_limit && !matches!(first, Some(Err(_))) {
                return;
            }
            let _ = s
                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1009,
                    reason: "message too big".into(),
                })))
                .await;
            if up_limit {
                // Dropping a socket with unread input sends RST and discards unsent output:
                // give the close frame time to leave first, or the relay never sees the 1009.
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            return;
        }
        while let Some(Ok(msg)) = s.recv().await {
            if let Message::Text(t) = msg {
                if drop_replay {
                    return;
                }
                if t.as_str().contains("\"diagnostic_close\"") || t.as_str().contains("\"diagnostic_complete\"") {
                    let _ = s.send(Message::Text(r#"{"type":"private-do-not-log","value":"private-output"}"#.into())).await;
                    tokio::time::sleep(millis(120)).await;
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
                if t.as_str().contains("\"pre_turnrl_ws\"") || t.as_str().contains("\"pre_policycap_ws\"") {
                    if auth.contains("tok-a") {
                        let event = if t.as_str().contains("\"pre_turnrl_ws\"") {
                            r#"{"type":"error","error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}"#
                        } else {
                            r#"{"type":"error","error":{"code":"policy_capacity_unavailable","message":"fake capacity notice"}}"#
                        };
                        let _ = s.send(Message::Text(r#"{"type":"response.created","response":{"id":"r-a"}}"#.into())).await;
                        let _ = s.send(Message::Text(event.into())).await;
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
                if t.as_str().contains("\"stamp_ws\"") {
                    let _ = s.send(Message::Text(r#"{"type":"response.output_text.delta","delta":"ok"}"#.into())).await;
                    let _ = s.send(Message::Text(WS_ERROR_FRAME.into())).await;
                    continue;
                }
                if t.as_str().contains("\"budget_long\"") || t.as_str().contains("\"budget_next\"") {
                    if auth.contains("tok-a") || (auth.contains("tok-b") && t.as_str().contains("\"budget_next\"")) {
                        let _ = s.send(Message::Text(r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","resets_in_seconds":45}}"#.into())).await;
                    } else {
                        for _ in 0..3 {
                            tokio::time::sleep(millis(450)).await;
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
                if t.as_str().contains("\"tag\":\"terminal_code:") {
                    // A status-less error frame Codex treats as terminal; the upstream then keeps
                    // the socket open without a terminal response event (the relay must settle the
                    // turn on the error frame itself, as Codex does).
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap_or_default();
                    let code = v["tag"].as_str().unwrap_or("").trim_start_matches("terminal_code:").to_string();
                    let created = r#"{"type":"response.created","response":{"id":"r-t"}}"#;
                    let _ = s.send(Message::Text(created.into())).await;
                    let err = format!(r#"{{"type":"error","error":{{"code":"{code}","message":"fixture {code}"}}}}"#);
                    let _ = s.send(Message::Text(err.into())).await;
                    continue;
                }
                if t.as_str().contains("\"soft_error\"") {
                    // An error frame Codex ignores (no status, unknown code), then the turn goes on.
                    for f in [
                        r#"{"type":"response.created","response":{"id":"r-s"}}"#,
                        r#"{"type":"error","error":{"code":"some_notice","message":"fyi"}}"#,
                        r#"{"type":"response.output_text.delta","delta":"ok"}"#,
                        r#"{"type":"response.completed","response":{"id":"r-s","usage":{"input_tokens":5,"output_tokens":1}}}"#,
                    ] {
                        let _ = s.send(Message::Text(f.into())).await;
                    }
                    continue;
                }
                if t.as_str().contains("\"burst\"") {
                    // One delta, then (when the test releases it) 50 more written at once, then hold.
                    let _ = s.send(Message::Text(r#"{"type":"response.output_text.delta","delta":"b0"}"#.into())).await;
                    burst_sync.0.notify_one();
                    burst_sync.1.notified().await;
                    for i in 1..=50 {
                        let _ = s.send(Message::Text(format!(r#"{{"type":"response.output_text.delta","delta":"b{i}"}}"#).into())).await;
                    }
                    burst_sync.0.notify_one();
                    continue;
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
                    tokio::time::sleep(millis(300)).await;
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
    for v in &max_hdr {
        r.headers_mut()
            .append("x-codex-websocket-max-message-bytes", v.parse().unwrap());
    }
    r
}

pub(super) struct Env {
    pub(super) proxy: String,
    pub(super) mock: Mock,
    pub(super) rec: Arc<Rec>,
    pub(super) store: Arc<Store>,
    pub(super) http: reqwest::Client,
    pub(super) config: Arc<ciallopa_core::settings::SharedConfig>,
    pub(super) drain: Arc<ciallopa_upstream::drain::Drain>,
    pub(super) app: Router,
    pub(super) state: AppState,
}

pub(super) async fn env(idle: u64) -> Env {
    env_usage(idle, None).await
}

pub(super) async fn env_usage(idle: u64, usage: Option<Arc<ciallopa_usage::Recorder>>) -> Env {
    env_cfg(idle, usage, "").await
}

pub(super) async fn env_cfg(
    idle: u64,
    usage: Option<Arc<ciallopa_usage::Recorder>>,
    extra: &str,
) -> Env {
    env_cfg_accounts(idle, usage, extra, None).await
}

pub(super) async fn env_cfg_accounts(
    idle: u64,
    usage: Option<Arc<ciallopa_usage::Recorder>>,
    extra: &str,
    accounts: Option<Arc<dyn AccountStore>>,
) -> Env {
    env_inner(idle, usage, extra, accounts, None).await
}

/// Bytes ciallopa wrote to the upstream, one entry per TCP connection (handshake included).
pub(super) type Tap = Arc<Mutex<Vec<Arc<Mutex<Vec<u8>>>>>>;

/// Plain TCP relay in front of `target` that records what the proxy writes upstream.
async fn tap_to(target: String) -> (String, Tap) {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    let tap: Tap = Default::default();
    let t = tap.clone();
    tokio::spawn(async move {
        while let Ok((mut down, _)) = ln.accept().await {
            let buf = Arc::new(Mutex::new(Vec::new()));
            t.lock().unwrap().push(buf.clone());
            let target = target.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let Ok(mut up) = tokio::net::TcpStream::connect(&target).await else {
                    return;
                };
                let (mut dr, mut dw) = down.split();
                let (mut ur, mut uw) = up.split();
                let to_up = async {
                    let mut b = vec![0u8; 64 * 1024];
                    loop {
                        match dr.read(&mut b).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                buf.lock().unwrap().extend_from_slice(&b[..n]);
                                if uw.write_all(&b[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    let _ = uw.shutdown().await;
                };
                let to_down = async {
                    let _ = tokio::io::copy(&mut ur, &mut dw).await;
                    let _ = dw.shutdown().await;
                };
                tokio::join!(to_up, to_down);
            });
        }
    });
    (format!("127.0.0.1:{}", addr.port()), tap)
}

pub(super) async fn env_inner(
    idle: u64,
    usage: Option<Arc<ciallopa_usage::Recorder>>,
    extra: &str,
    accounts: Option<Arc<dyn AccountStore>>,
    tap: Option<&mut Option<Tap>>,
) -> Env {
    let mock = Mock::default();
    let up = Router::new()
        .route(
            "/backend-api/codex/responses",
            post(up_responses).get(up_ws),
        )
        .route("/backend-api/codex/models", get(up_models))
        .route("/backend-api/codex/responses/compact", post(up_compact))
        .route("/backend-api/codex/images/generations", post(up_images))
        .route("/backend-api/codex/images/edits", post(up_images))
        .route("/backend-api/wham/usage", get(up_models))
        .with_state(mock.clone());
    let mut up_addr = serve(up).await;
    if let Some(slot) = tap {
        let (addr, t) = tap_to(up_addr).await;
        up_addr = addr;
        *slot = Some(t);
    }
    let cfg = Config::parse(&format!(
        "api_keys = [\"{KEY}\"]\nupstream = \"http://{up_addr}/backend-api\"\n[upstream_opts]\nstream_idle_timeout_secs = {idle}\n{extra}"
    ))
    .unwrap();
    let rec = Arc::new(Rec::default());
    let store = Arc::new(Store::rotating(&["a", "b"]));
    let state = proxy_state(
        cfg,
        accounts.unwrap_or_else(|| store.clone()),
        rec.clone(),
        usage,
    );
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
        http: http_client(),
        config,
        drain,
        app,
    }
}

impl Env {
    pub(super) async fn warm_http(&self) {
        // Establish the account connections before measuring preparation/replay budgets.
        for a in self.state.accounts.accounts().iter() {
            let (client, _) = self.state.upstream.http_for_attempt(&a.id).unwrap();
            let response = client
                .get(self.state.upstream.url("/__test_ready"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 404);
            response.bytes().await.unwrap();
        }
    }

    pub(super) fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .post(format!("http://{}{path}", self.proxy))
            .bearer_auth(KEY)
    }
    pub(super) fn outcomes(&self) -> Vec<(String, Outcome)> {
        self.rec.0.lock().unwrap().clone()
    }
}

pub(super) async fn held_response(e: &Env) -> Response {
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

pub(super) fn sized_post(len: usize, chunked: bool) -> http::Request<Body> {
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

pub(super) async fn busy_json(r: Response) -> serde_json::Value {
    assert_eq!(r.status(), 503);
    assert_eq!(r.headers()["retry-after"], "3");
    assert_eq!(r.headers()["retry-after-ms"], "3000");
    serde_json::from_slice(&axum::body::to_bytes(r.into_body(), 4096).await.unwrap()).unwrap()
}

pub(super) fn cookies_seen(e: &Env) -> Vec<(String, Option<String>)> {
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

pub(super) async fn cookie_round(e: &Env, set: bool) {
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

pub(super) async fn sse_text(e: &Env, mode: &str) -> String {
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

pub(super) fn usage_recorder(dir: &std::path::Path) -> Arc<ciallopa_usage::Recorder> {
    ciallopa_usage::Recorder::open(&ciallopa_core::config::UsageConfig {
        path: dir.join("usage.db"),
        flush_ms: 5,
        ..Default::default()
    })
    .unwrap()
}

pub(super) async fn usage_rows(e: &Env) -> Vec<serde_json::Value> {
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

pub(super) async fn wait_rows(e: &Env, n: usize) -> Vec<serde_json::Value> {
    for _ in 0..100 {
        let rows = usage_rows(e).await;
        if rows.len() >= n {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    usage_rows(e).await
}

pub(super) async fn detail_of(e: &Env, id: &str, auth: bool) -> reqwest::Response {
    let r = e
        .http
        .get(format!("http://{}/usage/v1/records/{id}/detail", e.proxy));
    let r = if auth { r.bearer_auth(KEY) } else { r };
    r.send().await.unwrap()
}

pub(super) fn hdr<'a>(h: &'a [(String, String)], k: &str) -> Option<&'a str> {
    h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

pub(super) async fn noct_post(
    e: &Env,
    path: &str,
    mode: &str,
    body: &str,
    accept: bool,
) -> (String, String) {
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

pub(super) fn turn_states(e: &Env) -> Vec<(String, Option<String>)> {
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

pub(super) struct RefreshFail;
#[async_trait]
impl AccountStore for RefreshFail {
    fn accounts(&self) -> Arc<[Account]> {
        Arc::from([Account {
            id: "a".into(),
            email: None,
            chatgpt_account_id: Some("acc-a".into()),
            plan_type: None,
            priority: 0,
            weight: 1,
            disabled: false,
        }])
    }
    fn generation(&self) -> u64 {
        0
    }
    async fn credentials(&self, _: &AccountId) -> Result<Credentials, AuthError> {
        Err(AuthError::Refresh("timeout".into()))
    }
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.credentials(id).await
    }
    async fn manual_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.force_refresh(id).await
    }
    async fn reload(&self) -> Result<(), AuthError> {
        Ok(())
    }
}

// ---- client-leg permessage-deflate (ws_compression) ----

pub(super) fn ws_req(e: &Env) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = format!("ws://{}/v1/responses", e.proxy)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    req
}

/// Client config offering permessage-deflate (Codex: `DeflateConfig::default()`).
pub(super) fn deflate_client(
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
pub(super) async fn ws_roundtrips<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>, n: usize)
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

pub(super) fn ext(r: &http::Response<Option<Vec<u8>>>) -> Option<String> {
    r.headers()
        .get("sec-websocket-extensions")
        .map(|v| v.to_str().unwrap().to_string())
}

pub(super) async fn mode_post(e: &Env, mode: &str) -> reqwest::Response {
    e.post("/backend-api/codex/responses")
        .header("x-test-mode", mode)
        .body(r#"{"model":"gpt-x","input":[]}"#)
        .send()
        .await
        .unwrap()
}

/// Text the client sees for an encrypted-content rejection.
pub(super) const ENC_HINT: &str = "Start a new session to continue.";

pub(super) fn assert_no_account_outcome(e: &Env) {
    let o = e.outcomes();
    assert!(
        o.iter()
            .all(|(_, o)| matches!(o, Outcome::Success { .. } | Outcome::ClientError { .. })),
        "{o:?}"
    );
}

pub(super) async fn post_mode(e: &Env, mode: &str) -> reqwest::Response {
    e.post("/backend-api/codex/responses")
        .header("x-test-mode", mode)
        .body(r#"{"model":"gpt-5.5","input":[],"stream":true}"#)
        .send()
        .await
        .unwrap()
}

// Regression (CF 1010): a Cloudflare block page is an egress block, not an auth failure. Before:
// both accounts AuthFailed (30 min cooldown each), the raw page went to the client.

/// Next text frame, skipping pings and pongs (bounded).
pub(super) async fn ws_text<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> String
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

pub(super) async fn wait_inflight(e: &Env, n: usize) {
    for _ in 0..200 {
        if e.drain.inflight() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("inflight stayed {} (want {n})", e.drain.inflight());
}

pub(super) async fn read_all(e: &Env, mode: &str) {
    let mut r = e
        .post("/v1/responses")
        .header("x-test-mode", mode)
        .body(r#"{"model":"gpt-5.5","stream":true,"input":[]}"#)
        .send()
        .await
        .unwrap();
    while let Ok(Some(_)) = r.chunk().await {}
}

pub(super) async fn ws_turn(e: &Env, tag: &str) {
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

pub(super) async fn ws_frames(e: &Env, tag: &str) -> Vec<String> {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as M};
    let mut req = format!("ws://{}/backend-api/codex/responses", e.proxy)
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
    let mut got = Vec::new();
    while let Ok(Some(Ok(m))) = tokio::time::timeout(Duration::from_secs(3), ws.next()).await {
        let Ok(t) = m.to_text() else { break };
        got.push(t.to_string());
        if m.is_close() || t.contains("response.completed") || t.contains("\"error\"") {
            break;
        }
    }
    let _ = ws.close(None).await;
    got
}

pub(super) async fn obs_of(e: &Env, id: &str) -> (u16, serde_json::Value) {
    let r = e
        .http
        .get(format!(
            "http://{}/usage/v1/records/{id}/observation",
            e.proxy
        ))
        .bearer_auth(KEY)
        .send()
        .await
        .unwrap();
    let s = r.status().as_u16();
    (s, r.json().await.unwrap_or_default())
}

pub(super) fn ms(v: &serde_json::Value, k: &str) -> u64 {
    v[k].as_u64().unwrap_or_else(|| panic!("{k} missing: {v}"))
}

pub(super) fn assert_shutdown_event(text: &str) {
    let v: serde_json::Value = serde_json::from_str(text).expect("shutdown event is JSON");
    assert_eq!(v["type"], "response.failed", "{v}");
    assert_eq!(v["response"]["error"]["code"], "gateway_restarting", "{v}");
    assert_eq!(v["response"]["error"]["headers"]["retry-after"], "2", "{v}");
}
