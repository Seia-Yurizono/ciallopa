use super::fail;
use super::request::{
    bad, build_tool, multipart_edit_to_json, out_headers, parse_multipart, responses_request, Kind,
    DEFAULT_TOOL_MODEL, DIRECT_MODELS,
};
use crate::downstream::observed;
use crate::proxy::{self, json_error, Outbound};
use crate::{
    errors,
    usage::{End, Tracker},
};
use crate::{session_route, AppState};
use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// A terminal event without an image: `image_output_missing`, or the incomplete reason's code.
fn fail_no_image(trk: &mut Option<Tracker>, ty: &str, ev: &Value) {
    if ty == "response.incomplete" {
        let reason = crate::usage::incomplete_reason(ev);
        let code = crate::usage::incomplete_code(&reason);
        fail(
            trk,
            &code,
            errors::SRC_STREAM,
            format!("response.incomplete: {reason}"),
        );
        if let Some(t) = trk.as_mut() {
            t.incomplete(ev);
        }
    } else {
        fail(
            trk,
            "image_output_missing",
            errors::SRC_STREAM,
            "upstream completed without image output",
        );
    }
}

fn mime_of(fmt: &str) -> &'static str {
    match fmt.trim().to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        _ => "image/png",
    }
}

fn sse(event: &str, data: &Value) -> Bytes {
    Bytes::from(format!("event: {event}\ndata: {data}\n\n"))
}

pub(super) async fn run(
    st: AppState,
    headers: HeaderMap,
    body: Bytes,
    kind: Kind,
    trk: &mut Option<Tracker>,
) -> Response {
    if !st.config.load().images.enabled {
        return json_error(StatusCode::NOT_FOUND, "not_found", "images API disabled");
    }
    let ct = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let multipart = ct.to_ascii_lowercase().starts_with("multipart/");
    let (json_body, form) = if multipart {
        match parse_multipart(&ct, &body) {
            Ok(f) => (None, Some(f)),
            Err(e) => return bad(&format!("parse multipart form failed: {e}")),
        }
    } else {
        match serde_json::from_slice::<Value>(&body) {
            Ok(v) if v.is_object() => (Some(v), None),
            _ => return bad("invalid OpenAI image request JSON"),
        }
    };
    let get_s = |k: &str| -> Option<String> {
        match (&json_body, &form) {
            (Some(j), _) => j
                .get(k)
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            (_, Some(f)) => f.get(k).map(str::to_string),
            _ => None,
        }
    };
    let get_n = |k: &str| -> Option<i64> {
        match (&json_body, &form) {
            (Some(j), _) => j.get(k).and_then(|v| v.as_i64()),
            (_, Some(f)) => f.get(k).and_then(|s| s.parse().ok()),
            _ => None,
        }
    };
    let get_b = |k: &str| -> bool {
        match (&json_body, &form) {
            (Some(j), _) => j.get(k).and_then(|v| v.as_bool()).unwrap_or(false),
            (_, Some(f)) => f.get(k).is_some_and(|s| s.eq_ignore_ascii_case("true")),
            _ => false,
        }
    };
    let req_model = get_s("model").unwrap_or_default();
    // Multipart metadata was not JSON at the entry point. Keep the requested image model,
    // never substitute the text model used internally by the Responses wrapper.
    if let Some(t) = trk.as_mut() {
        t.meta(Some(
            &json!({"model": if req_model.is_empty() { DEFAULT_TOOL_MODEL } else { &req_model }}),
        ));
    }
    let base_model = req_model
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let stream = get_b("stream");
    let response_format = if get_s("response_format").is_some_and(|f| f.eq_ignore_ascii_case("url"))
    {
        "url"
    } else {
        "b64_json"
    };

    if DIRECT_MODELS.contains(&base_model.as_str()) {
        // (b) direct passthrough to /codex/images/*.
        let payload = match (&json_body, &form) {
            (Some(j), _) => {
                let mut j = j.clone();
                if let Some(o) = j.as_object_mut() {
                    o.insert("model".into(), json!(base_model));
                    if stream {
                        o.insert("stream".into(), json!(true));
                    } else {
                        o.remove("stream");
                    }
                }
                j
            }
            (_, Some(f)) => multipart_edit_to_json(f, &base_model, stream),
            _ => return bad("empty request"),
        };
        let path = if kind == Kind::Edit {
            "/codex/images/edits"
        } else {
            "/codex/images/generations"
        };
        let mut route = session_route(&headers, None);
        route.model = base_model.clone();
        let ob = Outbound {
            method: Method::POST,
            path: path.into(),
            headers: out_headers(&st.config.load().images, &headers, stream),
            body: Bytes::from(payload.to_string()),
            route,
            expect_sse: false,
            decode_response: false,
        };
        return match proxy::send_tracked(&st, ob, trk).await {
            Ok(c) => proxy::stream_image_body(&st, c, trk.take()),
            Err(r) => r,
        };
    }

    // (a) wrap in /codex/responses with the image_generation tool.
    let tool_model = if req_model.trim().is_empty() {
        DEFAULT_TOOL_MODEL.to_string()
    } else {
        req_model.trim().to_string()
    };
    let mut tool = build_tool(kind, &tool_model, &get_s, &get_n);
    let prompt = get_s("prompt").unwrap_or_default();
    let mut images = vec![];
    if kind == Kind::Edit {
        match (&json_body, &form) {
            (Some(j), _) => {
                if let Some(a) = j.get("images").and_then(|v| v.as_array()) {
                    images.extend(
                        a.iter()
                            .filter_map(|i| i.get("image_url").and_then(|u| u.as_str()))
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty()),
                    );
                }
                if let Some(m) = j
                    .pointer("/mask/image_url")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.trim().is_empty())
                {
                    tool.insert("input_image_mask".into(), json!({"image_url": m.trim()}));
                }
            }
            (_, Some(f)) => {
                images = f.images();
                if let Some(m) = f.mask() {
                    tool.insert("input_image_mask".into(), json!({"image_url": m}));
                }
            }
            _ => {}
        }
    }
    let main_model = st.config.load().images.main_model.clone();
    let req = responses_request(&main_model, &prompt, &images, tool);
    let mut route = session_route(&headers, None);
    route.model = main_model;
    let ob = Outbound {
        method: Method::POST,
        path: "/codex/responses".into(),
        headers: out_headers(&st.config.load().images, &headers, true),
        body: Bytes::from(req.to_string()),
        route,
        expect_sse: false,
        decode_response: false,
    };
    let c = match proxy::send_tracked(&st, ob, trk).await {
        Ok(c) if c.resp.status().is_success() => c,
        Ok(c) => return proxy::stream_image_body(&st, c, trk.take()),
        Err(r) => return r,
    };
    let events = sse_events(c.resp, c.idle);
    if stream {
        stream_images(&st, events, kind, response_format, trk.take())
    } else {
        buffered_images(events, response_format, trk).await
    }
}

enum ImageStep {
    Partial {
        index: i64,
        format: String,
        b64: String,
    },
    Done {
        results: Vec<ImageResult>,
        created: i64,
        usage: Option<Value>,
        incomplete: Option<Value>,
    },
    NoImage {
        ty: &'static str,
        event: Value,
    },
    Failed {
        code: String,
        message: Value,
    },
    ReadError(ImageReadError),
    Truncated,
}

#[derive(Default)]
struct ImageReducer {
    col: Collector,
}

impl ImageReducer {
    fn step(&mut self, event: Result<Value, ImageReadError>) -> Option<ImageStep> {
        let event = match event {
            Ok(event) => event,
            Err(error) => return Some(ImageStep::ReadError(error)),
        };
        match event.get("type").and_then(Value::as_str).unwrap_or("") {
            "response.image_generation_call.partial_image" => {
                let b64 = event
                    .get("partial_image_b64")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                (!b64.is_empty()).then(|| ImageStep::Partial {
                    index: event
                        .get("partial_image_index")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                    format: event
                        .get("output_format")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .into(),
                    b64: b64.into(),
                })
            }
            "response.output_item.done" => {
                self.col.item_done(&event);
                None
            }
            ty @ ("response.completed" | "response.incomplete") => {
                let (results, created, usage) = self.col.extract(&event);
                if results.is_empty() {
                    return Some(ImageStep::NoImage {
                        ty: if ty == "response.incomplete" {
                            "response.incomplete"
                        } else {
                            "response.completed"
                        },
                        event,
                    });
                }
                let incomplete = (ty == "response.incomplete").then_some(event);
                Some(ImageStep::Done {
                    results,
                    created,
                    usage,
                    incomplete,
                })
            }
            "response.failed" | "error" => Some(ImageStep::Failed {
                code: errors::event_code(&event),
                message: event
                    .pointer("/response/error/message")
                    .or_else(|| event.pointer("/error/message"))
                    .cloned()
                    .unwrap_or(json!("upstream failed")),
            }),
            _ => None,
        }
    }

    fn eof(&self) -> ImageStep {
        ImageStep::Truncated
    }

    async fn next<S>(&mut self, events: &mut S) -> ImageStep
    where
        S: futures::Stream<Item = Result<Value, ImageReadError>> + Unpin,
    {
        while let Some(event) = events.next().await {
            if let Some(step) = self.step(event) {
                return step;
            }
        }
        self.eof()
    }
}

impl ImageStep {
    fn record(&self, trk: &mut Option<Tracker>) {
        match self {
            Self::Done {
                incomplete: Some(event),
                ..
            } => {
                if let Some(t) = trk.as_mut() {
                    t.incomplete(event);
                }
            }
            Self::NoImage { ty, event } => fail_no_image(trk, ty, event),
            Self::Failed { code, message } => fail(
                trk,
                code,
                errors::SRC_STREAM,
                message.as_str().unwrap_or("upstream failed"),
            ),
            Self::ReadError(error) => fail(trk, error.code, error.source, &error.message),
            Self::Truncated => fail(
                trk,
                "stream_truncated",
                errors::SRC_STREAM,
                "upstream image stream ended before completion",
            ),
            _ => {}
        }
    }

    fn error_message(&self) -> Value {
        match self {
            Self::NoImage { .. } => json!("upstream did not return image output"),
            Self::Failed { message, .. } => message.clone(),
            Self::ReadError(error) => json!(error.message),
            Self::Truncated => json!("upstream image stream ended before completion"),
            _ => unreachable!("only failed image steps have an error message"),
        }
    }
}

fn stream_images(
    st: &AppState,
    events: impl futures::Stream<Item = Result<Value, ImageReadError>> + Send + 'static,
    kind: Kind,
    response_format: &'static str,
    mut trk: Option<Tracker>,
) -> Response {
    let prefix = if kind == Kind::Edit {
        "image_edit"
    } else {
        "image_generation"
    };
    if let Some(t) = trk.as_mut() {
        t.status(200);
        t.sse_response();
    }
    let body = observed(st.upstream.drain.clone(), None, move |tx| async move {
        let mut events = Box::pin(events);
        let mut reducer = ImageReducer::default();
        loop {
            let step = reducer.next(&mut events).await;
            match &step {
                ImageStep::Partial { index, format, b64 } => {
                    let name = format!("{prefix}.partial_image");
                    let mut data = json!({"type": name, "partial_image_index": index});
                    set_image(&mut data, response_format, format, b64);
                    if tx.send(Ok(sse(&name, &data))).await.is_err() {
                        if let Some(t) = trk.take() {
                            t.finish(End::ClientGone);
                        }
                        return;
                    }
                }
                ImageStep::Done { results, usage, .. } => {
                    let name = format!("{prefix}.completed");
                    for image in results {
                        let mut data = json!({"type": name});
                        if let Some(usage) = usage {
                            data["usage"] = usage.clone();
                        }
                        set_image(&mut data, response_format, &image.fmt, &image.result);
                        if tx.send(Ok(sse(&name, &data))).await.is_err() {
                            if let Some(t) = trk.take() {
                                t.finish(End::ClientGone);
                            }
                            return;
                        }
                    }
                    step.record(&mut trk);
                    if let Some(t) = trk.take() {
                        t.finish(End::Completed);
                    }
                    return;
                }
                _ => {
                    step.record(&mut trk);
                    let _ = tx
                        .send(Ok(sse(
                            "error",
                            &json!({"error": {"message": step.error_message()}}),
                        )))
                        .await;
                    if let Some(t) = trk.take() {
                        t.finish(End::Completed);
                    }
                    return;
                }
            }
        }
    });
    let mut response = Response::new(body);
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response.headers_mut().insert(
        http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache"),
    );
    response
}

async fn buffered_images(
    events: impl futures::Stream<Item = Result<Value, ImageReadError>>,
    response_format: &'static str,
    trk: &mut Option<Tracker>,
) -> Response {
    let mut events = Box::pin(events);
    let mut reducer = ImageReducer::default();
    loop {
        let step = reducer.next(&mut events).await;
        step.record(trk);
        match step {
            ImageStep::Partial { .. } => {}
            ImageStep::Done {
                results,
                created,
                usage,
                ..
            } => {
                let first = &results[0];
                let mut out = json!({"created": created, "data": []});
                for (key, value) in [
                    ("background", &first.background),
                    ("output_format", &first.fmt),
                    ("quality", &first.quality),
                    ("size", &first.size),
                ] {
                    if !value.is_empty() {
                        out[key] = json!(value);
                    }
                }
                if let Some(usage) = usage {
                    out["usage"] = usage;
                }
                out["data"] = Value::Array(
                    results
                        .iter()
                        .map(|image| {
                            let mut data = json!({});
                            if !image.revised_prompt.is_empty() {
                                data["revised_prompt"] = json!(image.revised_prompt);
                            }
                            set_image(&mut data, response_format, &image.fmt, &image.result);
                            data
                        })
                        .collect(),
                );
                let mut response = Response::new(Body::from(out.to_string()));
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                return response;
            }
            step => {
                let status = if matches!(&step, ImageStep::ReadError(error) if error.code == "idle_timeout")
                {
                    StatusCode::GATEWAY_TIMEOUT
                } else {
                    StatusCode::BAD_GATEWAY
                };
                let message = if matches!(step, ImageStep::Truncated) {
                    json!("stream error: stream disconnected before completion")
                } else {
                    step.error_message()
                };
                return json_error(
                    status,
                    "upstream_error",
                    message.as_str().unwrap_or("upstream failed"),
                );
            }
        }
    }
}

fn set_image(d: &mut Value, rf: &str, fmt: &str, b64: &str) {
    if rf == "url" {
        d["url"] = json!(format!("data:{};base64,{b64}", mime_of(fmt)));
    } else {
        d["b64_json"] = json!(b64);
    }
}

struct ImageResult {
    result: String,
    revised_prompt: String,
    fmt: String,
    size: String,
    background: String,
    quality: String,
}

#[derive(Default)]
struct Collector {
    by_index: BTreeMap<i64, Value>,
    fallback: Vec<Value>,
}

impl Collector {
    fn item_done(&mut self, ev: &Value) {
        let Some(item) = ev.get("item").filter(|i| i.is_object()) else {
            return;
        };
        let index = ev.get("output_index").and_then(|v| v.as_i64());
        if item.get("type").and_then(|t| t.as_str()) != Some("image_generation_call") {
            // Only image results can be used at completion. Preserve last-item-wins
            // for a repeated index without retaining unrelated text/reasoning payloads.
            if let Some(i) = index {
                self.by_index.remove(&i);
            }
            return;
        }
        match index {
            Some(i) => {
                self.by_index.insert(i, item.clone());
            }
            None => self.fallback.push(item.clone()),
        }
    }
    /// CPA `codexExtractImageResults`.
    fn extract(&self, completed: &Value) -> (Vec<ImageResult>, i64, Option<Value>) {
        let created = completed
            .pointer("/response/created_at")
            .and_then(|v| v.as_i64())
            .filter(|v| *v > 0)
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0)
            });
        let s = |i: &Value, k: &str| {
            i.get(k)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string()
        };
        let output = completed
            .pointer("/response/output")
            .and_then(|v| v.as_array());
        let items: Vec<&Value> = match output {
            Some(output) if !output.is_empty() => output.iter().collect(),
            _ => self.by_index.values().chain(self.fallback.iter()).collect(),
        };
        let results = items
            .into_iter()
            .filter(|i| i.get("type").and_then(|t| t.as_str()) == Some("image_generation_call"))
            .filter_map(|i| {
                let result = s(i, "result");
                (!result.is_empty()).then(|| ImageResult {
                    result,
                    revised_prompt: s(i, "revised_prompt"),
                    fmt: s(i, "output_format"),
                    size: s(i, "size"),
                    background: s(i, "background"),
                    quality: s(i, "quality"),
                })
            })
            .collect();
        let usage = completed
            .pointer("/response/tool_usage/image_gen")
            .filter(|v| v.is_object())
            .cloned();
        (results, created, usage)
    }
}

/// Parse upstream SSE into JSON `data:` events, with the idle timeout.
struct ImageReadError {
    code: &'static str,
    source: &'static str,
    message: String,
}

fn sse_events(
    resp: reqwest::Response,
    idle: std::time::Duration,
) -> impl futures::Stream<Item = Result<Value, ImageReadError>> + Send {
    let up = resp.bytes_stream();
    futures::stream::unfold(
        (
            Box::pin(up),
            crate::sse::Framer::new(usize::MAX),
            std::collections::VecDeque::<Bytes>::new(),
            false,
        ),
        move |(mut up, mut framer, mut events, mut done)| async move {
            loop {
                while let Some(event) = events.pop_front() {
                    let text = String::from_utf8_lossy(&event);
                    if let Some(value) = crate::encrypted::sse_data(text.as_bytes())
                        .and_then(|data| serde_json::from_str(&data).ok())
                    {
                        return Some((Ok(value), (up, framer, events, done)));
                    }
                }
                if done {
                    return None;
                }
                match tokio::time::timeout(idle, up.next()).await {
                    Err(_) => {
                        return Some((
                            Err(ImageReadError {
                                code: "idle_timeout",
                                source: errors::SRC_NETWORK,
                                message: "stream idle timeout".into(),
                            }),
                            (up, framer, events, true),
                        ))
                    }
                    Ok(None) => {
                        done = true;
                        // The previous line parser accepted a newline-terminated data line at EOF.
                        if let Some(tail) = framer.finish().filter(|tail| tail.ends_with(b"\n")) {
                            events.push_back(tail);
                        }
                    }
                    Ok(Some(Err(e))) => {
                        return Some((
                            Err(ImageReadError {
                                code: "stream_read_error",
                                source: errors::SRC_NETWORK,
                                message: e.to_string(),
                            }),
                            (up, framer, events, true),
                        ))
                    }
                    Ok(Some(Ok(chunk))) => {
                        for frame in framer.push(chunk) {
                            let crate::sse::Frame::Event(event) = frame else {
                                continue;
                            };
                            events.push_back(event);
                        }
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reducer_keeps_partial_completion_failure_and_eof_distinct() {
        let mut reducer = ImageReducer::default();
        for event in [
            json!({"type":"response.created"}),
            json!({"type":"response.image_generation_call.partial_image", "partial_image_b64":""}),
            json!({"type":"response.output_item.done", "output_index":0, "item":{"type":"image_generation_call", "result":"final", "output_format":"webp"}}),
        ] {
            assert!(reducer.step(Ok(event)).is_none());
        }
        assert!(matches!(reducer.step(Ok(json!({
            "type":"response.image_generation_call.partial_image", "partial_image_b64":"partial",
            "partial_image_index":2, "output_format":"png"
        }))), Some(ImageStep::Partial { index:2, format, b64 }) if format == "png" && b64 == "partial"));
        let incomplete = json!({"type":"response.incomplete", "response":{
            "created_at":123, "output":[], "incomplete_details":{"reason":"max_output_tokens"},
            "tool_usage":{"image_gen":{"count":1}}
        }});
        let Some(ImageStep::Done {
            results,
            created,
            usage,
            incomplete: reason,
        }) = reducer.step(Ok(incomplete.clone()))
        else {
            panic!("collected image completes an incomplete response");
        };
        assert_eq!(results[0].result, "final");
        assert_eq!(results[0].fmt, "webp");
        assert_eq!(created, 123);
        assert_eq!(usage, Some(json!({"count":1})));
        assert_eq!(reason, Some(incomplete));
        for ty in ["response.completed", "response.incomplete"] {
            assert!(
                matches!(ImageReducer::default().step(Ok(json!({"type":ty}))), Some(ImageStep::NoImage { ty: actual, .. }) if actual == ty)
            );
        }
        assert!(
            matches!(reducer.step(Ok(json!({"type":"error", "error":{"code":"bad_image", "message":{"reason":"failed"}}}))), Some(ImageStep::Failed { code, message }) if code == "bad_image" && message == json!({"reason":"failed"}))
        );
        assert!(matches!(
            reducer.step(Err(ImageReadError {
                code: "idle_timeout",
                source: errors::SRC_NETWORK,
                message: "stream idle timeout".into()
            })),
            Some(ImageStep::ReadError(ImageReadError {
                code: "idle_timeout",
                ..
            }))
        ));
        assert!(matches!(reducer.eof(), ImageStep::Truncated));
    }

    #[tokio::test]
    async fn sse_events_join_data_across_chunks_and_keep_newline_terminated_eof() {
        let wire = Bytes::from_static(
            concat!(
                ": comment\r\ndata: {\"type\":\"response.output_text.delta\",\r\n",
                "data: \"delta\":\"hi\"}\r\n\r\n",
                "data: [DONE]\n\n",
                "data: {\"type\":\"response.completed\"}\n"
            )
            .as_bytes(),
        );
        for split in 0..=wire.len() {
            let chunks = [wire.slice(..split), wire.slice(split..)];
            let body = reqwest::Body::wrap_stream(futures::stream::iter(
                chunks.map(Ok::<_, std::io::Error>),
            ));
            let response = reqwest::Response::from(http::Response::new(body));
            let events: Vec<_> = sse_events(response, std::time::Duration::from_secs(1))
                .collect()
                .await;
            let events: Vec<_> = events
                .into_iter()
                .map(|event| event.map_err(|e| e.message).unwrap())
                .collect();
            assert_eq!(
                events,
                [
                    json!({"type":"response.output_text.delta", "delta":"hi"}),
                    json!({"type":"response.completed"})
                ]
            );
        }
    }

    #[test]
    fn image_results_keep_completion_precedence_and_index_order() {
        let mut col = Collector::default();
        for ev in [
            json!({"output_index": 2, "item": {"type": "image_generation_call", "result": "two"}}),
            json!({"output_index": 1, "item": {"type": "image_generation_call", "result": "old"}}),
            json!({"item": {"type": "message", "content": "not an image"}}),
            json!({"output_index": 1, "item": {"type": "message", "content": "replaced"}}),
            json!({"output_index": 0, "item": {"type": "image_generation_call", "result": "zero"}}),
            json!({"item": {"type": "image_generation_call", "result": "fallback"}}),
        ] {
            col.item_done(&ev);
        }
        let (images, created, usage) = col.extract(&json!({"response": {
            "created_at": 123, "output": [], "tool_usage": {"image_gen": {"count": 3}}
        }}));
        assert_eq!(
            images.iter().map(|i| i.result.as_str()).collect::<Vec<_>>(),
            ["zero", "two", "fallback"]
        );
        assert_eq!(created, 123);
        assert_eq!(usage, Some(json!({"count": 3})));
        let (images, _, _) = col.extract(&json!({"response": {"output": [
            {"type": "message", "content": "not an image"},
            {"type": "image_generation_call", "result": "final", "output_format": "png"}
        ]}}));
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].result, "final");
        assert_eq!(images[0].fmt, "png");
        assert!(col
            .extract(&json!({"response": {"output": [
                {"type": "message", "content": "authoritative output without images"}
            ]}}))
            .0
            .is_empty());
    }
}
