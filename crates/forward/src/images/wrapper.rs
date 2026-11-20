use super::fail;
use super::request::{
    bad, build_tool, multipart_edit_to_json, out_headers, parse_multipart, responses_request, Kind,
    DEFAULT_TOOL_MODEL, DIRECT_MODELS,
};
use crate::attempt::Outbound;
use crate::downstream::observed;
use crate::errors::json_error;
use crate::{
    errors,
    usage::{End, Tracker},
};
use crate::{session_route, AppState};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::{self, Write};

// Two 6 MiB images encode to 16 MiB of base64. Allow 2 MiB more for retained
// prompts/metadata, and another 2 MiB for a terminal's SSE/JSON envelope.
const RESULT_BYTES: usize = 18 << 20;
const EVENT_BYTES: usize = 20 << 20;
// Cap collection nodes even for tiny results, with room beyond the usual one or two images.
const RESULT_ITEMS: usize = 16;
// One buffered response or one SSE frame, including escaped metadata, usage and data URLs.
const OUTPUT_BYTES: usize = 20 << 20;
const TOO_LARGE: &str = "image_output_too_large";

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
        return match crate::attempt::send_tracked(&st, ob, trk).await {
            Ok(c) => crate::http::pump::stream_back_tracked(&st, c, trk.take(), false, true),
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
    let c = match crate::attempt::send_tracked(&st, ob, trk).await {
        Ok(c) if c.resp.status().is_success() => c,
        Ok(c) => return crate::http::pump::stream_back_tracked(&st, c, trk.take(), false, true),
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
    },
    NoImage,
    Failed {
        code: String,
        message: Value,
    },
    Truncated,
}

impl Collector {
    fn step(
        &mut self,
        mut event: Value,
        trk: &mut Option<Tracker>,
    ) -> Result<Option<ImageStep>, ImageError> {
        let step = match event.get("type").and_then(Value::as_str) {
            Some("response.image_generation_call.partial_image") => {
                let b64 = take_string(&mut event, "partial_image_b64");
                (!b64.is_empty()).then(|| ImageStep::Partial {
                    index: event
                        .get("partial_image_index")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                    format: take_string(&mut event, "output_format"),
                    b64,
                })
            }
            Some("response.output_item.done") => {
                let index = event.get("output_index").and_then(Value::as_i64);
                if let Some(item) = event.get_mut("item").filter(|v| v.is_object()) {
                    self.retain(index, item)?;
                }
                None
            }
            Some(ty @ ("response.completed" | "response.incomplete")) => {
                let incomplete = ty == "response.incomplete";
                Some(self.complete(event, incomplete, trk)?)
            }
            Some("response.failed" | "error") => {
                let code = errors::event_code(&event);
                let message = event
                    .pointer_mut("/response/error/message")
                    .map(Value::take)
                    .or_else(|| event.pointer_mut("/error/message").map(Value::take))
                    .unwrap_or(json!("upstream failed"));
                Some(ImageStep::Failed { code, message })
            }
            _ => None,
        };
        Ok(step)
    }

    fn complete(
        &mut self,
        mut event: Value,
        incomplete: bool,
        trk: &mut Option<Tracker>,
    ) -> Result<ImageStep, ImageError> {
        if let Some(output) = event
            .pointer_mut("/response/output")
            .and_then(Value::as_array_mut)
            .filter(|a| !a.is_empty())
        {
            // A nonempty terminal array is authoritative, even when it contains only text.
            *self = Self::default();
            for mut item in std::mem::take(output) {
                self.retain(None, &mut item)?;
            }
        }
        let collected = std::mem::take(self);
        let results: Vec<_> = collected
            .by_index
            .into_values()
            .chain(collected.fallback)
            .collect();
        if incomplete {
            if let Some(t) = trk.as_mut() {
                t.incomplete(&event);
            }
        }

        if results.is_empty() {
            if incomplete {
                let reason = crate::usage::incomplete_reason(&event);
                fail(
                    trk,
                    &crate::usage::incomplete_code(&reason),
                    errors::SRC_STREAM,
                    format!("response.incomplete: {reason}"),
                );
            } else {
                fail(
                    trk,
                    "image_output_missing",
                    errors::SRC_STREAM,
                    "upstream completed without image output",
                );
            }
            return Ok(ImageStep::NoImage);
        }
        let created = event
            .pointer("/response/created_at")
            .and_then(Value::as_i64)
            .filter(|v| *v > 0)
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0)
            });
        let usage = event
            .pointer_mut("/response/tool_usage/image_gen")
            .filter(|v| v.is_object())
            .map(Value::take);
        Ok(ImageStep::Done {
            results,
            created,
            usage,
        })
    }

    async fn next<S>(
        &mut self,
        events: &mut S,
        trk: &mut Option<Tracker>,
    ) -> Result<ImageStep, ImageError>
    where
        S: futures::Stream<Item = Result<Value, ImageError>> + Unpin,
    {
        while let Some(event) = events.next().await {
            if let Some(step) = self.step(event?, trk)? {
                return Ok(step);
            }
        }
        Ok(ImageStep::Truncated)
    }
}

impl ImageStep {
    fn error_body(self, trk: &mut Option<Tracker>, stream: bool) -> Result<Bytes, ImageError> {
        const TRUNCATED: &str = "upstream image stream ended before completion";
        let (code, message) = match self {
            Self::NoImage => (None, json!("upstream did not return image output")),
            Self::Failed { code, message } => (Some(code), message),
            Self::Truncated => {
                fail(trk, "stream_truncated", errors::SRC_STREAM, TRUNCATED);
                (
                    None,
                    json!(if stream {
                        TRUNCATED
                    } else {
                        "stream error: stream disconnected before completion"
                    }),
                )
            }
            _ => unreachable!("only failed image steps have an error body"),
        };
        let mut data = json!({"error": {}});
        data["error"]["message"] = if stream || message.is_string() {
            message
        } else {
            json!("upstream failed")
        };
        if !stream {
            data["error"]["type"] = json!("upstream_error");
        }
        let bytes = encode(&data, stream.then_some("error"), OUTPUT_BYTES)?;
        if let Some(code) = code {
            fail(
                trk,
                &code,
                errors::SRC_STREAM,
                data["error"]["message"]
                    .as_str()
                    .unwrap_or("upstream failed"),
            );
        }
        Ok(bytes)
    }
}

// serde_json may expand strings while escaping them. Check each write before growing the
// final wire buffer, including the SSE header and delimiter.
struct OutputWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for OutputWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.limit - self.bytes.len() {
            return Err(io::ErrorKind::FileTooLarge.into());
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode(data: &Value, event: Option<&str>, limit: usize) -> Result<Bytes, ImageError> {
    let mut out = OutputWriter {
        bytes: Vec::new(),
        limit,
    };
    (|| -> io::Result<()> {
        if let Some(name) = event {
            write!(out, "event: {name}\ndata: ")?;
        }
        serde_json::to_writer(&mut out, data)?;
        if event.is_some() {
            out.write_all(b"\n\n")?;
        }
        Ok(())
    })()
    .map_err(|_| ImageError::limit("serialized output bytes", limit))?;
    Ok(Bytes::from(out.bytes))
}

fn stream_images(
    st: &AppState,
    events: impl futures::Stream<Item = Result<Value, ImageError>> + Send + 'static,
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
        t.rec.status = 200;
        t.rec.transport = ciallopa_usage::Transport::Sse;
    }
    let body = observed(st.upstream.drain.clone(), None, move |tx| async move {
        let result = send_images(&tx, events, prefix, response_format, &mut trk).await;
        let end = match result {
            Ok(end) => end,
            Err(error) => {
                fail(&mut trk, error.code, error.source, &error.message);
                let mut data = json!({"error":{"message": error.message}});
                if error.code == TOO_LARGE {
                    data["error"]["type"] = json!(TOO_LARGE);
                }
                let _ = tx.send(Ok(sse("error", &data))).await;
                End::Completed
            }
        };
        if let Some(t) = trk {
            t.finish(end);
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

async fn send_images(
    tx: &crate::downstream::ObsTx,
    events: impl futures::Stream<Item = Result<Value, ImageError>>,
    prefix: &str,
    response_format: &str,
    trk: &mut Option<Tracker>,
) -> Result<End, ImageError> {
    let mut events = Box::pin(events);
    let mut reducer = Collector::default();
    loop {
        match reducer.next(&mut events, trk).await? {
            ImageStep::Partial { index, format, b64 } => {
                let name = format!("{prefix}.partial_image");
                let mut data = json!({"type": name, "partial_image_index": index});
                set_image(&mut data, response_format, &format, b64);
                let frame = encode(&data, Some(&name), OUTPUT_BYTES)?;
                if tx.send(Ok(frame)).await.is_err() {
                    return Ok(End::ClientGone);
                }
            }
            ImageStep::Done { results, usage, .. } => {
                drop(events);
                let name = format!("{prefix}.completed");
                let mut data = json!({"type": name});
                if let Some(usage) = usage {
                    data["usage"] = usage;
                }
                for image in results {
                    set_image(&mut data, response_format, &image.fmt, image.result);
                    let frame = encode(&data, Some(&name), OUTPUT_BYTES)?;
                    if tx.send(Ok(frame)).await.is_err() {
                        return Ok(End::ClientGone);
                    }
                }
                return Ok(End::Completed);
            }
            step => {
                drop(events);
                let _ = tx.send(Ok(step.error_body(trk, true)?)).await;
                return Ok(End::Completed);
            }
        }
    }
}

fn buffered_output(
    results: Vec<ImageResult>,
    created: i64,
    usage: Option<Value>,
    rf: &str,
) -> Value {
    let mut out = json!({"created": created});
    let mut rows = Vec::with_capacity(results.len());
    for (i, image) in results.into_iter().enumerate() {
        let mut row = json!({});
        set_image(&mut row, rf, &image.fmt, image.result);
        if !image.revised_prompt.is_empty() {
            row["revised_prompt"] = Value::String(image.revised_prompt);
        }
        rows.push(row);
        if i == 0 {
            for (key, value) in [
                ("background", image.background),
                ("output_format", image.fmt),
                ("quality", image.quality),
                ("size", image.size),
            ] {
                if !value.is_empty() {
                    out[key] = Value::String(value);
                }
            }
        }
    }
    out["data"] = Value::Array(rows);
    if let Some(usage) = usage {
        out["usage"] = usage;
    }
    out
}

async fn buffered_images(
    events: impl futures::Stream<Item = Result<Value, ImageError>>,
    response_format: &'static str,
    trk: &mut Option<Tracker>,
) -> Response {
    let mut events = Box::pin(events);
    let mut reducer = Collector::default();
    let step = loop {
        match reducer.next(&mut events, trk).await {
            Ok(ImageStep::Partial { .. }) => continue,
            other => break other,
        }
    };
    drop(events);
    let result = step.and_then(|step| match step {
        ImageStep::Done {
            results,
            created,
            usage,
        } => {
            let data = buffered_output(results, created, usage, response_format);
            encode(&data, None, OUTPUT_BYTES).map(|bytes| (StatusCode::OK, bytes))
        }
        step => step
            .error_body(trk, false)
            .map(|bytes| (StatusCode::BAD_GATEWAY, bytes)),
    });
    match result {
        Ok((status, bytes)) => (
            status,
            [(http::header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response(),
        Err(error) => {
            fail(trk, error.code, error.source, &error.message);
            let status = if error.code == "idle_timeout" {
                StatusCode::GATEWAY_TIMEOUT
            } else {
                StatusCode::BAD_GATEWAY
            };
            json_error(
                status,
                if error.code == TOO_LARGE {
                    TOO_LARGE
                } else {
                    "upstream_error"
                },
                &error.message,
            )
        }
    }
}

fn set_image(d: &mut Value, rf: &str, fmt: &str, mut b64: String) {
    let key = if rf == "url" {
        let fmt = fmt.trim();
        let prefix = if fmt.eq_ignore_ascii_case("jpg") || fmt.eq_ignore_ascii_case("jpeg") {
            "data:image/jpeg;base64,"
        } else if fmt.eq_ignore_ascii_case("webp") {
            "data:image/webp;base64,"
        } else {
            "data:image/png;base64,"
        };
        b64.reserve_exact(prefix.len());
        b64.insert_str(0, prefix);
        "url"
    } else {
        "b64_json"
    };
    d[key] = Value::String(b64);
}

struct ImageResult {
    result: String,
    revised_prompt: String,
    fmt: String,
    size: String,
    background: String,
    quality: String,
}

fn take_string(v: &mut Value, key: &str) -> String {
    match v.get_mut(key).map(Value::take) {
        Some(Value::String(s)) => s,
        _ => String::new(),
    }
}

impl ImageResult {
    fn from_item(item: &mut Value) -> Option<Self> {
        if item.get("type").and_then(Value::as_str) != Some("image_generation_call") {
            return None;
        }
        let mut field = |key| {
            let mut s = take_string(item, key);
            s.truncate(s.trim_end().len());
            let leading = s.len() - s.trim_start().len();
            s.drain(..leading);
            s
        };
        let result = field("result");
        (!result.is_empty()).then(|| Self {
            result,
            revised_prompt: field("revised_prompt"),
            fmt: field("output_format"),
            size: field("size"),
            background: field("background"),
            quality: field("quality"),
        })
    }
    fn bytes(&self) -> usize {
        self.result.capacity()
            + self.revised_prompt.capacity()
            + self.fmt.capacity()
            + self.size.capacity()
            + self.background.capacity()
            + self.quality.capacity()
    }
}

#[derive(Default)]
struct Collector {
    by_index: BTreeMap<i64, ImageResult>,
    fallback: Vec<ImageResult>,
    bytes: usize,
}

impl Collector {
    fn retain(&mut self, index: Option<i64>, item: &mut Value) -> Result<(), ImageError> {
        if let Some(old) = index.and_then(|i| self.by_index.remove(&i)) {
            self.bytes -= old.bytes();
        }
        let Some(image) = ImageResult::from_item(item) else {
            return Ok(());
        };
        if self.by_index.len() + self.fallback.len() == RESULT_ITEMS {
            return Err(ImageError::limit("result item count", RESULT_ITEMS));
        }
        let bytes = image.bytes();
        if bytes > RESULT_BYTES - self.bytes {
            return Err(ImageError::limit("retained result bytes", RESULT_BYTES));
        }
        self.bytes += bytes;
        if let Some(i) = index {
            self.by_index.insert(i, image);
        } else {
            self.fallback.push(image);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ImageError {
    code: &'static str,
    source: &'static str,
    message: String,
}

impl ImageError {
    fn limit(name: &str, max: usize) -> Self {
        Self {
            code: TOO_LARGE,
            source: errors::SRC_STREAM,
            message: format!("image {name} exceeds limit {max}"),
        }
    }
}

fn sse_events(
    resp: reqwest::Response,
    idle: std::time::Duration,
) -> impl futures::Stream<Item = Result<Value, ImageError>> + Send {
    let state = (
        Box::pin(resp.bytes_stream().fuse()),
        crate::sse::Framer::new(EVENT_BYTES),
        Bytes::new(),
    );
    futures::stream::try_unfold(state, move |(mut up, mut framer, mut chunk)| async move {
        loop {
            let event = match framer.next_part(&mut chunk) {
                Some((crate::sse::Frame::Oversize(_), _)) => {
                    return Err(ImageError::limit("event bytes", EVENT_BYTES))
                }
                Some((crate::sse::Frame::Event(bytes), _)) => bytes,
                None => match tokio::time::timeout(idle, up.next()).await {
                    Err(_) => {
                        return Err(ImageError {
                            code: "idle_timeout",
                            source: errors::SRC_NETWORK,
                            message: "stream idle timeout".into(),
                        })
                    }
                    Ok(Some(Err(e))) => {
                        return Err(ImageError {
                            code: "stream_read_error",
                            source: errors::SRC_NETWORK,
                            message: e.to_string(),
                        })
                    }
                    Ok(Some(Ok(bytes))) => {
                        chunk = bytes;
                        continue;
                    }
                    // Preserve a newline-terminated data line at EOF.
                    Ok(None) => match framer.finish().filter(|tail| tail.ends_with(b"\n")) {
                        Some(tail) => tail,
                        None => return Ok(None),
                    },
                },
            };
            let text = String::from_utf8_lossy(&event);
            if let Some(value) =
                crate::sse::data(text.as_bytes()).and_then(|data| serde_json::from_str(&data).ok())
            {
                return Ok(Some((value, (up, framer, chunk))));
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reducer_keeps_partial_completion_and_failure_distinct() {
        let mut reducer = Collector::default();
        for event in [
            json!({"type":"response.created"}),
            json!({"type":"response.image_generation_call.partial_image", "partial_image_b64":""}),
            json!({"type":"response.output_item.done", "output_index":0, "item":{"type":"image_generation_call", "result":"final", "output_format":"webp"}}),
        ] {
            assert!(reducer.step(event, &mut None).unwrap().is_none());
        }
        assert!(matches!(reducer.step(json!({
            "type":"response.image_generation_call.partial_image", "partial_image_b64":"partial",
            "partial_image_index":2, "output_format":"png"
        }), &mut None).unwrap(), Some(ImageStep::Partial { index:2, format, b64 }) if format == "png" && b64 == "partial"));
        let incomplete = json!({"type":"response.incomplete", "response":{
            "created_at":123, "output":[], "incomplete_details":{"reason":"max_output_tokens"},
            "tool_usage":{"image_gen":{"count":1}}
        }});
        let Some(ImageStep::Done {
            results,
            created,
            usage,
        }) = reducer.step(incomplete, &mut None).unwrap()
        else {
            panic!("collected image completes an incomplete response");
        };
        assert_eq!(results[0].result, "final");
        assert_eq!(results[0].fmt, "webp");
        assert_eq!(created, 123);
        assert_eq!(usage, Some(json!({"count":1})));
        for ty in ["response.completed", "response.incomplete"] {
            assert!(matches!(
                reducer.step(json!({"type":ty}), &mut None).unwrap(),
                Some(ImageStep::NoImage)
            ));
        }
        assert!(matches!(
            reducer.step(json!({"type":"error","error":{"code":"bad_image","message":{"reason":"failed"}}}), &mut None).unwrap(),
            Some(ImageStep::Failed { code, message }) if code == "bad_image" && message == json!({"reason":"failed"})
        ));
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

    #[tokio::test]
    async fn event_limit_counts_delimiter_and_drops_upstream_on_first_oversize() {
        use futures::stream;
        use std::{sync::Arc, time::Duration};
        for size in [EVENT_BYTES - 1, EVENT_BYTES, EVENT_BYTES + 1] {
            let wire = Bytes::from(format!("data: 0{}\r\n\r\n", " ".repeat(size - 11)));
            assert_eq!(wire.len(), size);
            let chunks = [wire.slice(..size - 1), wire.slice(size - 1..)];
            let body = reqwest::Body::wrap_stream(stream::iter(chunks.map(Ok::<_, io::Error>)));
            let mut events = Box::pin(sse_events(
                http::Response::new(body).into(),
                Duration::from_secs(1),
            ));
            let result = events.next().await.unwrap();
            if size > EVENT_BYTES {
                assert_eq!(result.unwrap_err().code, TOO_LARGE);
            } else {
                assert_eq!(result.unwrap(), json!(0));
            }
            assert!(events.next().await.is_none());
        }
        // No delimiter and no EOF. Polling the source beyond the first offending byte fails the test.
        let live = Arc::new(());
        let observed = Arc::downgrade(&live);
        let chunks = stream::iter([
            Bytes::from(vec![b'x'; EVENT_BYTES]),
            Bytes::from_static(b"x"),
        ])
        .chain(stream::once(async move {
            drop(live);
            panic!("polled past oversize")
        }))
        .map(Ok::<_, io::Error>);
        let response = http::Response::new(reqwest::Body::wrap_stream(chunks)).into();
        let mut events = Box::pin(sse_events(response, Duration::from_secs(1)));
        let error = events.next().await.unwrap().unwrap_err();
        assert_eq!(error.code, TOO_LARGE);
        assert!(error.message.contains("event bytes"));
        assert!(observed.upgrade().is_none(), "upstream still retained");
        assert!(events.next().await.is_none());
    }

    async fn buffered(wire: String, rf: &'static str) -> (StatusCode, Value) {
        let response = http::Response::new(reqwest::Body::from(wire)).into();
        let events = sse_events(response, std::time::Duration::from_secs(1));
        let res = buffered_images(events, rf, &mut None).await;
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), OUTPUT_BYTES)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    fn item(result: &str) -> String {
        format!(r#"{{"type":"image_generation_call","result":"{result}"}}"#)
    }
    fn done(item: &str) -> String {
        format!("data: {{\"type\":\"response.output_item.done\",\"item\":{item}}}\n\n")
    }
    fn terminal(items: &str) -> String {
        format!(
            "data: {{\"type\":\"response.completed\",\"response\":{{\"output\":[{items}]}}}}\n\n"
        )
    }

    #[tokio::test]
    async fn results_from_one_chunk_and_authoritative_terminal_share_both_limits() {
        for (n, size, limit) in [
            (RESULT_ITEMS + 1, 4, "item count"),
            (
                RESULT_ITEMS,
                RESULT_BYTES / RESULT_ITEMS + 4,
                "retained result bytes",
            ),
        ] {
            let image = item(&"A".repeat(size));
            let wire = done(&image).repeat(n) + &terminal("");
            let (status, out) = buffered(wire, "b64_json").await;
            assert_eq!(status, StatusCode::BAD_GATEWAY);
            assert_eq!(out["error"]["type"], TOO_LARGE);
            assert!(out["error"]["message"].as_str().unwrap().contains(limit));
            let items = vec![image.as_str(); n].join(",");
            let wire = done(&image) + &terminal(&items);
            let (status, out) = buffered(wire, "b64_json").await;
            assert_eq!(status, StatusCode::BAD_GATEWAY);
            assert_eq!(out["error"]["type"], TOO_LARGE);
            assert!(out["error"]["message"].as_str().unwrap().contains(limit));
        }
    }

    #[tokio::test]
    async fn terminal_can_repeat_two_large_images_and_return_data_urls() {
        let image = item(&"A".repeat(8 << 20));
        let wire = done(&image).repeat(2) + &terminal(&format!("{image},{image}"));
        let (status, out) = buffered(wire, "url").await;
        assert_eq!(status, StatusCode::OK);
        let rows = out["data"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        for row in rows {
            let image = row["url"]
                .as_str()
                .unwrap()
                .strip_prefix("data:image/png;base64,")
                .unwrap();
            assert_eq!(image.len(), 8 << 20);
            assert!(image.bytes().all(|c| c == b'A'));
        }
    }

    #[test]
    fn replacing_index_refunds_all_fields_and_preserves_order_and_terminal_precedence() {
        let mut col = Collector::default();
        for (mut ev, bytes) in [
            json!({"output_index": 2, "item": {"type": "image_generation_call", "result": "two"}}),
            json!({"output_index": 1, "item": {"type": "image_generation_call", "result": "old","revised_prompt":"prompt","output_format":"webp","quality":"low","background":"green","size":"512"}}),
            json!({"output_index": 1, "item": {"type": "image_generation_call", "result": "new"}}),
            json!({"output_index": 3, "item": {"type": "image_generation_call", "result": "old"}}),
            json!({"output_index": 3, "item": {"type": "message", "content": "replaced"}}),
            json!({"item": {"type":"image_generation_call","result":"fallback"}}),
        ].into_iter().zip([3, 27, 6, 9, 6, 14]) {
            ev["type"] = json!("response.output_item.done");
            assert!(col.step(ev, &mut None).unwrap().is_none());
            assert_eq!(col.bytes, bytes);
        }
        let Some(ImageStep::Done { results, .. }) = col
            .step(
                json!({"type":"response.completed","response":{"output":[]}}),
                &mut None,
            )
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            results
                .iter()
                .map(|r| r.result.as_str())
                .collect::<Vec<_>>(),
            ["new", "two", "fallback"]
        );
        col.retain(
            None,
            &mut json!({"type":"image_generation_call","result":"discarded"}),
        )
        .unwrap();
        assert!(matches!(
            col.step(
                json!({"type":"response.completed","response":{"output":[{"type":"message"}]}}),
                &mut None
            )
            .unwrap(),
            Some(ImageStep::NoImage)
        ));
    }

    #[test]
    fn serialized_limit_includes_escaping_and_sse_envelope() {
        let mut data = json!({});
        set_image(&mut data, "url", "jpeg", "A\"\n".into());
        let json = r#"{"url":"data:image/jpeg;base64,A\"\n"}"#;
        for event in [None, Some("image_edit.completed")] {
            let wire = match event {
                None => json.to_owned(),
                Some(name) => format!("event: {name}\ndata: {json}\n\n"),
            };
            assert_eq!(encode(&data, event, wire.len()).unwrap(), wire);
            let e = encode(&data, event, wire.len() - 1).unwrap_err();
            assert_eq!(e.code, TOO_LARGE);
            assert!(e.message.contains("serialized output bytes"));
        }
    }
}
