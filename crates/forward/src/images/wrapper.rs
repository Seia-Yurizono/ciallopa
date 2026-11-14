use super::fail;
use super::request::{
    bad, build_tool, multipart_edit_to_json, out_headers, parse_multipart, responses_request, Kind,
    DEFAULT_TOOL_MODEL, DIRECT_MODELS,
};
use crate::downstream::observed;
use crate::proxy::{self, json_error, Outbound};
use crate::{errors, usage::Tracker};
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
    let prefix = if kind == Kind::Edit {
        "image_edit"
    } else {
        "image_generation"
    };
    let events = sse_events(c.resp, c.idle);
    if stream {
        let rf = response_format;
        let mut trk = trk.take();
        if let Some(t) = trk.as_mut() {
            t.status(200);
            t.sse_response();
        }
        let body = observed(st.upstream.drain.clone(), None, move |tx| async move {
            let mut events = Box::pin(events);
            let mut col = Collector::default();
            while let Some(ev) = events.next().await {
                let ev = match ev {
                    Ok(v) => v,
                    Err(e) => {
                        fail(&mut trk, e.code, e.source, &e.message);
                        let _ = tx
                            .send(Ok(sse("error", &json!({"error": {"message": e.message}}))))
                            .await;
                        return;
                    }
                };
                match ev.get("type").and_then(|t| t.as_str()).unwrap_or("") {
                    "response.image_generation_call.partial_image" => {
                        let b64 = ev
                            .get("partial_image_b64")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if b64.is_empty() {
                            continue;
                        }
                        let name = format!("{prefix}.partial_image");
                        let mut d = json!({"type": name, "partial_image_index": ev.get("partial_image_index").and_then(|v| v.as_i64()).unwrap_or(0)});
                        let fmt = ev
                            .get("output_format")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        set_image(&mut d, rf, fmt, b64);
                        if tx.send(Ok(sse(&name, &d))).await.is_err() {
                            return;
                        }
                    }
                    "response.output_item.done" => col.item_done(&ev),
                    ty @ ("response.completed" | "response.incomplete") => {
                        let (results, _, usage) = col.extract(&ev);
                        if results.is_empty() {
                            fail_no_image(&mut trk, ty, &ev);
                            let _ = tx.send(Ok(sse("error", &json!({"error": {"message": "upstream did not return image output"}})))).await;
                            return;
                        }
                        let name = format!("{prefix}.completed");
                        for img in results {
                            let mut d = json!({"type": name});
                            if let Some(u) = &usage {
                                d["usage"] = u.clone();
                            }
                            set_image(&mut d, rf, &img.fmt, &img.result);
                            if tx.send(Ok(sse(&name, &d))).await.is_err() {
                                return;
                            }
                        }
                        if let Some(t) = trk.as_mut() {
                            if ty == "response.incomplete" {
                                t.incomplete(&ev);
                            }
                            t.finish_response();
                        }
                        return;
                    }
                    "response.failed" | "error" => {
                        let msg = ev
                            .pointer("/response/error/message")
                            .or_else(|| ev.pointer("/error/message"))
                            .cloned()
                            .unwrap_or(json!("upstream failed"));
                        fail(
                            &mut trk,
                            &errors::event_code(&ev),
                            errors::SRC_STREAM,
                            msg.as_str().unwrap_or("upstream failed"),
                        );
                        let _ = tx
                            .send(Ok(sse("error", &json!({"error": {"message": msg}}))))
                            .await;
                        return;
                    }
                    _ => {}
                }
            }
            fail(
                &mut trk,
                "stream_truncated",
                errors::SRC_STREAM,
                "upstream image stream ended before completion",
            );
            let _ = tx
                .send(Ok(sse(
                    "error",
                    &json!({"error": {"message": "upstream image stream ended before completion"}}),
                )))
                .await;
        });
        let mut r = Response::new(body);
        r.headers_mut().insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        r.headers_mut().insert(
            http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        );
        return r;
    }

    let mut events = Box::pin(events);
    let mut col = Collector::default();
    while let Some(ev) = events.next().await {
        let ev = match ev {
            Ok(v) => v,
            Err(e) => {
                fail(trk, e.code, e.source, &e.message);
                let status = if e.code == "idle_timeout" {
                    StatusCode::GATEWAY_TIMEOUT
                } else {
                    StatusCode::BAD_GATEWAY
                };
                return json_error(status, "upstream_error", &e.message);
            }
        };
        match ev.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "response.output_item.done" => col.item_done(&ev),
            ty @ ("response.completed" | "response.incomplete") => {
                let (results, created, usage) = col.extract(&ev);
                if results.is_empty() {
                    fail_no_image(trk, ty, &ev);
                    return json_error(
                        StatusCode::BAD_GATEWAY,
                        "upstream_error",
                        "upstream did not return image output",
                    );
                }
                if let Some(t) = trk.as_mut().filter(|_| ty == "response.incomplete") {
                    t.incomplete(&ev);
                }
                let first = &results[0];
                let mut out = json!({"created": created, "data": []});
                for (k, v) in [
                    ("background", &first.background),
                    ("output_format", &first.fmt),
                    ("quality", &first.quality),
                    ("size", &first.size),
                ] {
                    if !v.is_empty() {
                        out[k] = json!(v);
                    }
                }
                if let Some(u) = usage {
                    out["usage"] = u;
                }
                let data: Vec<Value> = results
                    .iter()
                    .map(|img| {
                        let mut d = json!({});
                        if !img.revised_prompt.is_empty() {
                            d["revised_prompt"] = json!(img.revised_prompt);
                        }
                        set_image(&mut d, response_format, &img.fmt, &img.result);
                        d
                    })
                    .collect();
                out["data"] = Value::Array(data);
                let mut r = Response::new(Body::from(out.to_string()));
                r.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                return r;
            }
            "response.failed" | "error" => {
                let msg = ev
                    .pointer("/response/error/message")
                    .or_else(|| ev.pointer("/error/message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("upstream failed")
                    .to_string();
                fail(trk, &errors::event_code(&ev), errors::SRC_STREAM, &msg);
                return json_error(StatusCode::BAD_GATEWAY, "upstream_error", &msg);
            }
            _ => {}
        }
    }
    fail(
        trk,
        "stream_truncated",
        errors::SRC_STREAM,
        "upstream image stream ended before completion",
    );
    json_error(
        StatusCode::BAD_GATEWAY,
        "upstream_error",
        "stream error: stream disconnected before completion",
    )
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
        (Box::pin(up), Vec::<u8>::new(), false),
        move |(mut up, mut buf, done)| async move {
            if done {
                return None;
            }
            loop {
                if let Some(pos) = buf.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=pos).collect();
                    let line = String::from_utf8_lossy(&line);
                    let line = line.trim();
                    if let Some(d) = line.strip_prefix("data:") {
                        if let Ok(v) = serde_json::from_str::<Value>(d.trim()) {
                            return Some((Ok(v), (up, buf, false)));
                        }
                    }
                    continue;
                }
                match tokio::time::timeout(idle, up.next()).await {
                    Err(_) => {
                        return Some((
                            Err(ImageReadError {
                                code: "idle_timeout",
                                source: errors::SRC_NETWORK,
                                message: "stream idle timeout".into(),
                            }),
                            (up, buf, true),
                        ))
                    }
                    Ok(None) => return None,
                    Ok(Some(Err(e))) => {
                        return Some((
                            Err(ImageReadError {
                                code: "stream_read_error",
                                source: errors::SRC_NETWORK,
                                message: e.to_string(),
                            }),
                            (up, buf, true),
                        ))
                    }
                    Ok(Some(Ok(c))) => buf.extend_from_slice(&c),
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
