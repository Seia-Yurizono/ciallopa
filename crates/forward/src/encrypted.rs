//! Upstream rejection of the encrypted reasoning content carried in a conversation's history
//! (`invalid_encrypted_content`).
//!
//! Codex replays the `encrypted_content` of earlier reasoning / compaction items on every turn.
//! The upstream refuses it when it was produced under another account or organization (or is
//! corrupt), which no retry on any account can fix: the request itself must change. Like
//! official Codex, nothing in the history is stripped or rewritten, and the request is not
//! retried, switched or held against the account. Only the error is made clear:
//!
//! * HTTP, before any output: the upstream sends `200` + an SSE `error` event, which Codex
//!   ignores and then retries the closed stream five times. It is answered with an HTTP `400`
//!   in the upstream's error shape instead, which Codex shows once and does not retry.
//! * HTTP, after output: the event becomes a `response.failed` with the same code and message.
//! * WebSocket: the upstream `error` frame (`status: 400`, fatal for Codex) is passed through
//!   with the clear message.
//!
//! The upstream message (with its `Reason: …`) is kept in the usage log detail.

use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use http::{HeaderValue, StatusCode};
use serde_json::Value;

/// Upstream error code, kept in every rewritten error and used as the usage-log error code.
pub(crate) const CODE: &str = "invalid_encrypted_content";

/// The message the client sees instead of the upstream's opaque one.
pub(crate) const MESSAGE: &str = "The encrypted reasoning content in this conversation's \
history was rejected upstream (it was most likely created under a different account or \
organization) and cannot be reused. Start a new session to continue.";

/// Error objects an event can carry: the event itself, `error`, `response.error`.
fn error_objs(v: &Value) -> [Option<&Value>; 3] {
    [Some(v), v.get("error"), v.pointer("/response/error")]
}

fn field<'a>(o: &'a Value, k: &str) -> Option<&'a str> {
    o.get(k).and_then(|t| t.as_str()).map(str::trim)
}

/// Rejection wording without (or with a generic) code, as seen from relays that rename it.
fn rejection_text(m: &str) -> bool {
    let m = m.to_ascii_lowercase();
    m.contains("encrypted content")
        && (m.contains("could not be verified")
            || m.contains("could not be decrypted")
            || m.contains("did not match"))
}

/// The upstream message when `v` (an SSE `data:` payload, a WS frame or an HTTP error body)
/// is an encrypted-content rejection: code `invalid_encrypted_content`, or the upstream
/// wording under no code / `thinking_signature_invalid`. Only `error` / `response.failed`
/// events and bare error bodies are inspected, never output events.
pub(crate) fn rejection(v: &Value) -> Option<String> {
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let errorish =
        ty == "error" || ty == "response.failed" || (ty.is_empty() && v.get("error").is_some());
    if !errorish {
        return None;
    }
    let objs = error_objs(v);
    let objs = || objs.iter().flatten().filter(|o| o.is_object());
    let message = || {
        objs()
            .filter_map(|o| field(o, "message"))
            .find(|m| !m.is_empty())
            .map(str::to_string)
    };
    if objs().any(|o| field(o, "code") == Some(CODE)) {
        return Some(message().unwrap_or_else(|| CODE.to_string()));
    }
    let generic = objs().all(|o| {
        field(o, "code").is_none_or(|c| c.is_empty() || c == "thinking_signature_invalid")
    });
    if !generic {
        return None;
    }
    message().filter(|m| rejection_text(m))
}

/// Same, on raw JSON text. Cheap byte check first: most events never mention it.
pub(crate) fn rejection_text_json(text: &str) -> Option<String> {
    let lower_hit = text.contains(CODE)
        || text.contains("ncrypted content")
        || text.contains("thinking_signature_invalid");
    if !lower_hit {
        return None;
    }
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| rejection(&v))
}

/// The upstream-shaped error object with the clear message (`param` kept when given).
fn error_obj(v: Option<&Value>) -> Value {
    let param = v
        .map(error_objs)
        .and_then(|objs| {
            objs.into_iter()
                .flatten()
                .find_map(|o| o.get("param").filter(|p| !p.is_null()).cloned())
        })
        .unwrap_or_else(|| Value::from("input"));
    serde_json::json!({
        "message": MESSAGE,
        "type": "invalid_request_error",
        "param": param,
        "code": CODE,
    })
}

/// HTTP `400` answer for a rejection seen before any output (`upstream`: the original event).
pub(crate) fn http_response(upstream: Option<&Value>) -> Response {
    let body = serde_json::json!({ "error": error_obj(upstream) }).to_string();
    let mut r = Response::new(Body::from(body));
    *r.status_mut() = StatusCode::BAD_REQUEST;
    r.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r
}

/// SSE `response.failed` replacing a rejection seen after output was already streamed.
pub(crate) fn failed_event(upstream: Option<&Value>) -> Bytes {
    let mut resp = serde_json::json!({ "status": "failed", "error": error_obj(upstream) });
    if let Some(id) = upstream
        .and_then(|v| v.pointer("/response/id"))
        .filter(|i| i.is_string())
    {
        resp["id"] = id.clone();
    }
    let v = serde_json::json!({ "type": "response.failed", "response": resp });
    Bytes::from(format!("event: response.failed\ndata: {v}\n\n"))
}

/// WS `error` frame: same shape and fields, clear message, `status: 400` (Codex treats a WS
/// error with an HTTP status as that HTTP error; 400 is a fatal invalid request).
pub(crate) fn ws_frame(upstream: &Value) -> String {
    let mut v = upstream.clone();
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    if ty != "error" {
        return serde_json::json!({
            "type": "error",
            "status": 400,
            "error": error_obj(Some(upstream)),
        })
        .to_string();
    }
    if let Some(o) = v.as_object_mut() {
        let mut e = error_obj(Some(upstream));
        if let (Some(dst), Some(src)) = (
            e.as_object_mut(),
            o.get("error").and_then(|x| x.as_object()),
        ) {
            // Keep any extra upstream fields; message / code / type are ours.
            for (k, val) in src {
                if !matches!(k.as_str(), "message" | "code" | "type") {
                    dst.insert(k.clone(), val.clone());
                }
            }
        }
        o.insert("error".into(), e);
        if !o.get("status").is_some_and(|s| s.is_u64()) {
            o.insert("status".into(), 400.into());
        }
    }
    v.to_string()
}

/// One SSE event's joined `data:` payload (None when it has none).
pub(crate) fn sse_data(ev: &[u8]) -> Option<String> {
    let ev = std::str::from_utf8(ev).ok()?;
    let data: Vec<&str> = ev
        .lines()
        .filter_map(|l| l.strip_prefix("data:"))
        .map(|d| d.strip_prefix(' ').unwrap_or(d))
        .collect();
    (!data.is_empty()).then(|| data.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REAL: &str = r#"{"type":"error","error":{"message":"The encrypted content for item rs_0abc could not be verified. Reason: Encrypted content organization_id did not match the target organization.","type":"invalid_request_error","param":"input","code":"invalid_encrypted_content"},"sequence_number":2}"#;

    #[test]
    fn detects_real_and_equivalent_shapes() {
        let v: Value = serde_json::from_str(REAL).unwrap();
        assert!(rejection(&v)
            .unwrap()
            .contains("organization_id did not match"));
        let ws: Value = serde_json::from_str(&REAL.replace(
            "{\"type\":\"error\",",
            "{\"type\":\"error\",\"status\":400,",
        ))
        .unwrap();
        assert!(rejection(&ws).is_some());
        for t in [
            r#"{"type":"response.failed","response":{"error":{"code":"invalid_encrypted_content","message":"x"}}}"#,
            r#"{"error":{"code":"invalid_encrypted_content","message":"m"}}"#,
            r#"{"type":"error","error":{"code":"thinking_signature_invalid","message":"The encrypted content for item rs_1 could not be verified."}}"#,
            r#"{"type":"error","error":{"message":"The encrypted content for item rs_1 could not be verified. Reason: Encrypted content item_id did not match the target item id."}}"#,
        ] {
            assert!(rejection_text_json(t).is_some(), "{t}");
        }
        for t in [
            // Output that merely talks about it.
            r#"{"type":"response.output_text.delta","delta":"invalid_encrypted_content: The encrypted content could not be verified"}"#,
            // Another real code wins over wording.
            r#"{"type":"error","error":{"code":"context_length_exceeded","message":"The encrypted content could not be verified"}}"#,
            r#"{"type":"error","error":{"code":"server_is_overloaded","message":"overloaded"}}"#,
            r#"{"type":"response.completed","response":{"id":"r"}}"#,
        ] {
            assert!(rejection_text_json(t).is_none(), "{t}");
        }
    }

    #[tokio::test]
    async fn http_400_is_upstream_shaped() {
        let v: Value = serde_json::from_str(REAL).unwrap();
        let r = http_response(Some(&v));
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        assert_eq!(r.headers()["content-type"], "application/json");
        let b = axum::body::to_bytes(r.into_body(), 1 << 16).await.unwrap();
        let j: Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(j["error"]["code"], CODE);
        assert_eq!(j["error"]["type"], "invalid_request_error");
        assert_eq!(j["error"]["param"], "input");
        assert_eq!(j["error"]["message"], MESSAGE);
        let lower = MESSAGE.to_ascii_lowercase();
        assert!(!lower.contains("ciallopa") && !lower.contains("proxy"));
    }

    #[test]
    fn ws_frame_keeps_shape() {
        let v: Value = serde_json::from_str(REAL).unwrap();
        let out: Value = serde_json::from_str(&ws_frame(&v)).unwrap();
        assert_eq!(out["type"], "error");
        assert_eq!(out["status"], 400);
        assert_eq!(out["sequence_number"], 2);
        assert_eq!(out["error"]["code"], CODE);
        assert_eq!(out["error"]["param"], "input");
        assert_eq!(out["error"]["message"], MESSAGE);
        let mut with = v.clone();
        with["status"] = 400.into();
        let out: Value = serde_json::from_str(&ws_frame(&with)).unwrap();
        assert_eq!(out["status"], 400);
    }

    #[test]
    fn failed_event_after_output() {
        let v: Value = serde_json::from_str(REAL).unwrap();
        let ev = String::from_utf8(failed_event(Some(&v)).to_vec()).unwrap();
        assert!(ev.starts_with("event: response.failed\ndata: ") && ev.ends_with("\n\n"));
        let j: Value = serde_json::from_str(&sse_data(ev.as_bytes()).unwrap()).unwrap();
        assert_eq!(j["type"], "response.failed");
        assert_eq!(j["response"]["status"], "failed");
        assert_eq!(j["response"]["error"]["code"], CODE);
        assert_eq!(j["response"]["error"]["message"], MESSAGE);
    }
}
