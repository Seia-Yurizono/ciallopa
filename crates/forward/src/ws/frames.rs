use crate::usage::Tracker;
use axum::response::Response;
use ciallopa_upstream::WsMessage;

/// Both legs are tungstenite streams; raw frames are never relayed.
pub(super) type Message = WsMessage;

pub(super) fn to_up(m: Message) -> Option<WsMessage> {
    match m {
        WsMessage::Frame(_) => None,
        m => Some(m),
    }
}

pub(super) fn to_down(m: WsMessage) -> Option<Message> {
    to_up(m)
}

/// Answer to a client message refused by the buffered-body budget: the same 503
/// `gateway_busy` wording and `Retry-After` as HTTP ([`crate::admission::BUSY_RETRY_AFTER_SECS`]),
/// as an upstream-style wrapped error event (Codex maps it to a retryable HTTP 503). The
/// message is not forwarded and no turn starts; the connection stays open.
pub(super) fn busy_frame() -> Message {
    let secs = crate::admission::BUSY_RETRY_AFTER_SECS;
    Message::text(
        serde_json::json!({
            "type": "error",
            "status": 503,
            "error": {"type": "gateway_busy", "message": "Gateway is busy; retry shortly."},
            "headers": {
                "retry-after": secs.to_string(),
                "retry-after-ms": (secs.saturating_mul(1000)).to_string(),
            },
        })
        .to_string(),
    )
}

/// A local error response (e.g. every account cooling down for the turn's model) as the
/// upstream-style wrapped error event, same shape as [`busy_frame`]. Records it on `turn`.
pub(super) async fn error_event(resp: Response, turn: Option<Tracker>) -> Message {
    let (parts, body) = resp.into_parts();
    let b = axum::body::to_bytes(body, 1 << 20)
        .await
        .unwrap_or_default();
    if let Some(mut t) = turn {
        t.status(parts.status.as_u16());
        t.finish_response();
        t.error_headers(&parts.headers);
        if let Some(m) = parts.extensions.get::<crate::errors::ErrMeta>() {
            t.meta_err(m);
        }
        t.error_body(&b);
    }
    let error = serde_json::from_slice::<serde_json::Value>(&b)
        .ok()
        .and_then(|v| v.get("error").cloned())
        .unwrap_or_else(
            || serde_json::json!({"type": "unavailable", "message": "Service unavailable."}),
        );
    let mut headers = serde_json::Map::new();
    for k in ["retry-after", "retry-after-ms"] {
        if let Some(v) = parts.headers.get(k).and_then(|v| v.to_str().ok()) {
            headers.insert(k.to_string(), v.into());
        }
    }
    let mut ev =
        serde_json::json!({"type": "error", "status": parts.status.as_u16(), "error": error});
    if !headers.is_empty() {
        ev["headers"] = serde_json::Value::Object(headers);
    }
    Message::text(ev.to_string())
}

/// A client `response.create` that may be replayed on another account.
/// Top-level fields of a client message that replay and usage read. `input`, `tools` and
/// other large fields are skipped while parsing, so a large message is not materialised
/// again as a JSON tree.
#[derive(serde::Deserialize)]
pub(super) struct Head {
    #[serde(rename = "type")]
    ty: Option<serde_json::Value>,
    previous_response_id: Option<serde_json::Value>,
    generate: Option<serde_json::Value>,
    service_tier: Option<serde_json::Value>,
    client_metadata: Option<serde_json::Value>,
    pub(super) model: Option<serde_json::Value>,
    reasoning: Option<serde_json::Value>,
}

impl Head {
    pub(super) fn parse(text: &str) -> Option<Head> {
        serde_json::from_str(text).ok()
    }

    pub(super) fn to_value(&self) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        for (k, v) in [
            ("type", &self.ty),
            ("previous_response_id", &self.previous_response_id),
            ("generate", &self.generate),
            ("service_tier", &self.service_tier),
            ("client_metadata", &self.client_metadata),
            ("model", &self.model),
            ("reasoning", &self.reasoning),
        ] {
            if let Some(v) = v {
                m.insert(k.to_string(), v.clone());
            }
        }
        serde_json::Value::Object(m)
    }
}

pub(crate) fn replayable(text: &str) -> bool {
    Head::parse(text).is_some_and(|h| {
        h.ty.as_ref().and_then(|t| t.as_str()) == Some("response.create")
            && h.previous_response_id
                .as_ref()
                .and_then(|p| p.as_str())
                .is_none_or(|p| p.trim().is_empty())
    })
}
