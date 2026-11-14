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

/// Refuse the message with retryable 503; keep WS open without starting a turn.
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
        t.record_response(&parts, &b);
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

/// Read replay/usage fields without materializing large input/tool payloads.
#[derive(serde::Deserialize, serde::Serialize)]
pub(super) struct Head {
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    ty: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous_response_id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generate: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    service_tier: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) model: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<serde_json::Value>,
}

impl Head {
    pub(super) fn parse(text: &str) -> Option<Head> {
        serde_json::from_str(text).ok()
    }

    pub(super) fn replayable(&self) -> bool {
        self.ty.as_ref().and_then(|t| t.as_str()) == Some("response.create")
            && self
                .previous_response_id
                .as_ref()
                .and_then(|p| p.as_str())
                .is_none_or(|p| p.trim().is_empty())
    }

    pub(super) fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("JSON envelope fields serialize")
    }
}

#[cfg(test)]
pub(crate) fn replayable(text: &str) -> bool {
    Head::parse(text).is_some_and(|h| h.replayable())
}
