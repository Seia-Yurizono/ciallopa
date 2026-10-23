//! Bounded, passive observation of explicit Images SSE errors. No terminal/replay policy.
use ciallopa_usage::detail::CAPTURE_CAP;
use serde_json::Value;

#[derive(Default)]
pub(crate) struct ImageErrors {
    event: Vec<u8>,
    skip: bool,
    line_has_bytes: bool,
    cr: bool,
    done: bool,
}

pub(crate) struct ImageError {
    pub message: String,
    pub event: Vec<u8>,
}

impl ImageErrors {
    /// Inspect without retaining or delaying the original chunk. Oversized events are
    /// skipped through their real empty-line delimiter, including across chunk boundaries.
    pub fn feed(&mut self, chunk: &[u8]) -> Option<ImageError> {
        if self.done {
            return None;
        }
        for &byte in chunk {
            if !self.skip {
                if self.event.len() < CAPTURE_CAP {
                    self.event.push(byte);
                } else {
                    self.event.clear();
                    self.skip = true;
                }
            }
            // CRLF is one line break. A bare CR is also an SSE line break.
            if byte == b'\n' && self.cr {
                self.cr = false;
                continue;
            }
            self.cr = byte == b'\r';
            if matches!(byte, b'\r' | b'\n') {
                if !self.line_has_bytes {
                    if !self.skip {
                        if let Some(message) = error_message(&self.event) {
                            self.done = true;
                            return Some(ImageError {
                                message,
                                event: std::mem::take(&mut self.event),
                            });
                        }
                    }
                    self.event.clear();
                    self.skip = false;
                }
                self.line_has_bytes = false;
            } else {
                self.line_has_bytes = true;
            }
        }
        None
    }
}

fn error_message(event: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(event);
    let mut name = "";
    let mut data = String::new();
    let mut has_data = false;
    for line in text.split(['\r', '\n']) {
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => name = value,
            "data" => {
                if has_data {
                    data.push('\n');
                }
                data.push_str(value);
                has_data = true;
            }
            _ => {}
        }
    }
    let json = serde_json::from_str::<Value>(&data).ok();
    if !has_data
        || (name != "error"
            && json
                .as_ref()
                .and_then(|v| v.get("type"))
                .and_then(Value::as_str)
                != Some("error"))
    {
        return None;
    }
    Some(
        json.as_ref()
            .and_then(|v| v.pointer("/error/message").or_else(|| v.get("message")))
            .and_then(Value::as_str)
            .unwrap_or("upstream image error event")
            .chars()
            .take(4096)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_fields_survive_every_chunk_boundary_and_line_ending() {
        for end in ["\n", "\r\n", "\r"] {
            for event in [
                format!(
                    "event: error{end}data: {{\"error\":{{\"message\":\"failed\"}}}}{end}{end}"
                ),
                format!("data: {{\"type\":\"error\",{end}data: \"message\":\"failed\"}}{end}{end}"),
            ] {
                for split in 0..=event.len() {
                    let mut observer = ImageErrors::default();
                    let error = observer
                        .feed(&event.as_bytes()[..split])
                        .or_else(|| observer.feed(&event.as_bytes()[split..]));
                    assert_eq!(error.map(|e| e.message).as_deref(), Some("failed"));
                }
            }
        }
    }

    #[test]
    fn oversize_skips_the_whole_event_and_resumes_at_its_boundary() {
        let mut observer = ImageErrors::default();
        let big = format!(
            "data: {}\nevent: error\ndata: {{\"type\":\"error\"}}\n",
            "A".repeat(CAPTURE_CAP)
        );
        for chunk in big.as_bytes().chunks(71) {
            assert!(observer.feed(chunk).is_none());
            assert!(observer.event.len() <= CAPTURE_CAP);
        }
        assert!(
            observer.feed(b"\n").is_none(),
            "do not classify the tail of an oversized event"
        );
        assert!(observer
            .feed(b"data: {\"metadata\":{\"type\":\"error\"},\"note\":\"error\"}\n\n")
            .is_none());
        assert!(
            observer.feed(b"event: error\n\n").is_none(),
            "SSE dispatch needs a data field"
        );
        assert!(observer.feed(b"data: {\"type\":\"error\"}\n\n").is_some());
    }
}
