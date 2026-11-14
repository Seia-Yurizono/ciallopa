use bytes::Bytes;
use std::borrow::Cow;

/// Forwards an SSE body in whole events, so an upstream encrypted-content rejection that
/// arrives after output can be replaced by a `response.failed` carrying the clear message
/// (`encrypted::failed_event`) before any of its bytes leave. Everything else is forwarded
/// unchanged; only the incomplete tail of the current event is held (Codex cannot use it
/// before it completes anyway). An event larger than `BOOTSTRAP_MAX` is flushed as is.
/// Non-SSE bodies pass straight through.
pub(crate) struct Aligner {
    sse: bool,
    tail: Vec<u8>,
}

impl Aligner {
    pub(crate) fn new(sse: bool) -> Self {
        Self {
            sse,
            tail: Vec::new(),
        }
    }

    /// Bytes ready to send after `chunk` (possibly empty).
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Bytes {
        if !self.sse {
            return Bytes::copy_from_slice(chunk);
        }
        self.tail.extend_from_slice(chunk);
        match last_boundary(&self.tail) {
            Some(end) => {
                let rest = self.tail.split_off(end);
                let ready = std::mem::replace(&mut self.tail, rest);
                rewrite_rejections(ready)
            }
            None if self.tail.len() > BOOTSTRAP_MAX => Bytes::from(std::mem::take(&mut self.tail)),
            None => Bytes::new(),
        }
    }

    /// Whatever is still held (an unterminated last event), at end of stream.
    pub(crate) fn finish(&mut self) -> Option<Bytes> {
        (!self.tail.is_empty()).then(|| rewrite_rejections(std::mem::take(&mut self.tail)))
    }
}

/// End (exclusive, separator included) of the last complete SSE event in `b`.
fn last_boundary(b: &[u8]) -> Option<usize> {
    let mut i = b.len();
    while i >= 2 {
        if b[i - 1] == b'\n' {
            if b[i - 2] == b'\n' {
                return Some(i);
            }
            if i >= 3 && b[i - 2] == b'\r' && b[i - 3] == b'\n' {
                return Some(i);
            }
        }
        i -= 1;
    }
    None
}

/// Replace encrypted-content rejection events in complete SSE bytes (cheap byte check first).
fn rewrite_rejections(ready: Vec<u8>) -> Bytes {
    let maybe = memfind(&ready, crate::encrypted::CODE.as_bytes())
        || memfind(&ready, b"ncrypted content")
        || memfind(&ready, b"thinking_signature_invalid");
    if !maybe {
        return Bytes::from(ready);
    }
    let mut out = Vec::with_capacity(ready.len());
    let mut pos = 0;
    while pos < ready.len() {
        let (end, sep) = find_sep(&ready[pos..]).unwrap_or((ready.len() - pos, 0));
        let ev = &ready[pos..pos + end];
        let hit = crate::encrypted::sse_data(ev).and_then(|d| {
            crate::encrypted::rejection_text_json(&d)
                .and_then(|_| serde_json::from_str::<serde_json::Value>(&d).ok())
        });
        match hit {
            Some(v) => out.extend_from_slice(&crate::encrypted::failed_event(Some(&v))),
            None => out.extend_from_slice(&ready[pos..pos + end + sep]),
        }
        pos += end + sep;
    }
    Bytes::from(out)
}

/// Max bytes held while waiting for the first meaningful SSE event.
pub(crate) const BOOTSTRAP_MAX: usize = 1 << 20;

/// Split complete SSE events off the front of `buf`; yields each event's joined `data:`.
pub(crate) fn sse_events(buf: &[u8], from: usize) -> (Vec<Cow<'_, str>>, usize) {
    sse_events_after(buf, from, from)
}

/// `from` is the unconsumed event's start; `search_from` skips its previously searched
/// prefix. They differ only for an unfinished event carried across chunks.
pub(crate) fn sse_events_after(
    buf: &[u8],
    from: usize,
    search_from: usize,
) -> (Vec<Cow<'_, str>>, usize) {
    let mut out = Vec::new();
    let mut pos = from;
    let mut search = search_from.max(from);
    loop {
        let rest = &buf[search..];
        let Some((end, sep)) = find_sep(rest) else {
            break;
        };
        let end = search + end;
        // The usual single data line borrows the existing event buffer. Multi-line SSE
        // needs joining, but verifying the type never builds a second large JSON Value.
        out.push(match String::from_utf8_lossy(&buf[pos..end]) {
            Cow::Borrowed(ev) => sse_data(ev),
            Cow::Owned(ev) => Cow::Owned(sse_data(&ev).into_owned()),
        });
        pos = end + sep;
        search = pos;
    }
    (out, pos)
}

fn sse_data(ev: &str) -> Cow<'_, str> {
    let mut lines = ev
        .lines()
        .filter_map(|l| l.strip_prefix("data:"))
        .map(|d| d.strip_prefix(' ').unwrap_or(d));
    let first = lines.next().unwrap_or("");
    let Some(second) = lines.next() else {
        return Cow::Borrowed(first);
    };
    let mut text = first.to_string();
    for line in std::iter::once(second).chain(lines) {
        text.push('\n');
        text.push_str(line);
    }
    Cow::Owned(text)
}

fn find_sep(b: &[u8]) -> Option<(usize, usize)> {
    let a = b.windows(2).position(|w| w == b"\n\n").map(|i| (i, 2));
    let c = b.windows(4).position(|w| w == b"\r\n\r\n").map(|i| (i, 4));
    match (a, c) {
        (Some(x), Some(y)) => Some(if x.0 <= y.0 { x } else { y }),
        (x, y) => x.or(y),
    }
}

pub(crate) fn memfind(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

#[cfg(test)]
mod quota_tests {
    use super::*;

    #[test]
    fn sse_types_survive_fragmented_crlf_and_multiline_data() {
        let wire = concat!(
            ": comment\r\nevent: response.failed\r\n",
            "data: {\"metadata\":{\"type\":\"response.failed\"},\r\n",
            "data: \"type\": \"response.output_text.delta\",\"delta\":\"response.completed\"}\r\n\r\n",
            "data: { \"type\": \"response.done\" }\n\n"
        );
        let mut pending = Vec::new();
        let mut kinds = Vec::new();
        for b in wire.bytes() {
            let search_from = pending.len().saturating_sub(3);
            pending.push(b);
            let (events, used) = sse_events_after(&pending, 0, search_from);
            for event in events {
                kinds.push(crate::usage::event_type(&event).unwrap().into_owned());
            }
            pending.drain(..used);
        }
        assert!(pending.is_empty());
        assert_eq!(kinds, ["response.output_text.delta", "response.done"]);
        let (events, _) = sse_events(b"data: {\"type\":\"response.completed\"}\n\n", 0);
        assert!(
            matches!(events[0], Cow::Borrowed(_)),
            "single data lines borrow the event buffer"
        );
    }

    #[test]
    fn aligner_rewrites_split_rejection_only() {
        let ev = concat!(
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"The encrypted content for item rs_1 could not be verified.\",",
            "\"type\":\"invalid_request_error\",\"param\":\"input\",\"code\":\"invalid_encrypted_content\"}}\n\n"
        );
        let head = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"invalid_encrypted_content\"}\r\n\r\n";
        let all = format!("{head}{ev}");
        for cut in [1, head.len() - 1, head.len() + 5, all.len() - 1] {
            let mut a = Aligner::new(true);
            let mut out = a.push(&all.as_bytes()[..cut]).to_vec();
            out.extend_from_slice(&a.push(&all.as_bytes()[cut..]));
            if let Some(r) = a.finish() {
                out.extend_from_slice(&r);
            }
            let out = String::from_utf8(out).unwrap();
            assert!(out.starts_with(head), "{cut}: {out}");
            assert!(
                !out.contains("could not be verified") && !out.contains("event: error"),
                "{out}"
            );
            assert!(
                out.contains("event: response.failed")
                    && out.contains(crate::encrypted::MESSAGE.split(' ').next().unwrap())
            );
        }
        // Partial events are held; whole ones flow unchanged; non-SSE is untouched.
        let mut a = Aligner::new(true);
        assert!(a.push(b"data: {\"a\":1}").is_empty());
        assert_eq!(&a.push(b"\n\ndata: x")[..], b"data: {\"a\":1}\n\n");
        assert_eq!(&a.finish().unwrap()[..], b"data: x");
        let mut a = Aligner::new(false);
        assert_eq!(&a.push(b"partial")[..], b"partial");
        assert!(a.finish().is_none());
    }
}
