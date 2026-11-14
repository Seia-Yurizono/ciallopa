use bytes::{Bytes, BytesMut};
use std::borrow::Cow;

/// Complete events and fragments of events that exceeded the observation limit.
#[derive(Debug)]
pub(crate) enum Frame {
    Event(Bytes),
    Oversize(Bytes),
}

/// One incremental delimiter scanner shared by the streaming and borrowed-buffer views.
struct Scanner {
    line_len: u8,
    cr: bool,
    bare_cr: bool,
}

impl Scanner {
    fn new(bare_cr: bool) -> Self {
        Self {
            line_len: u8::from(!bare_cr),
            cr: false,
            bare_cr,
        }
    }

    fn scan(&mut self, bytes: &[u8]) -> Option<usize> {
        for (i, &byte) in bytes.iter().enumerate() {
            if self.bare_cr && byte == b'\n' && self.cr {
                self.cr = false;
                continue;
            }
            let newline = byte == b'\n' || (self.bare_cr && byte == b'\r');
            let empty = self.line_len == 0 || (!self.bare_cr && self.line_len == 1 && self.cr);
            self.cr = byte == b'\r';
            if newline {
                self.line_len = 0;
                if empty {
                    // The LF view retains the old two-line separator grouping.
                    self.line_len = u8::from(!self.bare_cr);
                    return Some(i + 1);
                }
            } else {
                self.line_len = self.line_len.saturating_add(1).min(2);
            }
        }
        None
    }
}

pub(crate) struct Framer {
    tail: BytesMut,
    scanner: Scanner,
    limit: usize,
    oversized: bool,
}

impl Framer {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            tail: BytesMut::new(),
            scanner: Scanner::new(false),
            limit,
            oversized: false,
        }
    }

    pub(crate) fn image_errors(limit: usize) -> Self {
        Self {
            scanner: Scanner::new(true),
            ..Self::new(limit)
        }
    }

    /// Complete events contained in one chunk borrow its allocation. Only fragmented
    /// events need an append; oversized events stay unclassifiable until their delimiter.
    pub(crate) fn push(&mut self, mut chunk: Bytes) -> impl Iterator<Item = Frame> + '_ {
        std::iter::from_fn(move || {
            while !chunk.is_empty() {
                let take = if self.oversized {
                    chunk.len()
                } else {
                    chunk
                        .len()
                        .min(self.limit.saturating_sub(self.tail.len()).saturating_add(1))
                };
                let boundary = self.scanner.scan(&chunk[..take]);
                let part = chunk.split_to(boundary.unwrap_or(take));
                let oversized =
                    self.oversized || self.tail.len().saturating_add(part.len()) > self.limit;
                if boundary.is_some() || oversized {
                    let bytes = if self.tail.is_empty() {
                        part
                    } else {
                        self.tail.extend_from_slice(&part);
                        self.tail.split().freeze()
                    };
                    self.oversized = oversized && boundary.is_none();
                    return Some(if oversized {
                        Frame::Oversize(bytes)
                    } else {
                        Frame::Event(bytes)
                    });
                }
                self.tail.extend_from_slice(&part);
            }
            None
        })
    }

    pub(crate) fn finish(&mut self) -> Option<Bytes> {
        let tail = (!self.tail.is_empty()).then(|| self.tail.split().freeze());
        self.scanner = Scanner::new(self.scanner.bare_cr);
        self.oversized = false;
        tail
    }

    /// Borrow complete events from an existing prelude/observation buffer without copying.
    fn events_after(buf: &[u8], from: usize, search_from: usize) -> impl Iterator<Item = &[u8]> {
        let mut scanner = Scanner::new(false);
        let mut pos = from;
        // At most three preceding bytes recover a partial LF/CRLF separator.
        let mut search = search_from.max(from).saturating_sub(3).max(from);
        std::iter::from_fn(move || {
            let end = search + scanner.scan(&buf[search..])?;
            let event = &buf[pos..end];
            pos = end;
            search = end;
            Some(event)
        })
    }
}

/// Forward complete SSE events, replacing only encrypted-content rejections.
pub(crate) struct Aligner {
    framer: Option<Framer>,
}

impl Aligner {
    pub(crate) fn new(sse: bool) -> Self {
        Self {
            framer: sse.then(|| Framer::new(BOOTSTRAP_MAX)),
        }
    }

    pub(crate) fn push(&mut self, chunk: Bytes) -> Bytes {
        let Some(framer) = self.framer.as_mut() else {
            return chunk;
        };
        let original = framer.tail.is_empty().then(|| chunk.clone());
        let mut parts = Vec::new();
        let mut ready = 0;
        let mut rewritten = false;
        for frame in framer.push(chunk) {
            let bytes = match frame {
                Frame::Event(event) => {
                    let replacement = rewrite_rejection(&event);
                    rewritten |= replacement.is_some();
                    replacement.unwrap_or(event)
                }
                Frame::Oversize(bytes) => bytes,
            };
            ready += bytes.len();
            parts.push(bytes);
        }
        if !rewritten {
            if let Some(original) = original {
                return original.slice(..ready);
            }
        }
        if parts.len() == 1 {
            return parts.pop().unwrap_or_default();
        }
        let mut out = BytesMut::with_capacity(ready);
        for part in parts {
            out.extend_from_slice(&part);
        }
        out.freeze()
    }

    pub(crate) fn finish(&mut self) -> Option<Bytes> {
        let tail = self.framer.as_mut()?.finish()?;
        Some(rewrite_rejection(&tail).unwrap_or(tail))
    }
}

fn rewrite_rejection(event: &[u8]) -> Option<Bytes> {
    let data = data(event)?;
    if !crate::encrypted::maybe_rejection(&data) {
        return None;
    }
    // Output may quote an error message; only error envelopes are candidates.
    let ty = crate::usage::event_type(&data);
    if !matches!(ty.as_deref(), None | Some("" | "error" | "response.failed")) {
        return None;
    }
    let v = serde_json::from_str::<serde_json::Value>(&data).ok()?;
    crate::encrypted::rejection(&v)?;
    Some(crate::encrypted::failed_event(Some(&v)))
}

/// Max bytes held while waiting for the first meaningful SSE event.
pub(crate) const BOOTSTRAP_MAX: usize = 1 << 20;

pub(crate) fn sse_events(buf: &[u8], from: usize) -> (Vec<Cow<'_, str>>, usize) {
    sse_events_after(buf, from, from)
}

pub(crate) fn sse_events_after(
    buf: &[u8],
    from: usize,
    search_from: usize,
) -> (Vec<Cow<'_, str>>, usize) {
    let mut used = from;
    let events = Framer::events_after(buf, from, search_from)
        .map(|event| {
            used += event.len();
            match String::from_utf8_lossy(event) {
                Cow::Borrowed(event) => data_text(event).unwrap_or_default(),
                Cow::Owned(event) => Cow::Owned(data_text(&event).unwrap_or_default().into_owned()),
            }
        })
        .collect();
    (events, used)
}

pub(crate) fn data(event: &[u8]) -> Option<Cow<'_, str>> {
    data_text(std::str::from_utf8(event).ok()?)
}

fn data_text(event: &str) -> Option<Cow<'_, str>> {
    let mut lines = event
        .split(['\r', '\n'])
        .filter_map(|line| {
            line.strip_prefix("data:")
                .or_else(|| (line == "data").then_some(""))
        })
        .map(|data| data.strip_prefix(' ').unwrap_or(data));
    let first = lines.next()?;
    let Some(second) = lines.next() else {
        return Some(Cow::Borrowed(first));
    };
    let mut text = first.to_owned();
    for line in std::iter::once(second).chain(lines) {
        text.push('\n');
        text.push_str(line);
    }
    Some(Cow::Owned(text))
}

pub(crate) fn memfind(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

#[cfg(test)]
mod quota_tests {
    use super::*;

    #[test]
    fn framer_events_are_independent_of_chunk_splits() {
        let events = [
            ": comment\r\nevent: message\r\ndata: {\"type\":\"response.output_text.delta\",\r\ndata: \"delta\":\"你好\"}\r\n\r\n",
            "data: {\"type\":\"response.done\"}\n\n",
            "data: [DONE]\n\n",
        ];
        let wire = events.concat() + "data: unfinished";
        let bytes = Bytes::from(wire);
        let check = |cuts: &[usize]| {
            let mut framer = Framer::new(usize::MAX);
            let mut actual = Vec::new();
            let mut pending = Vec::new();
            let mut observed = Vec::new();
            let mut start = 0;
            for end in cuts.iter().copied().chain(std::iter::once(bytes.len())) {
                for frame in framer.push(bytes.slice(start..end)) {
                    match frame {
                        Frame::Event(event) => actual.push(event),
                        Frame::Oversize(_) => panic!("unbounded framer"),
                    }
                }
                let search_from = pending.len().saturating_sub(3);
                pending.extend_from_slice(&bytes[start..end]);
                let (data, used) = sse_events_after(&pending, 0, search_from);
                observed.extend(data.into_iter().map(Cow::into_owned));
                pending.drain(..used);
                start = end;
            }
            assert_eq!(
                observed,
                events
                    .iter()
                    .map(|event| data(event.as_bytes()).unwrap().into_owned())
                    .collect::<Vec<_>>()
            );
            assert_eq!(pending, b"data: unfinished");
            assert_eq!(
                actual.iter().map(|event| &event[..]).collect::<Vec<_>>(),
                events
                    .iter()
                    .map(|event| event.as_bytes())
                    .collect::<Vec<_>>()
            );
            assert_eq!(framer.finish().as_deref(), Some(&b"data: unfinished"[..]));
        };
        // Exhaust all one/two-cut partitions, including cuts inside UTF-8 and CRLF.
        for first in 0..=bytes.len() {
            for second in first..=bytes.len() {
                check(&[first, second]);
            }
        }
        // Also generate arbitrary partitions with many independently placed boundaries.
        for seed in 1..=256u64 {
            let mut state = seed;
            let cuts: Vec<_> = (0..bytes.len())
                .filter(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state % 3 == 0
                })
                .collect();
            check(&cuts);
        }
    }

    #[test]
    fn oversized_fragments_forward_exact_bytes_and_resume_only_at_delimiter() {
        let wire =
            Bytes::from_static(b"data: 12345678901234567890\r\nevent: error\r\n\r\ndata: ok\n\n");
        for split in 0..=wire.len() {
            let mut framer = Framer::new(16);
            let mut forwarded = Vec::new();
            let mut events = Vec::new();
            for chunk in [wire.slice(..split), wire.slice(split..)] {
                for frame in framer.push(chunk) {
                    match frame {
                        Frame::Event(event) => {
                            forwarded.extend_from_slice(&event);
                            events.push(event);
                        }
                        Frame::Oversize(bytes) => forwarded.extend_from_slice(&bytes),
                    }
                }
                assert!(framer.tail.len() <= 16);
            }
            assert_eq!(forwarded, wire);
            assert_eq!(events, [Bytes::from_static(b"data: ok\n\n")]);
            assert!(framer.finish().is_none());
        }
    }

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
        for cut in 0..=all.len() {
            let mut a = Aligner::new(true);
            let mut out = a
                .push(Bytes::copy_from_slice(&all.as_bytes()[..cut]))
                .to_vec();
            out.extend_from_slice(&a.push(Bytes::copy_from_slice(&all.as_bytes()[cut..])));
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
        assert!(a.push(Bytes::from_static(b"data: {\"a\":1}")).is_empty());
        assert_eq!(
            &a.push(Bytes::from_static(b"\n\ndata: x"))[..],
            b"data: {\"a\":1}\n\n"
        );
        assert_eq!(&a.finish().unwrap()[..], b"data: x");
        let mut a = Aligner::new(false);
        assert_eq!(&a.push(Bytes::from_static(b"partial"))[..], b"partial");
        assert!(a.finish().is_none());
    }
}
