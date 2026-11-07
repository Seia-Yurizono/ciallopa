//! Response decoding for the explicit Responses policy, not a shared-client default.
//!
//! The stream owns the upstream body and decoder. Dropping it cancels both; there is no
//! background reader, response-sized buffer or total response-size limit here. Existing
//! bootstrap, observation and error-body bounds consequently apply to decoded bytes.
use bytes::{Buf, Bytes};
use flate2::{Decompress, FlushDecompress, Status};
use futures::{Stream, StreamExt};
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH, ETAG};
use reqwest::ResponseBuilderExt;
use std::error::Error;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

const OUTPUT_CHUNK: usize = 32 * 1024;
const INPUT_STEP: usize = 8 * 1024;
// Bound work even for long gzip metadata, empty members and always-ready empty chunks.
const STEPS_PER_POLL: usize = 16;

type BoxError = Box<dyn Error + Send + Sync>;
type Upstream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

#[derive(Debug)]
pub(crate) struct UnsupportedEncoding;

impl fmt::Display for UnsupportedEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("upstream response uses an unsupported content encoding")
    }
}

impl Error for UnsupportedEncoding {}

#[derive(Debug)]
enum DecodeError {
    Invalid(flate2::DecompressError),
    Truncated,
    NoProgress,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(_) => f.write_str("invalid gzip response body"),
            Self::Truncated => f.write_str("truncated gzip response body"),
            Self::NoProgress => f.write_str("gzip response decoder made no progress"),
        }
    }
}

impl Error for DecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Invalid(error) => Some(error),
            Self::Truncated | Self::NoProgress => None,
        }
    }
}

/// `reqwest::Body` adds error wrappers. Inspect the chain rather than `is_decode()`,
/// which reqwest also uses for underlying network-body failures.
pub(crate) fn is_decode_error(mut error: &(dyn Error + 'static)) -> bool {
    loop {
        if error.is::<DecodeError>() {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

/// Keep downstream bodies identity-encoded even when upstream ignores our identity
/// preference. An unexpected encoding is rejected before any business interpretation.
pub(crate) fn prepare(
    response: reqwest::Response,
) -> Result<reqwest::Response, UnsupportedEncoding> {
    let status = response.status();
    // These statuses have no payload to decode. In particular a 304's representation
    // headers need not describe an actual gzip member in this response.
    if status.is_informational() || matches!(status.as_u16(), 204 | 205 | 304) {
        return Ok(response);
    }

    let mut encodings = response.headers().get_all(CONTENT_ENCODING).iter();
    let Some(encoding) = encodings.next() else {
        return Ok(response);
    };
    if encodings.next().is_some() {
        return Err(UnsupportedEncoding);
    }
    let encoding = encoding.to_str().map_err(|_| UnsupportedEncoding)?.trim();
    if encoding.eq_ignore_ascii_case("identity") {
        return Ok(response);
    }
    if !encoding.eq_ignore_ascii_case("gzip") {
        return Err(UnsupportedEncoding);
    }

    let url = response.url().clone();
    let raw: http::Response<reqwest::Body> = response.into();
    let (mut parts, body) = raw.into_parts();
    parts.headers.remove(CONTENT_ENCODING);
    parts.headers.remove(CONTENT_LENGTH);
    for header in ["content-md5", "digest", "content-digest", "repr-digest"] {
        parts.headers.remove(header);
    }
    // Weak validators can describe semantically equivalent encoded representations.
    // A strong validator of the compressed octets does not validate the decoded body.
    if parts.headers.get(ETAG).is_some_and(|value| {
        !value
            .to_str()
            .is_ok_and(|value| value.trim_start().starts_with("W/"))
    }) {
        parts.headers.remove(ETAG);
    }

    // Response -> http::Response preserves parts but drops reqwest's separate URL.
    // Restore it through reqwest's public builder extension while retaining all parts.
    let mut url_parts = http::Response::builder().url(url);
    if let Some(extensions) = url_parts.extensions_mut() {
        parts.extensions.extend(std::mem::take(extensions));
    }
    let upstream = reqwest::Response::from(http::Response::new(body))
        .bytes_stream()
        .boxed();
    let decoded = reqwest::Body::wrap_stream(GzipStream::new(upstream));
    Ok(http::Response::from_parts(parts, decoded).into())
}

struct GzipStream {
    upstream: Upstream,
    input: Bytes,
    output: Box<[u8; OUTPUT_CHUNK]>,
    decoder: Decompress,
    // A member ended with a valid CRC and size. More bytes start a new gzip member;
    // only upstream EOF here is successful overall completion.
    between_members: bool,
    eof: bool,
    done: bool,
}

impl GzipStream {
    fn new(upstream: Upstream) -> Self {
        Self {
            upstream,
            input: Bytes::new(),
            output: Box::new([0; OUTPUT_CHUNK]),
            decoder: Decompress::new_gzip(15),
            between_members: false,
            eof: false,
            done: false,
        }
    }

    fn fail(&mut self, error: impl Into<BoxError>) -> Poll<Option<Result<Bytes, BoxError>>> {
        self.done = true;
        Poll::Ready(Some(Err(error.into())))
    }
}

impl Stream for GzipStream {
    type Item = Result<Bytes, BoxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        for _ in 0..STEPS_PER_POLL {
            if this.between_members {
                if !this.input.is_empty() {
                    this.decoder = Decompress::new_gzip(15);
                    this.between_members = false;
                } else if this.eof {
                    this.done = true;
                    return Poll::Ready(None);
                } else {
                    match this.upstream.as_mut().poll_next(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(Ok(input))) => this.input = input,
                        Poll::Ready(Some(Err(error))) => return this.fail(error),
                        Poll::Ready(None) => this.eof = true,
                    }
                    continue;
                }
            }

            let before_in = this.decoder.total_in();
            let before_out = this.decoder.total_out();
            let available = this.input.len().min(INPUT_STEP);
            let status = match this.decoder.decompress(
                &this.input[..available],
                &mut this.output[..],
                FlushDecompress::None,
            ) {
                Ok(status) => status,
                Err(error) => return this.fail(DecodeError::Invalid(error)),
            };
            let consumed = (this.decoder.total_in() - before_in) as usize;
            let produced = (this.decoder.total_out() - before_out) as usize;
            this.input.advance(consumed);
            if status == Status::StreamEnd {
                this.between_members = true;
            }
            if produced != 0 {
                return Poll::Ready(Some(Ok(Bytes::copy_from_slice(&this.output[..produced]))));
            }
            if this.between_members || consumed != 0 {
                continue;
            }
            // This empty-input call drains decoder output before waiting for another
            // network chunk, including after HTTP EOF. Only no progress at EOF is
            // truncation; a pending downstream read never waits for the gzip trailer.
            if this.eof {
                return this.fail(DecodeError::Truncated);
            }
            if !this.input.is_empty() {
                return this.fail(DecodeError::NoProgress);
            }
            match this.upstream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(input))) => this.input = input,
                Poll::Ready(Some(Err(error))) => return this.fail(error),
                Poll::Ready(None) => this.eof = true,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use futures::{channel::mpsc, stream};
    use std::io::Write;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time::Duration;

    fn gzip(body: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }

    fn response(chunks: Vec<Bytes>) -> reqwest::Response {
        let source = stream::iter(chunks.into_iter().map(Ok::<_, std::io::Error>));
        let mut response = http::Response::new(reqwest::Body::wrap_stream(source));
        response
            .headers_mut()
            .insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        response.into()
    }

    #[tokio::test]
    async fn arbitrary_boundaries_decode_concatenated_members_and_preserve_parts() {
        let first = "data: {\"delta\":\"你好\"}\n\n".as_bytes();
        let second = b"data: [DONE]\n\n";
        let mut encoded = gzip(first);
        encoded.extend(gzip(b""));
        encoded.extend(gzip(second));
        let chunks = encoded
            .iter()
            .map(|&byte| Bytes::from(vec![byte]))
            .collect();
        let mut response: http::Response<reqwest::Body> = response(chunks).into();
        *response.status_mut() = http::StatusCode::CREATED;
        *response.version_mut() = http::Version::HTTP_2;
        response
            .headers_mut()
            .insert("x-request-id", "request-1".parse().unwrap());
        response
            .headers_mut()
            .insert(CONTENT_LENGTH, encoded.len().into());
        response
            .headers_mut()
            .insert(ETAG, "\"compressed\"".parse().unwrap());
        response
            .headers_mut()
            .insert("content-digest", "sha-256=:test:".parse().unwrap());
        let response = prepare(response.into()).unwrap();
        assert_eq!(response.status(), http::StatusCode::CREATED);
        assert_eq!(response.version(), http::Version::HTTP_2);
        assert_eq!(response.headers()["x-request-id"], "request-1");
        for header in [
            "content-encoding",
            "content-length",
            "etag",
            "content-digest",
        ] {
            assert!(!response.headers().contains_key(header), "{header}");
        }
        let mut expected = first.to_vec();
        expected.extend(second);
        assert_eq!(response.bytes().await.unwrap(), expected);
    }

    #[tokio::test]
    async fn flushed_output_arrives_before_trailer_or_http_eof() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        let event = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n";
        encoder.write_all(event).unwrap();
        encoder.flush().unwrap();
        let prefix = encoder.get_ref().clone();
        let all = encoder.finish().unwrap();
        let (tx, rx) = mpsc::unbounded::<Result<Bytes, std::io::Error>>();
        tx.unbounded_send(Ok(Bytes::copy_from_slice(&prefix)))
            .unwrap();
        let mut raw = http::Response::new(reqwest::Body::wrap_stream(rx));
        raw.headers_mut()
            .insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let mut decoded = prepare(raw.into()).unwrap().bytes_stream();
        let first = tokio::time::timeout(Duration::from_secs(1), decoded.next())
            .await
            .expect("the first flushed output must not wait for a trailer")
            .unwrap()
            .unwrap();
        assert_eq!(first, &event[..]);
        tx.unbounded_send(Ok(Bytes::copy_from_slice(&all[prefix.len()..])))
            .unwrap();
        drop(tx);
        assert!(decoded.next().await.is_none());
    }

    #[tokio::test]
    async fn decoded_chunks_are_bounded_for_high_expansion_input() {
        let plain = vec![b'x'; 2 * 1024 * 1024 + 7];
        let response = response(vec![Bytes::from(gzip(&plain))]);
        let mut decoded = prepare(response).unwrap().bytes_stream();
        let mut total = 0;
        while let Some(chunk) = decoded.next().await {
            let chunk = chunk.unwrap();
            assert!(chunk.len() <= OUTPUT_CHUNK);
            assert!(chunk.iter().all(|&byte| byte == b'x'));
            total += chunk.len();
        }
        assert_eq!(total, plain.len());
    }

    #[tokio::test]
    async fn bad_crc_truncation_and_trailing_garbage_are_decode_errors() {
        let valid = gzip(b"data: hello\n\n");
        let mut bad_crc = valid.clone();
        let crc = bad_crc.len() - 8;
        bad_crc[crc] ^= 1;
        let mut bad_size = valid.clone();
        let last = bad_size.len() - 1;
        bad_size[last] ^= 1;
        let mut garbage = valid.clone();
        garbage.extend(b"garbage");
        let mut short_member = valid.clone();
        short_member.extend(&gzip(b"next")[..7]);
        for bad in [
            Vec::new(),
            b"not gzip".to_vec(),
            valid[..valid.len() - 1].to_vec(),
            valid[..10].to_vec(),
            bad_crc,
            bad_size,
            garbage,
            short_member,
        ] {
            let error = prepare(response(vec![Bytes::from(bad)]))
                .unwrap()
                .bytes()
                .await
                .unwrap_err();
            assert!(is_decode_error(&error), "{error:?}");
        }
        assert!(prepare(response(vec![Bytes::from(gzip(b""))]))
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn network_read_failures_are_not_reported_as_gzip_corruption() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(b"data: partial\n\n").unwrap();
        encoder.flush().unwrap();
        let source = stream::iter([
            Ok(Bytes::copy_from_slice(encoder.get_ref())),
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "fixture reset",
            )),
        ]);
        let mut raw = http::Response::new(reqwest::Body::wrap_stream(source));
        raw.headers_mut()
            .insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let error = prepare(raw.into()).unwrap().bytes().await.unwrap_err();
        assert!(!is_decode_error(&error));
    }

    #[tokio::test]
    async fn encoding_contract_is_strict_but_bodyless_statuses_do_not_decode() {
        for encoding in ["br", "gzip, identity", "", "gzip, gzip"] {
            let mut raw = response(vec![]);
            raw.headers_mut()
                .insert(CONTENT_ENCODING, encoding.parse().unwrap());
            assert!(prepare(raw).is_err(), "{encoding:?}");
        }
        let mut duplicate = response(vec![]);
        duplicate
            .headers_mut()
            .append(CONTENT_ENCODING, "gzip".parse().unwrap());
        assert!(prepare(duplicate).is_err());
        for encoding in ["GZip", " gzip "] {
            let mut raw = response(vec![Bytes::from(gzip(b"hello"))]);
            raw.headers_mut()
                .insert(CONTENT_ENCODING, encoding.parse().unwrap());
            assert_eq!(prepare(raw).unwrap().bytes().await.unwrap(), "hello");
        }
        for status in [204, 205, 304] {
            let mut raw: http::Response<reqwest::Body> = response(vec![]).into();
            *raw.status_mut() = http::StatusCode::from_u16(status).unwrap();
            assert!(prepare(raw.into())
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .is_empty());
        }
    }

    #[tokio::test]
    async fn identity_and_unencoded_bodies_and_weak_validators_are_preserved() {
        for encoding in [None, Some("identity")] {
            let mut raw = reqwest::Response::from(http::Response::new("hello"));
            if let Some(encoding) = encoding {
                raw.headers_mut()
                    .insert(CONTENT_ENCODING, encoding.parse().unwrap());
            }
            raw.headers_mut()
                .insert(ETAG, "\"original\"".parse().unwrap());
            let decoded = prepare(raw).unwrap();
            assert_eq!(decoded.headers()[ETAG], "\"original\"");
            assert_eq!(decoded.bytes().await.unwrap(), "hello");
        }
        let mut raw = response(vec![Bytes::from(gzip(b"hello"))]);
        raw.headers_mut()
            .insert(ETAG, "W/\"semantic\"".parse().unwrap());
        let decoded = prepare(raw).unwrap();
        assert_eq!(decoded.headers()[ETAG], "W/\"semantic\"");
        assert_eq!(decoded.bytes().await.unwrap(), "hello");
    }

    #[test]
    fn empty_upstream_chunks_yield_with_a_bounded_poll_budget() {
        let source = stream::iter((0..10_000).map(|_| Ok::<_, std::io::Error>(Bytes::new())));
        let raw = reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(source)));
        let mut decoder = GzipStream::new(raw.bytes_stream().boxed());
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut decoder).poll_next(&mut cx).is_pending());
    }

    #[test]
    fn dropping_decoded_body_drops_the_owned_upstream_stream() {
        struct Held(Arc<AtomicBool>);
        impl Stream for Held {
            type Item = Result<Bytes, std::io::Error>;
            fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                Poll::Pending
            }
        }
        impl Drop for Held {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let mut raw = http::Response::new(reqwest::Body::wrap_stream(Held(dropped.clone())));
        raw.headers_mut()
            .insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let response = prepare(raw.into()).unwrap();
        assert!(!dropped.load(Ordering::SeqCst));
        drop(response);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
