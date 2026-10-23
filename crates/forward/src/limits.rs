//! Small bounds for responses that must be buffered before forwarding or caching.
use crate::errors;
use axum::response::Response;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use http::{HeaderMap, StatusCode};
use std::time::Duration;

pub(crate) const ERROR_BODY_LIMIT: usize = 1024 * 1024;
pub(crate) const MODELS_BODY_LIMIT: usize = 8 * 1024 * 1024;
pub(crate) const BODY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
enum Failure {
    Timeout,
    TooLarge,
    Read,
}

#[derive(Debug)]
pub(crate) struct BodyFailure {
    failure: Failure,
    /// Only a small prefix survives a failed read, for the redacted attempt log.
    pub prefix: Bytes,
    limit: usize,
}

impl BodyFailure {
    pub fn code(&self, models: bool) -> &'static str {
        match (&self.failure, models) {
            (Failure::Timeout, false) => "error_body_timeout",
            (Failure::TooLarge, false) => "error_body_too_large",
            (Failure::Read, false) => "error_body_read",
            (Failure::Timeout, true) => "models_body_timeout",
            (Failure::TooLarge, true) => "models_body_too_large",
            (Failure::Read, true) => "models_body_read",
        }
    }

    pub fn response(&self, status: u16, headers: &HeaderMap, models: bool) -> Response {
        let timeout = matches!(self.failure, Failure::Timeout);
        let mut r = errors::local(
            if timeout {
                StatusCode::GATEWAY_TIMEOUT
            } else {
                StatusCode::BAD_GATEWAY
            },
            if matches!(self.failure, Failure::TooLarge) {
                errors::SRC_LOCAL
            } else {
                errors::SRC_NETWORK
            },
            self.code(models),
            None,
        );
        if let Some(meta) = r.extensions_mut().get_mut::<errors::ErrMeta>() {
            meta.upstream_status = Some(status);
            meta.upstream_request_id = errors::request_id(headers);
            let reason = match self.failure {
                Failure::Timeout => {
                    "reading the body exceeded its absolute time budget".to_string()
                }
                Failure::TooLarge => format!("body exceeded the {} byte limit", self.limit),
                Failure::Read => "body transfer failed".to_string(),
            };
            meta.message = Some(format!(
                "upstream HTTP {status}: {reason}; body incomplete and not forwarded"
            ));
        }
        r
    }
}

/// The deadline is absolute: a slow stream of error bytes cannot keep extending it.
pub(crate) async fn read_body(
    response: reqwest::Response,
    limit: usize,
    timeout: Duration,
) -> Result<Bytes, BodyFailure> {
    let mut stream = response.bytes_stream();
    let mut body = BytesMut::new();
    let read = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| Failure::Read)?;
            if chunk.len() > limit.saturating_sub(body.len()) {
                // Keep a bounded prefix even when the first chunk already exceeds the cap.
                let room = limit.saturating_sub(body.len());
                body.extend_from_slice(&chunk[..room]);
                return Err(Failure::TooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(())
    };
    let result = tokio::time::timeout(timeout, read)
        .await
        .unwrap_or(Err(Failure::Timeout));
    match result {
        Ok(()) => Ok(body.freeze()),
        Err(failure) => Err(BodyFailure {
            failure,
            prefix: Bytes::copy_from_slice(&body[..body.len().min(512)]),
            limit,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cap_keeps_complete_bodies_and_marks_incomplete_errors() {
        let exact = reqwest::Response::from(http::Response::new(Bytes::from_static(b"1234")));
        assert_eq!(read_body(exact, 4, BODY_TIMEOUT).await.unwrap(), "1234");
        let over = reqwest::Response::from(http::Response::new(Bytes::from_static(b"12345")));
        let error = read_body(over, 4, BODY_TIMEOUT).await.unwrap_err();
        assert_eq!(error.prefix, "1234");
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "up-123".parse().unwrap());
        let response = error.response(429, &headers, false);
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let meta = response.extensions().get::<errors::ErrMeta>().unwrap();
        assert_eq!(meta.code, "error_body_too_large");
        assert_eq!(meta.upstream_status, Some(429));
        assert_eq!(meta.upstream_request_id.as_deref(), Some("up-123"));
        assert!(meta.message.as_ref().unwrap().contains("body incomplete"));
    }

    #[tokio::test]
    async fn continuous_body_bytes_do_not_extend_the_absolute_read_deadline() {
        let stream = futures::stream::unfold(0, |n| async move {
            if n > 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Some((Ok::<_, std::io::Error>(Bytes::from_static(b"x")), n + 1))
        });
        let response =
            reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(stream)));
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            read_body(response, ERROR_BODY_LIMIT, Duration::from_millis(40)),
        )
        .await
        .expect("continuous bytes must not keep the read alive")
        .unwrap_err();
        assert_eq!(error.code(false), "error_body_timeout");
        assert!(!error.prefix.is_empty());
    }

    #[tokio::test]
    async fn broken_body_is_not_silently_returned_as_an_empty_error() {
        let stream = futures::stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(std::io::Error::other("connection reset")),
        ]);
        let response =
            reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(stream)));
        let error = read_body(response, ERROR_BODY_LIMIT, BODY_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(error.code(false), "error_body_read");
        assert_eq!(error.prefix, "partial");
    }
}
