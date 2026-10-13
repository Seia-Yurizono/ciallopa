//! tracing init with a redacting writer: every formatted line passes through
//! `redact()` before hitting stderr, so tokens can't leak even via Debug output.
use regex::Regex;
use std::io::{self, Write};
use std::sync::OnceLock;
use tracing_subscriber::{fmt::MakeWriter, EnvFilter};

fn patterns() -> &'static [(Regex, &'static str)] {
    static P: OnceLock<Vec<(Regex, &'static str)>> = OnceLock::new();
    P.get_or_init(|| {
        [
            // JWTs (access/id tokens)
            (r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}", "<jwt>"),
            // Authorization: Bearer xxx / Basic xxx
            (r"(?i)(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}", "$1 <redacted>"),
            // key=value / "key": "value" forms of sensitive fields
            (
                r#"(?i)("?(?:access_token|refresh_token|id_token|api[_-]?key|x-api-key|authorization|cookie|set-cookie|client_secret|code_verifier|code)"?\s*[:=]\s*"?)[^"\s,&}]{4,}"#,
                "$1<redacted>",
            ),
            // OpenAI-style refresh tokens / api keys
            (r"\b(rt|sk)-[A-Za-z0-9_-]{16,}", "$1-<redacted>"),
        ]
        .into_iter()
        .map(|(r, s)| (Regex::new(r).expect("static regex"), s))
        .collect()
    })
}

pub fn redact(s: &str) -> String {
    let mut out = s.to_string();
    for (re, rep) in patterns() {
        out = re.replace_all(&out, *rep).into_owned();
    }
    out
}

pub struct RedactingWriter<W: Write> {
    inner: W,
    buf: Vec<u8>,
}
impl<W: Write> Write for RedactingWriter<W> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let s = redact(&String::from_utf8_lossy(&self.buf));
            self.inner.write_all(s.as_bytes())?;
            self.buf.clear();
        }
        self.inner.flush()
    }
}
impl<W: Write> Drop for RedactingWriter<W> {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

pub struct MakeRedacting;
impl<'a> MakeWriter<'a> for MakeRedacting {
    type Writer = RedactingWriter<io::Stderr>;
    fn make_writer(&'a self) -> Self::Writer {
        RedactingWriter {
            inner: io::stderr(),
            buf: Vec::with_capacity(256),
        }
    }
}

/// `RUST_LOG` overrides `level`.
pub fn init(level: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(MakeRedacting)
        .with_ansi(false)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::redact;

    #[test]
    fn redacts_secrets() {
        let jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJlc2lnbmF0dXJl";
        let s = format!(
            "authorization: Bearer {jwt} refresh_token=rt-abcdefghijklmnopqrstuv {{\"access_token\": \"zzzzzzzz\"}} x-api-key: sk-cpa-0123456789abcdef"
        );
        let r = redact(&s);
        assert!(!r.contains(jwt), "{r}");
        assert!(!r.contains("abcdefghijklmnop"), "{r}");
        assert!(!r.contains("zzzzzzzz"), "{r}");
        assert!(!r.contains("0123456789abcdef"), "{r}");
        assert_eq!(
            redact("model=gpt-5 path=/backend-api/codex/responses"),
            "model=gpt-5 path=/backend-api/codex/responses"
        );
    }
}
