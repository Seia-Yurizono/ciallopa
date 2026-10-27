//! tracing init with a redacting writer: every formatted line passes through
//! `redact()` before hitting stderr, so tokens can't leak even via Debug output.
use regex::Regex;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use tracing_subscriber::{fmt::MakeWriter, EnvFilter};

static REDACT_EMAILS: AtomicBool = AtomicBool::new(true);

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
                r#"(?i)("?(?:access_token|refresh_token|id_token|api[_-]?key|x-api-key|authorization|cookie|set-cookie|client_secret|code_verifier)"?\s*[:=]\s*"?)[^"\s,&}]{4,}"#,
                "$1<redacted>",
            ),
            // OAuth authorization codes are long; short `code` values are error / transport
            // codes (`proxy_connect`, `server_is_overloaded`) and stay readable.
            (
                r#"(?i)(\bcode"?\s*[:=]\s*"?)[^"\s,&}]{32,}"#,
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

fn email_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        Regex::new(r"(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b").expect("email regex")
    })
}

pub fn redact(s: &str) -> String {
    let mut out = s.to_string();
    for (re, rep) in patterns() {
        out = re.replace_all(&out, *rep).into_owned();
    }
    if REDACT_EMAILS.load(Ordering::Relaxed) {
        out = email_re().replace_all(&out, "[email]").into_owned();
    }
    out
}

/// Hot-reload `[log] redact_emails` (default on).
pub fn set_redact_emails(on: bool) {
    REDACT_EMAILS.store(on, Ordering::Relaxed);
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

type ReloadHandle = tracing_subscriber::reload::Handle<EnvFilter, tracing_subscriber::Registry>;
static HANDLE: OnceLock<ReloadHandle> = OnceLock::new();

/// `RUST_LOG` overrides `level` (then [`set_level`] leaves the filter alone).
pub fn init(level: &str) {
    use tracing_subscriber::prelude::*;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let (layer, handle) = tracing_subscriber::reload::Layer::new(filter);
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(MakeRedacting)
        .with_ansi(false);
    if tracing_subscriber::registry()
        .with(layer)
        .with(fmt)
        .try_init()
        .is_ok()
    {
        let _ = HANDLE.set(handle);
    }
}

/// Valid `[log] level` (an EnvFilter directive such as `info` or `ciallopa=debug`).
pub fn validate_level(level: &str) -> Result<(), String> {
    EnvFilter::try_new(level)
        .map(|_| ())
        .map_err(|e| format!("log.level: {e}"))
}

/// Hot-reload the log level. No-op when `RUST_LOG` is set or logging was not initialised here.
pub fn set_level(level: &str) -> Result<(), String> {
    if std::env::var_os("RUST_LOG").is_some() {
        return Ok(());
    }
    let f = EnvFilter::try_new(level).map_err(|e| format!("log.level: {e}"))?;
    match HANDLE.get() {
        Some(h) => h.reload(f).map_err(|e| e.to_string()),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::{redact, set_redact_emails};

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

    #[test]
    fn short_codes_stay_readable() {
        assert_eq!(
            redact("code=proxy_connect code=\"server_is_overloaded\""),
            "code=proxy_connect code=\"server_is_overloaded\""
        );
        let oauth = "ac_AbCdEfGhIjKlMnOpQrStUvWxYz0123456789abcdef";
        let r = redact(&format!(
            "/auth/callback?code={oauth}&state=x {{\"code\": \"{oauth}\"}}"
        ));
        assert!(!r.contains(oauth), "{r}");
    }


    #[test]
    fn redacts_emails_when_enabled() {
        set_redact_emails(true);
        let r = redact("account=codex-deadbeef-alice@example.com-plus user=bob.carol+tag@x.io");
        assert!(!r.contains("alice@example.com") && !r.contains("bob.carol+tag@x.io"), "{r}");
        assert!(r.contains("[email]"), "{r}");
        set_redact_emails(false);
        let r = redact("alice@example.com");
        assert!(r.contains("alice@example.com"), "{r}");
        set_redact_emails(true); // restore default for other tests
    }
}
