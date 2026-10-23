//! Price table updates from a public price dataset, on request only (management page
//! 「检查更新」/「应用」 or `ciallopa prices sync`). Nothing is fetched in the background.
//!
//! A check downloads the dataset (`[usage] prices_source`, default LiteLLM's
//! `model_prices_and_context_window.json`), keeps the OpenAI text models and diffs them
//! against the local table (`[usage] prices`). Applying writes the table only when something
//! changes, after copying the previous file to `prices.toml.bak`. Models with a dated (promo)
//! entry are never touched; per-model multipliers the dataset does not give and `note`s
//! survive. The download carries no account data and uses the egress proxy (`proxy_url`)
//! like every other outbound connection.
use ciallopa_core::Config;
use ciallopa_usage::prices::{self, Change, Entry, PriceTable};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub const LITELLM_URL: &str = ciallopa_core::config::DEFAULT_PRICES_SOURCE;
pub const MODELS_DEV_URL: &str = "https://models.dev/api.json";
/// The LiteLLM file is ~3 MB; a cap keeps a wrong URL from filling memory.
const MAX_BODY: u64 = 32 * 1024 * 1024;
const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub type Err = Box<dyn std::error::Error + Send + Sync>;

/// The URL a `from` argument names: empty = `[usage] prices_source`, `litellm`,
/// `models.dev`, or an http(s) URL. `None` = a local file path.
pub fn source_url(cfg: &Config, from: &str) -> Result<Option<String>, Err> {
    Ok(Some(match from.trim() {
        "" => {
            let s = cfg.usage.prices_source.trim();
            if s.is_empty() {
                return Err("[usage] prices_source is empty (price checks are off)".into());
            }
            s.to_string()
        }
        "litellm" => LITELLM_URL.into(),
        "models.dev" => MODELS_DEV_URL.into(),
        u if u.starts_with("http://") || u.starts_with("https://") => u.into(),
        _ => return Ok(None),
    }))
}

/// Download (or read) a dataset and return its OpenAI entries.
pub async fn fetch_entries(cfg: &Config, from: &str) -> Result<(String, Vec<Entry>), Err> {
    let (shown, body) = match source_url(cfg, from)? {
        None => (from.to_string(), std::fs::read(from)?),
        Some(url) => {
            let http =
                ciallopa_upstream::build_reqwest(&ciallopa_upstream::ClientOptions::from(cfg))?;
            let mut r = http
                .get(&url)
                .timeout(TIMEOUT)
                .header(
                    http::header::USER_AGENT,
                    concat!("ciallopa/", env!("CARGO_PKG_VERSION")),
                )
                .send()
                .await?;
            if !r.status().is_success() {
                return Err(format!("GET {url}: HTTP {}", r.status().as_u16()).into());
            }
            if r.content_length().is_some_and(|n| n > MAX_BODY) {
                return Err(format!("GET {url}: file too large").into());
            }
            let mut body = Vec::new();
            while let Some(c) = r.chunk().await? {
                body.extend_from_slice(&c);
                if body.len() as u64 > MAX_BODY {
                    return Err(format!("GET {url}: file too large").into());
                }
            }
            (url, body)
        }
    };
    let v: Value = serde_json::from_slice(&body).map_err(|e| format!("{shown}: {e}"))?;
    let entries = prices::parse_source(&v).map_err(|e| format!("{shown}: {e}"))?;
    Ok((shown, entries))
}

/// Short content hash (detects a table edited between check and apply).
pub fn file_hash(p: &Path) -> String {
    crate::settings::hash(&std::fs::read(p).unwrap_or_default())
}

/// Write `local` + `changes` to `path` (previous file → `.toml.bak`). Returns the backup.
pub fn write_table(
    path: &Path,
    local: &PriceTable,
    changes: &[Change],
    source: &str,
) -> Result<(PriceTable, Option<PathBuf>), Err> {
    let next = prices::apply(local, changes);
    let body = format!(
        "# ciallopa price table: USD per 1M tokens, notional for subscription traffic.\n\
         # Updated from {source}. Comments are not kept by updates (see the .bak);\n\
         # dated entries (effective_from / effective_until) are never changed by an update.\n{}",
        next.to_toml()
    );
    let parsed = PriceTable::parse(&body)?; // never write a file we can't read back
    if let Some(d) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(d)?;
    }
    let bak = if path.exists() {
        let b = path.with_extension("toml.bak");
        std::fs::copy(path, &b)?;
        Some(b)
    } else {
        None
    };
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)?;
    Ok((parsed, bak))
}

/// A change as JSON for the management page.
pub fn change_json(c: &Change) -> Value {
    match c {
        Change::Add(e) => {
            json!({"kind": "add", "model": prices::label(e), "new": prices::fmt_entry(e)})
        }
        Change::Update { old, new } => json!({"kind": "update", "model": prices::label(old),
            "old": prices::fmt_entry(old), "new": prices::fmt_entry(new)}),
        Change::SkipDated(m) => json!({"kind": "skip_dated", "model": m}),
        Change::Missing(m) => json!({"kind": "missing", "model": m}),
    }
}

pub struct SyncOutcome {
    pub report: String,
    pub changes: Vec<Change>,
    pub written: bool,
}

/// Diff (and optionally write) `local_path` against `remote` entries.
pub fn sync_with(
    local_path: &Path,
    remote: &[Entry],
    source: &str,
    write: bool,
) -> Result<SyncOutcome, Err> {
    let local = PriceTable::load(local_path)?;
    let changes = prices::diff(&local, remote);
    let actionable = changes.iter().filter(|c| c.actionable()).count();
    let mut report = prices::render_diff(&changes);
    report.push_str(&format!(
        "{actionable} change(s) for {} ({} models in {source})\n",
        local_path.display(),
        remote.len()
    ));
    let mut written = false;
    if actionable > 0 {
        if write {
            let (_, bak) = write_table(local_path, &local, &changes, source)?;
            if let Some(b) = bak {
                report.push_str(&format!("backup: {}\n", b.display()));
            }
            report.push_str("written.\n");
            written = true;
        } else {
            report.push_str("dry run: re-run with --write to apply.\n");
        }
    }
    Ok(SyncOutcome {
        report,
        changes,
        written,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn remote() -> Vec<Entry> {
        prices::parse_source(&serde_json::json!({
            "gpt-a": {"litellm_provider": "openai", "mode": "chat", "input_cost_per_token": 1.5e-6, "output_cost_per_token": 2e-6},
            "gpt-b": {"litellm_provider": "openai", "mode": "chat", "input_cost_per_token": 3e-6, "output_cost_per_token": 4e-6},
            "gpt-p": {"litellm_provider": "openai", "mode": "chat", "input_cost_per_token": 5e-6, "output_cost_per_token": 25e-6}
        }))
        .unwrap()
    }

    #[test]
    fn dry_run_then_write_then_unchanged() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("prices.toml");
        std::fs::write(
            &p,
            "[[model]]\nmodel=\"gpt-a\"\nin=1.0\nout=2.0\nfast_x=2.5\n\n[[model]]\nmodel=\"gpt-p\"\nin=4.0\nout=20.0\neffective_until=\"2026-11-21\"\n",
        )
        .unwrap();
        let src = remote();
        let before = std::fs::read_to_string(&p).unwrap();
        let o = sync_with(&p, &src, "test", false).unwrap();
        assert!(!o.written);
        assert!(o.report.contains("dry run"), "{}", o.report);
        assert!(
            o.report.contains("+ gpt-b")
                && o.report.contains("~ gpt-a")
                && o.report.contains("! gpt-p")
        );
        assert_eq!(
            std::fs::read_to_string(&p).unwrap(),
            before,
            "no write without --write"
        );

        let o = sync_with(&p, &src, "test", true).unwrap();
        assert!(o.written);
        let t = PriceTable::load(&p).unwrap();
        let a = t.models.iter().find(|e| e.model == "gpt-a").unwrap();
        assert_eq!((a.input, a.fast_x), (1.5, Some(2.5)), "local extras kept");
        assert!(t.models.iter().any(|e| e.model == "gpt-b"));
        let promo = t.models.iter().find(|e| e.model == "gpt-p").unwrap();
        assert_eq!(promo.input, 4.0, "promo entry untouched");
        assert_eq!(
            std::fs::read_to_string(p.with_extension("toml.bak")).unwrap(),
            before
        );
        // unchanged ⇒ no write (mtime and content stay)
        let m1 = std::fs::metadata(&p).unwrap().modified().unwrap();
        let o = sync_with(&p, &src, "test", true).unwrap();
        assert!(!o.written);
        assert_eq!(std::fs::metadata(&p).unwrap().modified().unwrap(), m1);
    }

    #[test]
    fn fresh_install_creates_table() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("sub/prices.toml");
        let o = sync_with(&p, &remote(), "test", true).unwrap();
        assert!(o.written);
        assert!(!p.with_extension("toml.bak").exists());
        assert_eq!(PriceTable::load(&p).unwrap().models.len(), 3);
    }

    #[test]
    fn source_names() {
        let mut c = Config::default();
        assert_eq!(source_url(&c, "").unwrap().as_deref(), Some(LITELLM_URL));
        assert_eq!(
            source_url(&c, "models.dev").unwrap().as_deref(),
            Some(MODELS_DEV_URL)
        );
        assert_eq!(source_url(&c, "/tmp/x.json").unwrap(), None);
        c.usage.prices_source.clear();
        assert!(source_url(&c, "").is_err());
    }
}
