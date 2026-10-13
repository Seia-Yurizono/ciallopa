//! `ciallopa prices sync --from models.dev [--write]`: fetch standard prices, print the diff
//! against the local `prices.toml`, write (with a `.bak`) only when `--write` is given.
use ciallopa_core::Config;
use ciallopa_usage::prices::{self, Change, PriceTable};
use std::path::Path;

pub const MODELS_DEV_URL: &str = "https://models.dev/api.json";

type Err = Box<dyn std::error::Error>;

/// `models.dev` | `http(s)://…` | local file. Network goes through `proxy_url` (egress).
pub async fn fetch_source(cfg: &Config, from: &str) -> Result<serde_json::Value, Err> {
    let url = match from {
        "models.dev" => MODELS_DEV_URL,
        u if u.starts_with("http://") || u.starts_with("https://") => u,
        path => return Ok(serde_json::from_slice(&std::fs::read(path)?)?),
    };
    let http = ciallopa_upstream::build_reqwest(&ciallopa_upstream::ClientOptions::from(cfg))?;
    let r = http
        .get(url)
        .timeout(std::time::Duration::from_secs(60))
        .send()
        .await?;
    if !r.status().is_success() {
        return Err(format!("GET {url}: {}", r.status()).into());
    }
    Ok(r.json().await?)
}

pub struct SyncOutcome {
    pub report: String,
    pub changes: Vec<Change>,
    pub written: bool,
}

/// Diff (and optionally write) `local_path` against `source`.
pub fn sync_with(
    local_path: &Path,
    source: &serde_json::Value,
    provider: &str,
    write: bool,
) -> Result<SyncOutcome, Err> {
    let local = PriceTable::load(local_path)?;
    let remote = prices::from_models_dev(source, provider);
    if remote.is_empty() {
        return Err(format!("no priced models for provider {provider:?} in source").into());
    }
    let changes = prices::diff(&local, &remote);
    let actionable = changes
        .iter()
        .filter(|c| matches!(c, Change::Add(_) | Change::Update { .. }))
        .count();
    let mut report = prices::render_diff(&changes);
    report.push_str(&format!(
        "{actionable} change(s) for {} ({} remote models)\n",
        local_path.display(),
        remote.len()
    ));
    let mut written = false;
    if actionable > 0 {
        if write {
            let next = prices::apply(&local, &changes);
            if local_path.exists() {
                let bak = local_path.with_extension("toml.bak");
                std::fs::copy(local_path, &bak)?;
                report.push_str(&format!("backup: {}\n", bak.display()));
            }
            let body = format!(
                "# Written by `ciallopa prices sync` (comments are not preserved; see the .bak).\n\
                 # USD per 1M tokens; NOTIONAL for subscription traffic.\n{}",
                next.to_toml()
            );
            PriceTable::parse(&body)?; // never write a file we can't read back
            if let Some(d) = local_path.parent() {
                std::fs::create_dir_all(d)?;
            }
            std::fs::write(local_path, body)?;
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

    #[test]
    fn dry_run_then_write() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("prices.toml");
        std::fs::write(
            &p,
            "[[model]]\nmodel=\"a\"\nin=1.0\nout=2.0\nfast_x=2.5\n\n[[model]]\nmodel=\"p\"\nin=4.0\nout=20.0\neffective_until=\"2026-11-21\"\n",
        )
        .unwrap();
        let src: serde_json::Value = serde_json::from_str(
            r#"{"openai":{"models":{"a":{"cost":{"input":1.5,"output":2}},"b":{"cost":{"input":3,"output":4}},"p":{"cost":{"input":5,"output":25}}}}}"#,
        )
        .unwrap();
        let before = std::fs::read_to_string(&p).unwrap();
        let o = sync_with(&p, &src, "openai", false).unwrap();
        assert!(!o.written);
        assert!(o.report.contains("dry run"), "{}", o.report);
        assert!(o.report.contains("+ b") && o.report.contains("~ a") && o.report.contains("! p"));
        assert_eq!(
            std::fs::read_to_string(&p).unwrap(),
            before,
            "no write without --write"
        );

        let o = sync_with(&p, &src, "openai", true).unwrap();
        assert!(o.written);
        let t = PriceTable::load(&p).unwrap();
        let a = t.models.iter().find(|e| e.model == "a").unwrap();
        assert_eq!((a.input, a.fast_x), (1.5, Some(2.5)), "local extras kept");
        assert!(t.models.iter().any(|e| e.model == "b"));
        let promo = t.models.iter().find(|e| e.model == "p").unwrap();
        assert_eq!(promo.input, 4.0, "promo entry untouched");
        assert!(p.with_extension("toml.bak").exists());
        // idempotent
        let o = sync_with(&p, &src, "openai", true).unwrap();
        assert!(!o.written);
        assert!(sync_with(&p, &src, "nobody", false).is_err());
    }
}
