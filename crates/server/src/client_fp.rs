//! Persistence + API for the learned client fingerprint (`ciallopa_core::fingerprint`).
//! Stored in the usage DB (`kv`, key `client_fingerprint`) or, with the usage log disabled,
//! in `client_fingerprint.json` next to the configured usage DB path.
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use ciallopa_core::fingerprint::{self, Fingerprint};
use ciallopa_core::Config;
use ciallopa_usage::Recorder;
use std::path::PathBuf;
use std::sync::Arc;

const KEY: &str = "client_fingerprint";

#[derive(Clone)]
enum Backend {
    Db(Arc<Recorder>),
    File(PathBuf),
}

fn backend(cfg: &Config, usage: Option<Arc<Recorder>>) -> Backend {
    match usage {
        Some(r) => Backend::Db(r),
        None => Backend::File(cfg.usage.path.with_file_name("client_fingerprint.json")),
    }
}

fn load(b: &Backend) -> Option<Fingerprint> {
    let s = match b {
        Backend::Db(r) => r.kv_get(KEY).ok().flatten()?,
        Backend::File(p) => std::fs::read_to_string(p).ok()?,
    };
    serde_json::from_str(&s).ok()
}

fn save(b: &Backend, fp: &Fingerprint) {
    let Ok(s) = serde_json::to_string(fp) else {
        return;
    };
    let res = match b {
        Backend::Db(r) => r.kv_set(KEY, &s).map_err(|e| e.to_string()),
        Backend::File(p) => {
            let tmp = p.with_extension("json.tmp");
            p.parent()
                .map(std::fs::create_dir_all)
                .transpose()
                .and_then(|_| std::fs::write(&tmp, &s))
                .and_then(|_| std::fs::rename(&tmp, p))
                .map_err(|e| e.to_string())
        }
    };
    if let Err(e) = res {
        tracing::warn!(error = %e, "persisting client fingerprint failed");
    }
}

/// Restore the persisted fingerprint and persist future changes (off the request path).
pub fn install(cfg: &Config, usage: Option<Arc<Recorder>>) {
    let b = backend(cfg, usage);
    if let Some(fp) = load(&b) {
        if fingerprint::restore(fp) {
            let f = fingerprint::current();
            tracing::info!(user_agent = %f.user_agent, "restored learned client fingerprint");
        }
    }
    fingerprint::set_saver(Box::new(move |fp| {
        let (b, fp) = (b.clone(), fp.clone());
        let _ = std::thread::Builder::new()
            .name("fp-save".into())
            .spawn(move || save(&b, &fp));
    }));
}

/// Restore only (CLI subcommands such as `login`): no saver.
pub fn restore_only(cfg: &Config) {
    let b = if cfg.usage.enabled && cfg.usage.path.exists() {
        match Recorder::open_as(&cfg.usage, &cfg.instance_id) {
            Ok(r) => {
                let fp = load(&Backend::Db(r.clone()));
                r.shutdown();
                if let Some(fp) = fp {
                    fingerprint::restore(fp);
                }
                return;
            }
            Err(_) => return,
        }
    } else {
        backend(cfg, None)
    };
    if let Some(fp) = load(&b) {
        fingerprint::restore(fp);
    }
}

/// `GET /usage/v1/client-fingerprint` (behind the client key, read-only).
pub fn routes() -> Router {
    Router::new().route(
        "/usage/v1/client-fingerprint",
        get(|| async { Json(fingerprint::current()).into_response() }),
    )
}
