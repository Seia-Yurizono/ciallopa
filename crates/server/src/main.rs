// jemalloc instead of the system allocator (musl in release builds): about half the CPU
// per request. One arena keeps peak and retained RSS close to musl's after large bodies;
// a background purge thread raised the peak in measurements, so pages decay on activity.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] = b"narenas:1\0";
use ciallopa_core::{logging, Config};
use ciallopa_forward::AppState;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "ciallopa",
    version,
    about = "Minimal Codex multi-account proxy"
)]
struct Cli {
    /// Config file (env `CIALLOPA_CONFIG`, legacy `CPA_LITE_CONFIG`; default `config.toml`).
    #[arg(short, long)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the proxy (default).
    Serve,
    /// Write a new config file (the -c / CIALLOPA_CONFIG path, default ./config.toml) from the
    /// bundled example with a random admin API key, and print how to use it. Never overwrites.
    Init,
    /// Add an account: device code (default), paste callback URL, PKCE on 127.0.0.1:1455, or import.
    Login {
        #[arg(long, value_parser = ["device", "paste", "pkce", "import"], default_value = "device")]
        method: String,
        /// For `--method import`: Codex CLI auth.json path.
        #[arg(long, default_value = "~/.codex/auth.json")]
        from: PathBuf,
    },
    /// Exit 0 when the local server answers GET /healthz with 200 (container health probes).
    Healthcheck,
    /// Price table tools.
    Prices {
        #[command(subcommand)]
        cmd: PricesCmd,
    },
}

#[derive(Subcommand)]
enum PricesCmd {
    /// Download the price dataset, print the diff vs `[usage] prices`; write only with --write.
    Sync {
        /// `litellm`, `models.dev`, an http(s) URL or a local JSON file. Default:
        /// `[usage] prices_source`.
        #[arg(long, default_value = "")]
        from: String,
        /// Apply the changes (keeps a .bak). Without it: dry run.
        #[arg(long)]
        write: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    ciallopa::host::restrict_umask();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    // A timed-out spawn_blocking flush is not cancelled. Do not wait forever for it
    // when dropping the runtime (the default runtime destructor would do so).
    runtime.shutdown_timeout(std::time::Duration::from_secs(2));
    result
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_env = "msvc"))]
    ciallopa::memory::jemalloc_is_global();
    let cli = Cli::parse();
    let env_cfg = ciallopa_core::config::env_var("CONFIG");
    let config_path = cli
        .config
        .clone()
        .or_else(|| env_cfg.as_ref().map(|(v, _)| PathBuf::from(v)))
        .unwrap_or_else(|| PathBuf::from("config.toml"));
    if matches!(cli.cmd, Some(Cmd::Init)) {
        match ciallopa::init::run(&config_path) {
            Ok(text) => {
                print!("{text}");
                return Ok(());
            }
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
    }
    // Built-in defaults have no API key: running on them would answer 401 to everything, so a
    // config that cannot be opened is an error.
    if let Err(e) = std::fs::File::open(&config_path) {
        use ciallopa::init::Unusable;
        let what = match e.kind() {
            std::io::ErrorKind::NotFound => Some(Unusable::Missing),
            std::io::ErrorKind::PermissionDenied => Some(Unusable::Denied),
            _ => None,
        };
        let shown = absolute(&config_path);
        match what {
            Some(what) => {
                let args: Vec<String> = std::env::args().skip(1).collect();
                eprintln!(
                    "{}",
                    ciallopa::init::config_error(
                        &shown,
                        cli.config.is_some() || env_cfg.is_some(),
                        cli.config.as_deref(),
                        what,
                        &ciallopa::init::args_without_config(&args),
                    )
                );
            }
            None => eprintln!("error: cannot read {}: {e}", ciallopa::host::shown(&shown)),
        }
        std::process::exit(1);
    }
    let cfg = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "error: {}: {e}",
                ciallopa::host::shown(&absolute(&config_path))
            );
            std::process::exit(1);
        }
    };
    if matches!(cli.cmd, Some(Cmd::Healthcheck)) {
        let ok = ciallopa::healthcheck(cfg.listen, HEALTHCHECK_TIMEOUT).await;
        std::process::exit(if ok { 0 } else { 1 });
    }
    logging::init(&cfg.log.level);
    logging::set_redact_emails(cfg.log.redact_emails);
    if cli.config.is_none() && matches!(env_cfg, Some((_, true))) {
        tracing::warn!("CPA_LITE_CONFIG is deprecated; rename it to CIALLOPA_CONFIG");
    }
    if let Some(old) = ciallopa_core::config::legacy_dir_in_use() {
        tracing::warn!(dir = %old.display(), "~/.ciallopa not found; using legacy ~/.cpa-lite (move it to ~/.ciallopa)");
    }
    let serving = matches!(cli.cmd, None | Some(Cmd::Serve));
    if !serving {
        ciallopa::client_fp::restore_only(&cfg);
    }
    match cli.cmd.unwrap_or(Cmd::Serve) {
        Cmd::Login { method, from } => {
            use ciallopa_auth::login;
            let dir = cfg.auth_dir.clone();
            if ciallopa::host::euid() == Some(0) && ciallopa::host::service_user().is_some() {
                // Files written as root are unreadable for the service user: the account would
                // be saved where the service never sees it.
                eprintln!(
                    "error: login as root would save the account in {} owned by root, where the {} service cannot read it. Run it as the service user:\n  sudo -u {u} HOME=/var/lib/{u} ciallopa -c {} login",
                    dir.display(),
                    ciallopa::host::SERVICE_USER,
                    ciallopa::host::DEB_CONFIG,
                    u = ciallopa::host::SERVICE_USER,
                );
                std::process::exit(1);
            }
            println!("Accounts are saved to {}", ciallopa::host::shown(&dir));
            let oauth = oauth_client(&cfg)?;
            let id = match method.as_str() {
                "paste" => login::paste_callback(&oauth, &dir).await,
                "pkce" => login::pkce_localhost(&oauth, &dir).await,
                "import" => {
                    let from = match from.strip_prefix("~") {
                        Ok(rest) => std::env::var_os("HOME")
                            .map(PathBuf::from)
                            .unwrap_or_default()
                            .join(rest),
                        Err(_) => from,
                    };
                    login::import_codex_auth_json(&from, &dir).await
                }
                _ => login::device_code(&oauth, &dir).await,
            }?;
            println!("saved account {id} in {}", ciallopa::host::shown(&dir));
            return Ok(());
        }
        Cmd::Prices {
            cmd: PricesCmd::Sync { from, write },
        } => {
            if write
                && ciallopa::host::euid() == Some(0)
                && ciallopa::host::service_user().is_some()
            {
                // A table written as root is unreadable for the service user.
                eprintln!(
                    "error: run it as the service user:\n  sudo -u {u} HOME=/var/lib/{u} ciallopa -c {} prices sync --write",
                    ciallopa::host::DEB_CONFIG,
                    u = ciallopa::host::SERVICE_USER,
                );
                std::process::exit(1);
            }
            let (shown, entries) = match ciallopa::prices::fetch_entries(&cfg, &from).await {
                Ok(x) => x,
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            };
            let o = ciallopa::prices::sync_with(&cfg.usage.prices, &entries, &shown, write)
                .map_err(|e| e.to_string())?;
            print!("{}", o.report);
            return Ok(());
        }
        Cmd::Serve | Cmd::Healthcheck | Cmd::Init => {}
    }
    let container = ciallopa::host::in_container();
    if container && cfg.listen.ip().is_loopback() {
        tracing::warn!(listen = %cfg.listen, "listening on loopback inside a container: unreachable through published ports; set listen = \"0.0.0.0:8317\"");
    } else if !container && !cfg.listen.ip().is_loopback() {
        tracing::warn!(listen = %cfg.listen, "binding non-loopback; expected to sit behind Caddy");
    }
    if cfg.api_keys.is_empty() {
        tracing::warn!("api_keys is empty: every proxied request will get 401");
    }
    for a in cfg.advisories() {
        tracing::warn!("{a}");
    }
    // Exists from startup so the watcher picks up accounts added later (`ciallopa login`).
    if let Err(e) = ciallopa_auth::file::ensure_auth_dir(&cfg.auth_dir) {
        tracing::warn!(dir = %cfg.auth_dir.display(), error = %e, "cannot create auth_dir");
    }
    let usage = if cfg.usage.enabled {
        match ciallopa_usage::Recorder::open_as(&cfg.usage, &cfg.instance_id) {
            Ok(r) => {
                tracing::info!(path = %cfg.usage.path.display(), instance = %cfg.instance_id, "usage log enabled");
                match ciallopa_usage::PriceTable::load(&cfg.usage.prices) {
                    Ok(p) if p.is_empty() => {
                        tracing::info!(path = %cfg.usage.prices.display(), "no price table yet; costs show as unpriced until prices are pulled (settings page 「价格」 or `ciallopa prices sync --write`)")
                    }
                    Ok(p) => {
                        tracing::info!(path = %cfg.usage.prices.display(), models = p.models.len(), "price table loaded (notional cost)");
                        r.set_prices(p);
                    }
                    Err(e) => tracing::warn!(error = %e, "price table ignored"),
                }
                Some(r)
            }
            Err(e) => {
                // Never let the usage log stop the proxy.
                tracing::warn!(error = %e, "usage log disabled: open failed");
                None
            }
        }
    } else {
        None
    };
    // Write-time account aliases: key lives in usage kv when usage is on; otherwise ephemeral.
    let aliaser = match &usage {
        Some(u) => match u.load_or_create_aliaser() {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "alias_key unavailable; using ephemeral aliases");
                ciallopa_core::Aliaser::ephemeral()
            }
        },
        None => ciallopa_core::Aliaser::ephemeral(),
    };
    ciallopa_core::set_current(aliaser.clone());
    ciallopa::client_fp::install(&cfg, usage.clone());
    let oauth = oauth_client(&cfg)?;
    let oauth_pools = oauth.pools.clone();
    let router = Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone()));
    let affinity = ciallopa::affinity_store::Store::new(
        router.clone(),
        ciallopa::affinity_store::path(&cfg),
        aliaser.clone(),
    );
    let state = AppState {
        usage: usage.clone(),
        accounts: Arc::new(
            ciallopa_auth::FileAccountStore::open_with(cfg.auth_dir.clone(), oauth).await?,
        ),
        router: router.clone(),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg)?),
        config: Arc::new(cfg.into()),
        egress: Default::default(),
    };
    let _occupancy = ciallopa_forward::occupancy::spawn_sampler(state.clone());
    let known: std::collections::HashSet<String> = state
        .accounts
        .list()
        .await
        .into_iter()
        .map(|a| a.id)
        .collect();
    aliaser.register_many(&known);
    affinity.load(&|id| known.contains(id));
    let _affinity_writer = affinity.spawn();
    if known.is_empty() {
        tracing::warn!(auth_dir = %state.config.load().auth_dir.display(), "no accounts yet: proxied requests fail until one is added with `ciallopa login` or on /ui/accounts");
    }
    let listen = state.config.load().listen;
    let drain = state.upstream.drain.clone();
    // Live settings: file watch (debounced) + SIGHUP + admin API share one apply pipeline.
    let settings = ciallopa::settings::Settings::new(state, Some(config_path), oauth_pools);
    // Historical audit rows may still say account:<raw-id>; rewrite once (no-op if clean).
    settings.rewrite_audit_aliases();
    settings.init_known_accounts().await;
    let _watcher = ciallopa::settings::spawn_watcher(settings.clone());
    ciallopa::settings::spawn_sighup(settings.clone());
    let app = ciallopa::build_app_with(settings);
    // Streamed responses are many small writes; Nagle would hold each behind the
    // client's delayed ACK (about 40 ms per chunk on a kept-alive connection).
    use axum::serve::ListenerExt;
    let ln = tokio::net::TcpListener::bind(listen).await?.tap_io(|tcp| {
        if let Err(err) = tcp.set_nodelay(true) {
            tracing::debug!(%err, "TCP_NODELAY not set");
        }
    });
    tracing::info!(%listen, "ciallopa listening");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    // Forwarded bodies and upgraded WS sessions share a drain. The HTTP server alone
    // cannot wait for detached upgrades, nor bound a client that stops reading.
    let mut shutdown = {
        let drain = drain.clone();
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        tokio::spawn(async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = interrupt.recv() => {}
            }
            drain.start();
            let close_deadline = tokio::time::Instant::now() + DRAIN_BOUND + CLOSE_GRACE;
            tracing::info!(
                inflight = drain.inflight(),
                ws_connections = drain.active(),
                "shutting down; draining forwarded requests"
            );
            let _ = stop_tx.send(());
            let second_signal = async {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = interrupt.recv() => {}
                }
            };
            tokio::select! {
                _ = second_signal => {
                    tracing::warn!("second shutdown signal; cancelling remaining requests");
                    drain.force();
                    // A second signal during forced close must not restart its grace.
                    let left = close_deadline.saturating_duration_since(tokio::time::Instant::now());
                    drain.wait_idle(CLOSE_GRACE.min(left)).await;
                }
                cut = drain.shutdown(DRAIN_BOUND, CLOSE_GRACE) => {
                    if cut > 0 {
                        tracing::warn!(inflight = drain.inflight(), ws_connections = drain.active(), "drain bound reached; remaining requests cancelled");
                    }
                }
            }
        })
    };
    use std::future::IntoFuture;
    let server = axum::serve(ln, app)
        .with_graceful_shutdown(async {
            let _ = stop_rx.await;
        })
        .into_future();
    tokio::pin!(server);
    let result = tokio::select! {
        result = &mut server => {
            if drain.phase() == ciallopa_upstream::drain::Phase::Running {
                // Listener failure: no signal task will arrive to finish detached work.
                drain.force();
                shutdown.abort();
                drain.wait_idle(CLOSE_GRACE).await;
            } else {
                let _ = shutdown.await;
            }
            result
        }
        _ = &mut shutdown => Ok(()),
    };
    let flush = tokio::task::spawn_blocking(move || {
        affinity.flush();
        ciallopa::client_fp::flush();
        ciallopa::auto_reset::flush();
        ciallopa::reset_credits::quota_auto_flush();
        if let Some(u) = usage {
            u.shutdown();
        }
    });
    match tokio::time::timeout(FLUSH_BOUND, flush).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "shutdown flush failed; recent records may be missing")
        }
        Err(_) => tracing::warn!("shutdown flush timed out; recent records may be missing"),
    }
    result?;
    Ok(())
}

/// 60 + 3 + 5 + 2(runtime) stays below the packaged 75 s stop grace.
const DRAIN_BOUND: std::time::Duration = std::time::Duration::from_secs(60);
const FLUSH_BOUND: std::time::Duration = std::time::Duration::from_secs(5);
fn absolute(p: &std::path::Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|d| d.join(p))
            .unwrap_or_else(|_| p.to_path_buf())
    }
}

/// Whole-probe bound for `ciallopa healthcheck` (connect + request + status line).
const HEALTHCHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
/// Time for the forced 1001 closes to be written.
const CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(3);

fn oauth_client(cfg: &Config) -> Result<ciallopa_auth::OAuthClient, Box<dyn std::error::Error>> {
    Ok(ciallopa::oauth_client(cfg)?)
}
