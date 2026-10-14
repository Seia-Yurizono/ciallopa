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
    /// Add an account: device code (default), paste callback URL, PKCE on 127.0.0.1:1455, or import.
    Login {
        #[arg(long, value_parser = ["device", "paste", "pkce", "import"], default_value = "device")]
        method: String,
        /// For `--method import`: Codex CLI auth.json path.
        #[arg(long, default_value = "~/.codex/auth.json")]
        from: PathBuf,
    },
    /// Price table tools.
    Prices {
        #[command(subcommand)]
        cmd: PricesCmd,
    },
}

#[derive(Subcommand)]
enum PricesCmd {
    /// Fetch standard prices, print the diff vs `[usage] prices`; write only with --write.
    Sync {
        /// `models.dev`, an http(s) URL, or a local api.json path.
        #[arg(long, default_value = "models.dev")]
        from: String,
        #[arg(long, default_value = "openai")]
        provider: String,
        /// Apply the changes (keeps a .bak). Without it: dry run.
        #[arg(long)]
        write: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let env_cfg = ciallopa_core::config::env_var("CONFIG");
    let config_path = cli
        .config
        .clone()
        .or_else(|| env_cfg.as_ref().map(|(v, _)| PathBuf::from(v)))
        .unwrap_or_else(|| PathBuf::from("config.toml"));
    let cfg = if config_path.exists() {
        Config::load(&config_path)?
    } else {
        Config::parse("")?
    };
    logging::init(&cfg.log.level);
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
            println!("saved account {id} in {}", dir.display());
            return Ok(());
        }
        Cmd::Prices {
            cmd:
                PricesCmd::Sync {
                    from,
                    provider,
                    write,
                },
        } => {
            let src = ciallopa::prices_cli::fetch_source(&cfg, &from).await?;
            let o = ciallopa::prices_cli::sync_with(&cfg.usage.prices, &src, &provider, write)?;
            print!("{}", o.report);
            return Ok(());
        }
        Cmd::Serve => {}
    }
    if !cfg.listen.ip().is_loopback() {
        tracing::warn!(listen = %cfg.listen, "binding non-loopback; expected to sit behind Caddy");
    }
    if cfg.api_keys.is_empty() {
        tracing::warn!("api_keys is empty: every proxied request will get 401");
    }
    let usage = if cfg.usage.enabled {
        match ciallopa_usage::Recorder::open_as(&cfg.usage, &cfg.instance_id) {
            Ok(r) => {
                tracing::info!(path = %cfg.usage.path.display(), instance = %cfg.instance_id, "usage log enabled");
                match ciallopa_usage::PriceTable::load(&cfg.usage.prices) {
                    Ok(p) if p.is_empty() => {
                        tracing::info!(path = %cfg.usage.prices.display(), "no price table; cost_usd stays null")
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
    ciallopa::client_fp::install(&cfg, usage.clone());
    let oauth = oauth_client(&cfg)?;
    let oauth_pools = oauth.pools.clone();
    let state = AppState {
        usage: usage.clone(),
        accounts: Arc::new(
            ciallopa_auth::FileAccountStore::open_with(cfg.auth_dir.clone(), oauth).await?,
        ),
        router: Arc::new(ciallopa_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(ciallopa_upstream::UpstreamClient::new(&cfg)?),
        config: Arc::new(cfg.into()),
    };
    let listen = state.config.load().listen;
    // Live settings: file watch (debounced) + SIGHUP + admin API share one apply pipeline.
    let settings = ciallopa::settings::Settings::new(state, Some(config_path), oauth_pools);
    settings.init_known_accounts().await;
    let _watcher = ciallopa::settings::spawn_watcher(settings.clone());
    ciallopa::settings::spawn_sighup(settings.clone());
    let app = ciallopa::build_app_with(settings);
    let ln = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "ciallopa listening");
    axum::serve(ln, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    ciallopa::client_fp::flush();
    if let Some(u) = usage {
        u.shutdown();
    }
    Ok(())
}

fn oauth_client(cfg: &Config) -> Result<ciallopa_auth::OAuthClient, Box<dyn std::error::Error>> {
    Ok(ciallopa::oauth_client(&cfg.proxy_url)?)
}
