use clap::{Parser, Subcommand};
use cpa_lite_core::{logging, Config};
use cpa_lite_forward::AppState;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "cpa-lite",
    version,
    about = "Minimal Codex multi-account proxy"
)]
struct Cli {
    #[arg(short, long, env = "CPA_LITE_CONFIG", default_value = "config.toml")]
    config: PathBuf,
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let cfg = if cli.config.exists() {
        Config::load(&cli.config)?
    } else {
        Config::parse("")?
    };
    logging::init(&cfg.log.level);
    match cli.cmd.unwrap_or(Cmd::Serve) {
        Cmd::Login { method, from } => {
            use cpa_lite_auth::login;
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
        Cmd::Serve => {}
    }
    if !cfg.listen.ip().is_loopback() {
        tracing::warn!(listen = %cfg.listen, "binding non-loopback; expected to sit behind Caddy");
    }
    if cfg.api_keys.is_empty() {
        tracing::warn!("api_keys is empty: every proxied request will get 401");
    }
    let usage = if cfg.usage.enabled {
        match cpa_lite_usage::Recorder::open(&cfg.usage) {
            Ok(r) => {
                tracing::info!(path = %cfg.usage.path.display(), "usage log enabled");
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
    let state = AppState {
        usage: usage.clone(),
        accounts: Arc::new(
            cpa_lite_auth::FileAccountStore::open_with(cfg.auth_dir.clone(), oauth_client(&cfg)?)
                .await?,
        ),
        router: Arc::new(cpa_lite_routing::CpaRouter::new(cfg.routing.clone())),
        upstream: Arc::new(cpa_lite_upstream::UpstreamClient::new(&cfg)?),
        config: Arc::new(cfg),
    };
    let listen = state.config.listen;
    let app = cpa_lite::build_app(state);
    let ln = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "cpa-lite listening");
    axum::serve(ln, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    if let Some(u) = usage {
        u.shutdown();
    }
    Ok(())
}

/// Production OAuth client; `CPA_LITE_OAUTH_ISSUER` overrides the issuer (tests / mocks only).
fn oauth_client(cfg: &Config) -> Result<cpa_lite_auth::OAuthClient, Box<dyn std::error::Error>> {
    let c = cpa_lite_auth::OAuthClient::new(&cfg.proxy_url)?;
    Ok(match std::env::var("CPA_LITE_OAUTH_ISSUER") {
        Ok(i) if !i.is_empty() => cpa_lite_auth::OAuthClient::with_issuer(&i, c.http),
        _ => c,
    })
}
