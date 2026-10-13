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
    /// Login / import (M1, auth worker).
    Login {
        #[arg(long, value_parser = ["device", "paste", "pkce", "import"], default_value = "device")]
        method: String,
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
        Cmd::Login { method } => {
            tracing::error!(%method, "login not implemented in M0");
            std::process::exit(2);
        }
        Cmd::Serve => {}
    }
    if !cfg.listen.ip().is_loopback() {
        tracing::warn!(listen = %cfg.listen, "binding non-loopback; expected to sit behind Caddy");
    }
    if cfg.api_keys.is_empty() {
        tracing::warn!("api_keys is empty: every proxied request will get 401");
    }
    let state = AppState {
        accounts: Arc::new(cpa_lite_auth::FileAccountStore::open(cfg.auth_dir.clone()).await?),
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
    Ok(())
}
