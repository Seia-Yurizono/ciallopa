//! Fingerprint probe: issues the same requests through `build_reqwest` that
//! Codex CLI issues at startup, so fpserver can record JA4 + Akamai h2.
//! Usage: fp-probe [base_url]   (trust a custom CA with SSL_CERT_FILE)
use ciallopa_upstream::{build_reqwest, ClientOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "https://chatgpt.com/backend-api".into());
    let c = build_reqwest(&ClientOptions::default())?;
    for path in [
        "/codex/models?client_version=0.166.0",
        "/wham/accounts/check",
    ] {
        let r = c
            .get(format!("{base}{path}"))
            .header("originator", "codex_exec")
            .header(
                "user-agent",
                "codex_exec/0.166.0 (Debian 13.0.0; x86_64) dumb",
            )
            .header("authorization", "Bearer fake")
            .send()
            .await?;
        println!("{path} -> {} {:?}", r.status(), r.version());
    }
    // Responses WebSocket handshake through the same dialer the proxy uses.
    let up = ciallopa_upstream::UpstreamClient {
        base_url: base.trim_end_matches('/').to_string(),
        http: c,
        opts: std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(ClientOptions::default())),
        pools: std::sync::Arc::new(ciallopa_upstream::ClientPool::new(ClientOptions::default())),
        egress: std::sync::Arc::new(tokio::sync::watch::Sender::new(0)),
        drain: Default::default(),
    };
    let mut h = http::HeaderMap::new();
    h.insert("authorization", "Bearer fake".parse()?);
    h.insert(
        "user-agent",
        "codex_exec/0.166.0 (Debian 13.0.0; x86_64) dumb".parse()?,
    );
    h.insert("originator", "codex_exec".parse()?);
    match up.ws_connect("/codex/responses", h).await {
        Ok(_) => println!("ws -> connected"),
        Err(e) => println!("ws -> {e}"),
    }
    Ok(())
}
