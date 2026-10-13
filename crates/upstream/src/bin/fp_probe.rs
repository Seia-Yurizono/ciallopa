//! Fingerprint probe: issues the same requests through `build_reqwest` that
//! Codex CLI issues at startup, so fpserver can record JA4 + Akamai h2.
//! Usage: fp-probe [base_url]   (trust a custom CA with SSL_CERT_FILE)
use cpa_lite_upstream::{build_reqwest, ClientOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "https://chatgpt.com/backend-api".into());
    let c = build_reqwest(&ClientOptions::default())?;
    for path in [
        "/codex/models?client_version=0.162.1",
        "/wham/accounts/check",
    ] {
        let r = c
            .get(format!("{base}{path}"))
            .header("originator", "codex_exec")
            .header(
                "user-agent",
                "codex_exec/0.162.1 (Debian 13.0.0; x86_64) dumb",
            )
            .header("authorization", "Bearer fake")
            .send()
            .await?;
        println!("{path} -> {} {:?}", r.status(), r.version());
    }
    Ok(())
}
