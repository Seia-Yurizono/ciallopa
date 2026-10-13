# cpa-lite

Minimal Rust rewrite of CLIProxyAPI's Codex path: multi ChatGPT-subscription accounts for Codex clients, behind Caddy.
Design: `<local-path>`. Interfaces/ownership: `docs/INTERFACES.md`.

Status: **M0** — workspace, config, pinned network stack (= codex-rs Cargo.lock), upstream client mirroring codex-rs, axum server with `/healthz` + static key auth, redacting logs, stubs for auth/routing/forwarding.

```
cargo build && cargo clippy --all-targets -- -D warnings && cargo test
scripts/lock-check.sh   # deps ⊆ codex-rs Cargo.lock
scripts/fp-check.sh     # JA4 + Akamai h2 == Codex CLI 0.162.1 (sudo; restores /etc/hosts)
cargo run -p cpa-lite -- -c config.toml
```

## Disclaimer / 免责声明

> [!WARNING]
> **Personal, non-commercial use only. Not affiliated with, endorsed by, or supported by OpenAI.**
>
> - Use this project only in compliance with OpenAI's [Terms of Use](https://openai.com/policies/terms-of-use/), [Usage Policies](https://openai.com/policies/usage-policies/) and the applicable laws of your jurisdiction. You alone are responsible for how you use it.
> - Only use ChatGPT accounts that you own. The Terms of Use state that you "may not share your account credentials or make your account available to anyone else", and prohibit attempts to "lease, sell or distribute" the Services or to "circumvent any rate limits or restrictions or bypass any protective measures". Do not use this project to share, pool, resell or sublease access to accounts, or to get around usage limits.
> - Do not distribute this project at scale, run it as a public or paid relay/API service, or use it for any commercial purpose. The authors have never authorized commercial use of this project.
> - For learning and research. Provided "as is" without warranty of any kind; the authors accept no liability for account suspension, data loss or any other damage arising from its use.
>
> **仅供个人、非商业用途，与 OpenAI 无任何关联，亦未获其认可或支持。**
>
> - 请仅在遵守 OpenAI [使用条款（Terms of Use）](https://openai.com/policies/terms-of-use/)、[使用政策（Usage Policies）](https://openai.com/policies/usage-policies/)及所在地法律法规的前提下使用本项目，使用后果由使用者自行承担。
> - 仅使用你本人拥有的 ChatGPT 账号。OpenAI 使用条款规定不得共享账号凭据或将账号提供给他人使用，不得出租、出售或分发其服务，不得规避速率限制或绕过保护措施。请勿用本项目共享、合租、转售或转租账号访问权限，或规避用量限制。
> - 禁止大规模分发本项目，禁止将其作为公开或付费的中转/API 服务运营，禁止任何商业用途。作者从未授权任何基于本项目的商业行为。
> - 本项目仅供学习与研究，按"原样"提供，不附带任何担保；因使用本项目导致的账号封禁、数据丢失或其他任何损失，作者概不负责。
