# Release notes — v0.4.2

本版本基于 v0.4.1，是一次依赖安全更新：内置 SQLite 升到 3.54.1，修复 CVE-2026-119375；顺带升级了有 RustSec 公告的 zstd 相关 crate。功能、配置和 usage 数据库 schema 都没有变化（仍为 v15）。

**SQLite**

- rusqlite 0.39.0 → 0.41.1，libsqlite3-sys 0.37.0 → 0.39.1，内置 SQLite 3.51.3 → 3.54.1。
- 修复 [RUSTSEC-2026-0459](https://rustsec.org/advisories/RUSTSEC-2026-0459) / CVE-2026-119375：SQLite 3.54.0 之前，如果数据库文件旁边的 `-wal` 文件页大小与主库不一致，查询可能读到页缓存里未初始化的内存。需要本机攻击者能替换 usage 数据库及其 `-wal` 文件才能利用；ciallopa 的数据目录默认只有运行用户可读写，但仍建议升级。
- hashlink 0.11.0 → 0.12.2，随 rusqlite 0.41 一起升级（语句缓存用）。

**其他依赖**

- zstd 0.13.3 → 0.13.4，zstd-safe 7.2.4 → 7.4.0，zstd-sys 2.0.16 → 2.1.2：修复 RUSTSEC-2026-0391 / 0398 / 0403 / 0410 以及 zstd-safe 的多项 unsound 公告。底层仍是 libzstd 1.5.7，压缩后发往上游的 `/responses` 请求体与之前逐字节一致。
- mio 1.2.0 → 1.2.4，与 Codex 0.167.1 的依赖锁重新对齐。
- rustix 1.1.4 → 1.1.6（RUSTSEC-2026-0486），仅测试依赖使用，不进入发布的二进制。
- tokio 保持 1.52.3，与 Codex 0.167.1 一致。RUSTSEC-2026-0386 只涉及 `AsyncFd` 构造函数，ciallopa 及其启用的依赖都没有调用。
- 网络栈（reqwest、rustls、hyper、h2、tungstenite）版本不变，TLS / HTTP/2 / WebSocket 指纹不变。

**升级**

- 直接替换二进制、安装包或镜像即可，配置文件无需修改，usage 数据库不迁移。
- 发布后，`latest` 和 `0.4` 指向 0.4.2。按[容器清理策略](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/CONTAINER_CLEANUP.md)，GHCR 只保留最新的两个标签名，固定在 `0.4.1` 的部署之后可能无法再拉取该镜像，请先改为 `0.4.2` 或 `0.4`，见[部署指南](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/DEPLOYMENT.md#image-tags-and-upgrades)。

**验证**

- 本地 workspace 测试 561 项全部通过（新增一项：打开 usage 数据库后检查 `sqlite_version()` 不低于 3.54.0），loadbench 示例 5 项通过。严格 Clippy、格式检查、发布与容器脚本自测，以及 UI、设置、账号和请求观测四项 Node 检查均已通过。`cargo audit` 只剩上面说明的 tokio 提示。指纹检查（fp-check）通过，依赖锁比对（lock-check）对照 Codex 0.167.1 无漂移，提前于 Codex 的安全升级单独列出。安装包与容器的冒烟测试在标签工作流中运行。
- 验证全部使用合成数据和本地模拟上游，未调用真实模型上游。
