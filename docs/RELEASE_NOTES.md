本次从 0.6.4 升级到 0.6.5，改进流式错误处理、对齐 Codex 0.172.0，并补充 WebSocket 消息上限观测。

**流式错误与冷却**

- WebSocket 与 SSE 流内的 `error` / `response.failed` 事件优先从嵌套的 `error.headers` 读取重试指示，兼容顶层 `headers`。解析 `Retry-After`（回退到 `retry-after-ms`）及 `x-retry-metadata: NO_MORE_RETRY`，接入限流冷却、过载等待和连续过载次数达到阈值后的冷却。

**Codex 0.172.0 对齐**

- 支持 0.172.0 新增的无 `status` 终结错误码。`credit_balance_exhausted` 纳入额度换号逻辑，三个 `*_spend_limit_exceeded` 错误码终结当前 turn。默认版本基线 `PINNED_CODEX_VERSION` 统一更新至 `0.172.0`。

**WebSocket 上限观测与文档**

- 握手时用 `debug` 日志记录上游 101 响应广告的 `x-codex-websocket-max-message-bytes`、本地上限和最终广告值。默认 `ws_max_message_mb = 16`，文档已修正过时的“16 MiB 硬上限”表述。

**升级说明**

本次无数据库 schema 或配置项变更，数据库仍为 schema v22，可从 0.6.4 直接平滑升级。

安装与备份方法见[部署指南](https://github.com/Seia-Yurizono/ciallopa/blob/v0.6.5/docs/DEPLOYMENT.md#image-tags-and-upgrades)。
