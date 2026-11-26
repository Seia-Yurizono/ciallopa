本次从 0.6.5 升级到 0.6.6，修复请求体字节预算的释放时机，统一账号故障转移的候选资格判定，并修复 WebSocket 跨轮切号误判。

**请求体字节预算释放**

- WebSocket 输出提交（`commit()`）时同步释放 `BodyPermit`，保留并发槽，已预留的请求体字节预算不再占用到连接结束。
- HTTP 413 / 503 拒绝分支在 10 秒排空前即归还已预留的字节预算。

**账号故障转移与 WebSocket 跨轮行为**

- `Router::usable_other` / `prefer_other` 与 `select` 统一候选资格判定：WRR 零权重、排除集、当前模型冷却三项规则一致。
- 修复 WebSocket 跨轮累积 `exclude` 导致误判 `NoAccounts` 及多余 `1012` 关闭的问题；排除集仅在当前轮内生效。
- `x-codex-turn-state` 的连接级切号历史与轮内排除解耦。

**文档**

- `ARCHITECTURE.md` 与 `SETTINGS.md` 更新 WS / HTTP 字节预算释放时机及 Images wrapper 局部上限的说明。

**升级说明**

本次无数据库 schema 或配置项变更，数据库仍为 schema v22，可从 0.6.5 直接平滑升级。

安装与备份方法见[部署指南](https://github.com/Seia-Yurizono/ciallopa/blob/v0.6.6/docs/DEPLOYMENT.md#image-tags-and-upgrades)。
