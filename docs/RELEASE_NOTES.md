本次从 0.6.3 升级到 0.6.4，修复账号额度刷新/重置时误清模型限流冷却的问题，并保留流内容量限流的精确错误码与说明文案。

**路由与冷却**

- 后台额度刷新与重置卡消费仅清除账号级 usage-limit 冷却，保留上游返回的单模型限流或容量冷却（如 `turn_rate_limit_exceeded` 携带的 `Retry-After`）。
- 并发收到不同原因的冷却信号时，保留持续时间更长的冷却及其原因标签，避免长周期额度冷却与短周期模型限流互相覆盖。

**流式错误观测**

- WebSocket 与 SSE 流内收到 `turn_rate_limit_exceeded` 时，落库与展示均保留原始错误码，失败详情文案区分容量或速率限流与额度用尽。

**升级说明**

本次不涉及数据库 schema 变更（维持 schema v22）或配置项变化，可从 0.6.3 直接平滑升级。

安装与备份方法见[部署指南](https://github.com/Seia-Yurizono/ciallopa/blob/v0.6.4/docs/DEPLOYMENT.md#image-tags-and-upgrades)。
