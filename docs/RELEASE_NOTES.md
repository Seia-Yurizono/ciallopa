本次从 0.6.1 升级到 0.6.2，补强资源边界与排障串联，对齐 Codex 0.171.0 客户端契约，并新增只读自检命令。

**排障 ID**

- 非 2xx HTTP 响应与 WebSocket `type:error` 帧带 `x-request-id: cp-<记录 ID>`。Codex 0.171.0 会在报错中显示 `request id: cp-…`，可直接粘贴到管理页请求列表定位记录。
- 上游原有的 `x-request-id` 改名为 `x-upstream-request-id` 下发，`x-oai-request-id` 与上游错误正文保持不变，记录详情仍保存上游 ID。成功响应不改动。Codex 对 400 只显示正文，不显示 request id。

**资源边界**

- 用量查询在调用方取消（例如关闭页面）时中断正在执行的 SQLite 查询并及时归还读槽；百万行库上取消后归还读槽的 p95 从约 1.4 秒降到约 2 毫秒。并发的额度查询共享同一次读取，不缓存结果。
- Images 包装同时限制单个上游事件、保留结果字节、结果项数和序列化输出，超限返回 `image_output_too_large`（未发头时 502，流式已发头时发一个错误事件后结束），不再静默跳过超大事件。

**新功能**

- `ciallopa doctor`：只读自检配置与权限、用量库与 WAL、出口代理与上游 TLS/WebSocket 握手、账号令牌有效期，支持 `--json`，退出码 0/1/2。不刷新令牌、不调用模型、不写数据库。用法见[部署指南](https://github.com/Seia-Yurizono/ciallopa/blob/v0.6.2/docs/DEPLOYMENT.md#self-check-ciallopa-doctor)。
- 设置页保存确认按生效边界分组：下次使用生效、新连接生效、出口变化时 WebSocket 重连（空闲立即、进行中的 turn 结束后以 1000 关闭）、需重启。`GET /admin/v1/config` 的 `effect` 新增 `new_connection`、`ws_after_turn` 两个取值，原有 `hot`/`restart` 不变；只需判断是否重启的脚本应比较 `restart`。

**观测**

- 请求记录新增阶段耗时 `prep_ms`、`cred_ms`、`hold_ms`、`send_to_headers_ms`、`headers_at_ms`、`down_first_content_ms`，用于区分准备、凭据、等待、上游响应头与网关下行交付。

**验证改进**

- `cli-smoke.py` 基线升级到 Codex 0.171.0，新增契约用例（WS 错误帧 ID、1009 回退、turn 结束后 1000 关闭、503 + Retry-After、支出上限不重试、gzip 等），兼容矩阵区分真实上游、本地 mock 与源码推断证据。
- 新增 Linux 持久盘对照实验夹具与结果（`scripts/bench/`）。

**升级说明**

本次将用量数据库升级到 schema v22，新增上述六个可空阶段耗时列，历史记录保持为空。升级前请备份配置和用量数据库；回退应使用升级前备份，不承诺旧版本兼容升级后的数据库。

口径变化：从 schema v22 起，HTTP 请求的 `ttft_ms` 和总耗时从准入时刻开始计算，包含读取请求体与前置解析，因此与 v21 及更早的记录不能直接比较，大请求体的数值会偏大。

本次未对 Codex 0.171.0 实际客户端做 TLS 指纹抓包比对；网关的 HTTP/TLS 依赖未变。

安装与备份方法见[部署指南](https://github.com/Seia-Yurizono/ciallopa/blob/v0.6.2/docs/DEPLOYMENT.md#image-tags-and-upgrades)。
