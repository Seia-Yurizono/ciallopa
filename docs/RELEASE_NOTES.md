# Release notes — v0.3.0

本版本基于 v0.2.7，新增 SSE / WebSocket 的网关侧下游观测（usage 数据库 schema v14），改进管理页面的请求与额度展示，并统一三个管理页面的中文用语。

**网关侧下游观测**

- 每个 Responses SSE 请求或 WebSocket 轮次可附带一行可选的 `request_obs` 记录：上游首个文本 / 推理 / 工具调用事件、末事件与终止事件时间，SSE 请求体队列的入队与交给 HTTP 层的时间及阻塞时长，WebSocket `sink.send` 完成时间及阻塞时长，上下游载荷字节数，以及两侧 permessage-deflate 的协商结果。
- 数据在内存中聚合，与对应的请求记录在同一事务中写入，不逐块写盘；随请求记录一同清理。除 `response.id` 和上游请求 ID 外，不保存请求体、响应体、令牌或其他请求头。
- 新增只读接口 `GET /usage/v1/records/{id}/observation`。现有导出、`/records` 以及 `ttft_ms`、`duration_ms` 和 TPS 的含义保持不变。
- 所有时间均为网关侧观测，不代表客户端的接收、解析或渲染时间，也不代表套接字刷新或 TCP 确认。SSE 上游字节为 HTTP 内容解码（包括 v0.2.6 起的 gzip 流式解压）之后的字节数。普通 HTTP（非流式）请求没有观测记录。字段定义见 [docs/USAGE.md](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/USAGE.md)。

**管理页面**

- 请求详情：上游与下游两条时间轨道共用同一时间轴；累计阻塞时长只显示数值；分别显示两侧的 deflate 协商结果；精确数值和标识符收在一级展开内。
- 请求列表：耗时条在整页使用统一的线性刻度，并标出 TTFT 段；表头始终显示本页范围和图例。
- 账号页：每个额度条下方始终显示重置时间；无数据显示为虚线“未知”，套餐不含的窗口显示“不适用”；状态同时使用形状和简短文字，并在旁边显示 24 小时内的错误数。
- 费用只显示已定价请求的金额（例如 `$8.70`），不再显示“≥”或行内的未定价标记；全部未定价时显示“未定价”，缺少价格的请求数量在提示中说明。单独的未定价价格提醒保持不变。
- 统一概览、账号和设置三个页面的术语（如账号选择、换账号、并发、请求体缓冲、出口代理、未完成、已恢复），并按代码实际行为修正设置说明：空闲的 WebSocket 连接不占用并发名额；请求体缓冲预算不足时最多等待 1 秒，之后返回 503。

**升级**

- 首次启动时，usage 数据库自动从 schema v13 升级到 v14。此次迁移只新增 `request_obs` 表，不改写已有数据；升级前的记录没有观测数据。配置文件无需修改。
- 旧版本镜像不会降级已迁移的数据库。如需回滚，请在升级前按[备份与恢复](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/DEPLOYMENT.md#backup-and-restore)保存数据目录和当前镜像的离线归档。
- 发布后，`latest` 指向 0.3.0，并新增 `0.3` 别名。按[容器清理策略](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/CONTAINER_CLEANUP.md)，GHCR 只保留最新的两个标签名，固定在 `0.2.7` 或 `0.2` 的部署之后可能无法再拉取该镜像。拉取和重建前，请先更新固定的镜像标签，见[部署指南](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/DEPLOYMENT.md#image-tags-and-upgrades)。

**尚未包含**

- 概览页的新版总览布局目前仍是设计稿。同一首输出事件的上游 / 下游配对采集尚未实现，因此页面不提供“网关额外延迟”一类的对比指标。

**验证**

- 本地 workspace 测试 521 项全部通过，涵盖观测聚合、v13→v14 迁移、SSE/WS 边界和 gzip 解码路径。loadbench 示例 5 项通过。严格 Clippy、格式检查，以及 UI、设置、账号和请求观测渲染四项 Node 检查均已通过。安装包与容器的冒烟测试在标签工作流中运行。
- 依赖锁文件只更新了本仓库各 crate 的版本号，第三方依赖没有变化。验证全部使用合成数据和本地模拟上游，未调用真实模型上游。
