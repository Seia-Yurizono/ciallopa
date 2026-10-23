# 网关的配置、重试与扩展边界研究

研究日期：2026-10-23。外部版本固定如下；ciallopa 的现行行为以[架构](../ARCHITECTURE.md)为准。
本文覆盖 Caddy、Traefik、Kong、Apache APISIX，并以 LiteLLM 补充 AI 流语义。
目标是支持 Codex 为主、OpenCode 为次的网关演进，不是为项目挑一个替代技术栈。

本轮实际读取官方文档与固定发布源码，没有部署这些产品做吞吐比较，也没有调用真实模型服务。
已有 Caddy 本地链路验收保留原版本和限制，见 [RECOVERY](../RECOVERY.md)。
以下“源码确认”表示静态控制流依据；“文档说明”不是本轮运行验证。

## 结论先行

1. **值得借鉴的是明确的状态与生效边界。** 配置通过校验、存储成功、进入运行态、全部 worker 应用、旧流退出是不同事实。单进程也应分清“保存”和“正在使用”，但不必复制分布式控制面。
2. **重试必须有唯一的协议负责人。** 入口反代处理连接层问题，ciallopa 负责账号、凭据、Responses/WS 状态与重放资格；Agent 自身可能再次重试。不能只增加各层重试次数。
3. **单二进制并不落后。** Caddy、Traefik 的轻部署，以及 Kong DB-less、APISIX standalone 都说明复杂部署是使用场景的选择。当前无需引入 etcd、Redis、PostgreSQL 或 Kubernetes。
4. **观测的先进性在于解释一次请求，而非堆积指标。** 请求、尝试、实际供应商/模型、首输出和流结束应有各自身份与时间；采集失败要可见，但通常不应成为转发的同步依赖。
5. **协议原生能力优先于通用插件。** 一个 HTTP `response` hook 就可能隐含整段缓冲；“支持 WS”也可能只处理升级握手。第二个真实上游出现前，保持 Rust 内部明确职责比先承诺万能插件接口更合适。

## 版本与证据入口

以下为抓取日 GitHub `releases/latest` 返回的发布及其解析后的 commit；不把它解释为所有发行渠道或商业版的最新版本。
尤其 Kong 的官方产品文档覆盖商业版/较新版本，固定源码仅为开源仓库的 3.9.4。

| 产品 | 固定版本 | 完整 commit | 主要机制证据 |
| --- | --- | --- | --- |
| Caddy | v2.11.7 | `72dd0fb067f6d7826c7f79907670ba4a713bfe37` | [配置装载](https://github.com/caddyserver/caddy/blob/72dd0fb067f6d7826c7f79907670ba4a713bfe37/caddy.go#L337)、[反代/重试](https://github.com/caddyserver/caddy/blob/72dd0fb067f6d7826c7f79907670ba4a713bfe37/modules/caddyhttp/reverseproxy/reverseproxy.go#L1394)、[升级连接清理](https://github.com/caddyserver/caddy/blob/72dd0fb067f6d7826c7f79907670ba4a713bfe37/modules/caddyhttp/reverseproxy/streaming.go#L460) |
| Traefik Proxy | v3.7.16 | `af6d927c51191ae1df104aaf907793386b816c28` | [配置分类](https://github.com/traefik/traefik/blob/af6d927c51191ae1df104aaf907793386b816c28/docs/content/getting-started/configuration-overview.md)、[配置聚合](https://github.com/traefik/traefik/blob/af6d927c51191ae1df104aaf907793386b816c28/pkg/server/configurationwatcher.go#L98)、[Retry](https://github.com/traefik/traefik/blob/af6d927c51191ae1df104aaf907793386b816c28/pkg/middlewares/retry/retry.go#L137) |
| Kong OSS | 3.9.4 | `ddc85be461165c9eb4506669343aa0d0c627e687` | [声明式入口](https://github.com/Kong/kong/blob/ddc85be461165c9eb4506669343aa0d0c627e687/kong/api/routes/config.lua)、[LMDB 导入](https://github.com/Kong/kong/blob/ddc85be461165c9eb4506669343aa0d0c627e687/kong/db/declarative/import.lua#L424)、[数据面同步](https://github.com/Kong/kong/blob/ddc85be461165c9eb4506669343aa0d0c627e687/kong/clustering/data_plane.lua) |
| Apache APISIX | 3.19.0 | `473043623c161966f3f6ef4e8f12d2d8667a12d2` | [部署模式/应用确认](https://github.com/apache/apisix/blob/473043623c161966f3f6ef4e8f12d2d8667a12d2/docs/en/latest/deployment-modes.md)、[本地配置同步](https://github.com/apache/apisix/blob/473043623c161966f3f6ef4e8f12d2d8667a12d2/apisix/core/config_yaml.lua)、[balancer](https://github.com/apache/apisix/blob/473043623c161966f3f6ef4e8f12d2d8667a12d2/apisix/balancer.lua) |
| LiteLLM | v1.106.2 | `49943cd52ea46aacf8db2e5cb2b890d580fe9bb4` | [Router](https://github.com/BerriAI/litellm/blob/49943cd52ea46aacf8db2e5cb2b890d580fe9bb4/litellm/router.py)、[fallback 元数据](https://github.com/BerriAI/litellm/blob/49943cd52ea46aacf8db2e5cb2b890d580fe9bb4/litellm/router_utils/fallback_event_handlers.py)、[Responses 流](https://github.com/BerriAI/litellm/blob/49943cd52ea46aacf8db2e5cb2b890d580fe9bb4/litellm/responses/streaming_iterator.py) |

官方滚动页面均实际读取于 2026-10-23；下文单独链接。固定源码用于收窄滚动页面的概括性承诺，
不是证明未来版本不变；可复核入口以本页固定版本链接为准。

## Caddy：简洁部署，认真对待热更新的生命周期

[Caddy 配置 API](https://caddyserver.com/docs/api)把整份装载、局部修改和并发写冲突分开说明。
`POST /load` 等待装载完成或失败；新配置无法运行时保留旧配置。源码先 `run(newCfg, true)`，成功后交换当前上下文，再 Stop/Cleanup 旧应用。
这是一项具体的替换顺序，不等于所有模块完全无外部副作用，也不等于活动请求所有后续读取都固定同一个 revision。

两个短引文尤其有用：

> “Caddy's API does not support transactions spanning multiple requests”

> “By default, WebSocket connections are forcibly closed … when the config is reloaded.”

第一句来自 API 的并发访问说明：`Etag` / `If-Match` 防止同一范围被迟到写入覆盖；多个 HTTP 修改不组成事务。
第二句来自[反代流说明](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#streaming)：升级连接持有旧配置，`stream_close_delay` 延后关闭来缓和集中重连，`stream_timeout` 是整连接寿命。
源码的 `cleanupConnections()` 确认默认立即关闭已劫持连接。它们都不是 WS 单次写入无进展期限。

还需区分**运行生效和重启后恢复**：同一版本源码在交换运行配置之后尝试 autosave，写盘失败只记录日志。
因此不能仅凭 API 成功就承诺磁盘持久化成功。官网 `--resume` 的便利性以可用的 autosave 文件为前提。

反代默认不开重试；启用后，拨号失败与已建立连接后的失败分开处理。
后者默认仅 GET 可重试，可由 matcher 明确扩展；当前版本还有响应 matcher 触发的重试路径。
`lb_try_duration` 在决定再尝试时检查 elapsed，并不取消正在进行的单次 RoundTrip；连接/响应头超时仍是另一层。
这解释了为何“配置一个总重试秒数”不自动等价于 ciallopa 可取消的累计准备预算。

扩展通过编入二进制的 module 和 Provision/Validate/Cleanup 生命周期组织，见[扩展说明](https://caddyserver.com/docs/extending-caddy)。
[Tracing](https://caddyserver.com/docs/caddyfile/directives/tracing)使用 OTel、W3C tracecontext/baggage，并将 traceID/spanID 放进访问日志。
无需因借鉴其关联方式就先引入 Collector。

**对 ciallopa 的选择：** 保留现有版本校验、校验后替换和审计；继续让 Caddy 负责 TLS/入口。
沿用已验收的最小反代示例，不添加无依据的缓冲或重试旋钮。
下一次需要验收的是 **Caddy 配置 reload 时活动 Codex WS 的关闭与客户端恢复**，而不是把“零停机配置”当作长连接保证。
现有 Caddy 验收只覆盖本地 HTTP/1.1 SSE 与取消，未覆盖此 reload 情形，也未覆盖生产 TLS/容量。

## Traefik：配置来源、业务路由和重试中间件各司其职

固定版本文档区分 install/startup configuration 与 routing configuration，后者包含 router、middleware、service 和证书。
这让“需要重启”和“动态改请求处理”在概念上清楚，不要求所有选项一概热更新。

[File provider](https://doc.traefik.io/traefik/reference/install-configuration/providers/others/file/)使用文件事件，建议挂目录，避免替换单个 bind-mounted 文件后监听链路断开。
provider 更新默认有 2 秒节流，只保留该窗口内最新事件；这是降噪，不是事务或同步确认。
固定源码 `applyConfiguration()` 先 build 成功才发配置；解析错误不发布该次候选。
配置聚合器保存各 provider 最新快照、去重并 DeepCopy 后通知 listener。
这仅确认**这个失败路径不会发送坏候选**，不泛化为所有语义错误都整份 rollback，或多个 provider 能在同一个原子版本切换。

[Retry 页面](https://doc.traefik.io/traefik/reference/routing-configuration/http/middlewares/retry/)的开头仍写“收到后端响应，无论状态都停止重试”，
但同页及 3.7.16 固定源码已有 `status`、`retryNonIdempotentMethod`、`maxRequestBodyBytes`、`timeout`。
因此不能摘录旧概述便断言“不支持状态码重试”。源码确认：

- 网络错误重试要求到达 backend proxy 且尚未向后端写出请求；认证中间件先行返回的 401 不应被误认成可重试网络故障。
- 配置状态码重试时，POST/PATCH/LOCK 默认排除，明确启用非幂等方法才加入；为重放读取可复用请求体。
- 请求体超过允许缓存大小会关闭 HTTP 状态码重试，保留已窥读字节继续原请求；此路径并非直接拒绝所有大请求。
- 已向客户端写响应或发生 hijack 后停止重试。详细 tracing 为每次操作建 span，并从第二次开始附 `http.request.resend_count`。
- `timeout` 在响应写入决策检查 elapsed；它不是包住整个下游调用的取消 deadline。慢的单次请求和正常流仍需各自传输超时。

[Tracing 配置](https://doc.traefik.io/traefik/reference/install-configuration/observability/tracing/)默认不采集任意请求/响应头，query 参数默认脱敏；按需白名单比先采全量再清洗更清楚。
[固定版本插件文档](https://github.com/traefik/traefik/blob/af6d927c51191ae1df104aaf907793386b816c28/docs/content/reference/install-configuration/experimental/plugins.md)
明确将插件注册放在 install 配置，支持 middleware/provider，仍标注 experimental；运行时配置变化不等于可任意热载插件代码。

**对 ciallopa 的选择：** 借鉴设置页对生效范围的说明，以及 attempts 的可解释性。
不引入 provider discovery：当前是本机账号/配置文件，没有 Docker/Kubernetes 服务发现问题。
不建议让入口反代和 ciallopa 同时对生成 POST 做宽泛状态码重放；须先算清 Agent × 入口 × 网关的实际尝试链。

## Kong：声明式提交与控制面失联的降级契约

[DB-less 文档](https://developer.konghq.com/gateway/db-less-mode/)提出的直接问题很实用：
若先创建 Route 再添加鉴权插件，中间存在 Route 已可用、插件尚未应用的窗口。
整份声明式文件将关联实体一起提交，避免由操作序列产生此类中间态；DB-less 不要求运行外部数据库。

> “Once that file is loaded into Kong Gateway, it replaces the entire configuration.”

3.9.4 源码 `/config` 先解析/校验；导入以一个 LMDB transaction 写入实体和配置 hash，再 purge cache、广播 reconfigure。
这分别是**存储提交**和**worker 运行结构更新**；不能把前一个事务等同于每个活动请求贯穿新/旧版本的全局一致事务。
多节点 DB-less 实例的文件分发也仍需操作者安排，并不会凭一个 YAML 自然成为同步集群。

[Hybrid 文档](https://developer.konghq.com/gateway/hybrid-mode/)明确：已成功拿到配置的数据面在控制面失联后继续使用缓存；有可用缓存时重启也可恢复。
这建立在先有配置、缓存完整且启动依赖可用的前提上，不保证一个全新、无配置节点也能独立恢复。
滚动文档涉及较新版本增量同步等能力，本研究没有把它们写成固定 3.9.4 的已实现事实。

路由实体、upstream/target 的选择、Service 超时和 retry 数量分开。固定 Service schema 默认 retries=5，connect/write/read 各 60000ms，
但 retries 字段并不定义哪些状态可重放，更不证明 AI POST、WS turn 或业务副作用可安全复制。
这些默认值不是 ciallopa 的推荐值，本轮未完整逐项复验 Nginx proxy-next-upstream 的运行组合。

[Plugin handler 文档](https://developer.konghq.com/custom-plugins/handler.lua/)按 access、header_filter、body_filter、log 等阶段组织，并特别警示：

> “If a module implements the response function, Kong Gateway will automatically activate the ‘buffered proxy’ mode”

因此扩展接口会影响流语义，不只是代码组织。文档还区分 ws/wss Service 的帧 hook 与普通 http/https Service 上的 WebSocket upgrade；
后者并不因此运行所有 WS hook。该细节来自当前产品文档，不据此声称全部 WS 专用 hook 都在本轮固定 OSS 版本实跑可用。

固定版本 [OTel 实现](https://github.com/Kong/kong/blob/ddc85be461165c9eb4506669343aa0d0c627e687/kong/plugins/opentelemetry/traces.lua)
将 propagation、采样和批量导出分开：即使禁用采样，也可传播上下文；schema 提供 queue 与导出连接/读写超时。
这有助于把请求关联能力与“每条都保存完整 trace”分离。

**对 ciallopa 的选择：** 管理查询/配置变更失败应有明确提示，已装载、仍有效的转发配置继续服务；
不要让每条请求同步依赖管理 API 或日志数据库查询。凭据确实失效等数据面条件仍按真实错误处理，不是无限 fail-open。
当前保持单进程即可；不为获得这种职责分离先部署独立控制面、PostgreSQL、Redis 或配置分发集群。

## APISIX：确认“已接受”与“已应用”，避免过度承诺原子性

固定版本 [部署模式](https://github.com/apache/apisix/blob/473043623c161966f3f6ef4e8f12d2d8667a12d2/docs/en/latest/deployment-modes.md)
包括 traditional、decoupled、standalone；standalone 又有 file-driven 和 API-driven。
因此“APISIX 必须带 etcd”不准确。

file-driven 将 YAML/JSON 读入内存，默认每秒检查更新，不替换工作进程。
YAML 的 `#END` 是完整写入提示，不是跨资源事务：固定源码先解析文件，再按资源类型执行 schema/checker 和更新；
file-driven 路径会重建该类型列表，非法项不进入列表。文件语法解析失败保留旧表，**单个实体校验失败却不等于整份回滚**。
不应把“文件一次写入”宣传成“所有 route/upstream/worker 都原子切换”。

API-driven 支持资源类型 `conf_version`、个体 `modifiedIndex` 和配置 digest。一个对 UI 很有启发的契约是：

> “202 Accepted … without waiting to confirm it has been applied.”

带 `?wait=<milliseconds>` 时等各 worker（含已启用 stream 子系统）确认，成功才回 200；超时仍回 202，最大等待60秒。
这来自 3.19.0 固定文档，`config_yaml.lua` 的 worker/type applied digest 记录也与之对应。
即使“全部应用”也不承诺已开始的请求全部转用新 revision。
API-driven 启动为空配置，本地文件模式和 API 模式的持久化/恢复方式不能混为一谈。

[Upstream 定义](https://github.com/apache/apisix/blob/473043623c161966f3f6ef4e8f12d2d8667a12d2/docs/en/latest/admin-api.md)
分开 connect/send/read timeout、retries 与 retry_timeout。
固定 balancer 默认在未设置 retries 时使用节点数减一；retry_timeout 在准备下一次选择时检查 deadline，
并不取消已发起尝试或取代读取/写入期限。仍须结合底层可重试条件，不能按字段名想象成通用端到端预算。

[插件开发](https://github.com/apache/apisix/blob/473043623c161966f3f6ef4e8f12d2d8667a12d2/docs/en/latest/plugin-develop.md)
按生命周期及 priority 排序。3.19.0 文档还明确区分 `upstream.scheme=ws/wss` 的逐帧代理与 nginx 透明升级：
前者有 ws_handshake、ws_client_frame、ws_upstream_frame、ws_close，跳过普通 header/body filter，仍有前置与 log 阶段。
这个区分比“插件支持 HTTP/WS”一列布尔值更准确。

[OTel 源码](https://github.com/apache/apisix/blob/473043623c161966f3f6ef4e8f12d2d8667a12d2/apisix/plugins/opentelemetry.lua)
显式提供 sampler、max_queue_size、drop_on_queue_full、batch timeout 与 exporter timeout。
开关存在不等于所有模式默认不阻塞：`drop_on_queue_full=false` 的描述是强制处理批次，部署者仍需选择代价。

**对 ciallopa 的选择：** 单进程无需照搬 worker applied-ack 协议；现在设置页只需准确报告保存失败、已生效与需重启项。
若以后出现异步重建连接池/多实例，再考虑相应 generation/应用状态，而不是提前建立全局事务。
保留当前 usage 有界队列与丢弃计数，未来导出 OTel 也应继承这个“不让观测无限反压转发”的契约。

## LiteLLM：借鉴 AI 尝试语义，但不笼统照搬 fallback

[可靠性文档](https://docs.litellm.ai/docs/proxy/reliability)将同 model group 内 retry、跨 group fallback、deployment cooldown 分开。
固定版本的 fallback helper 携带已消耗重试次数及第一次确定的预算，避免流包装器重新进入 fallback 时凭空获得一套新次数。
同时保留原 model group 与实际路由 deployment 信息；这比只记录最后一个 HTTP 状态更适合解释一次生成。

固定 [`_acompletion_streaming_iterator`](https://github.com/BerriAI/litellm/blob/49943cd52ea46aacf8db2e5cb2b890d580fe9bb4/litellm/router.py#L3056)
让 deployment semaphore 随流持有，EOF/关闭/fallback 才释放，并在 finally 关闭底层流。
这个生命周期与 ciallopa 的 Body/WS 租约方向一致；两者具体准入单位仍不同，不能用部署级槽位等同整条 WS 连接槽位。

**首个 SSE 事件与首个不可撤销输出必须分开。**
固定 Responses 路径暂存有限 lifecycle 事件，fallback 成功后才让客户端看到最终选中 response 的 announcement。
这对 Codex 的 response ID、后续增量状态尤其相关；并不是“尚未看到文本就一定能重放”。

源码也暴露出不能泛化的边界：

- Chat Completions wrapper 在已有 generated content 时拒绝 fallback。
- Responses 的 transport error / 缺终止 EOF 在 `_output_started` 后抛原错误，不重放。
- 但同版本 Responses 明确错误事件可包装为 `MidStreamFallbackError`，Router 的 `_aresponses_fallback_attempt` 仍包含把已生成文本加入 developer/assistant continuation 的分支。

这是不同路径的静态实现差异，**本轮未构造端到端 fixture 验证所有可达组合**，不据此宣称 LiteLLM 所有 Responses 都会续写或所有协议都严格输出前重放。
对 ciallopa，跨账号加密上下文、工具调用和 Responses IDs 不能靠拼接纯文本恢复；继续坚持现有输出提交后的禁止自动拼接原则。

[超时文档](https://docs.litellm.ai/docs/proxy/timeout)同时出现“entire call”“first chunk”及 native pass-through 每次 next chunk 的说明。
这些是按路径/底层客户端生效的不同概念，不能仅看同名 timeout 就假定包含所有 retry/fallback 或已获得严格墙钟上界。
本轮没有完成每个 LiteLLM endpoint 的 timeout 追踪，不拿这些值替换 ciallopa 已定义的准备、响应头和 idle 三种预算。

[Callbacks](https://docs.litellm.ai/docs/observability/callbacks)提供成功、失败、stream hook；固定 `custom_logger.py` 也区分相关入口。
本项目应借鉴失败尝试和最终服务者可追溯，继续保留缺用量为未知：固定 Responses iterator 有断流后估计 usage 的路径，
其估计值不是供应商实际计费，应与本地 usage 的“上游报告”来源区分，不直接拷贝成费用。

**对 ciallopa 的选择：** 优先补最终失败握手账号与 attempt 链，记录一次尝试真正选用了谁及实际出口；
已有 HTTP attempt、record ID、upstream ID 能回答的问题不必换成完整 tracing 平台。
OpenCode 的多供应商需求将来落地时，再把原始 provider/model 与实际 served provider/model 分开；
未经客户端语义验证，不启用自动跨厂商模型替换。

## 对当前项目的取舍

当前已提供客户端分层重试说明、配置生效边界、活动流 drain/Force、Caddy 重载对照、
WS 失败握手候选及有限终止诊断。具体行为以 [ARCHITECTURE](../ARCHITECTURE.md)、
[SETTINGS](../SETTINGS.md) 和 [RECOVERY](../RECOVERY.md) 为准。

继续保持有效运行快照、有限观察与原生协议。更完整尝试链由排障需求推动，
第二上游先验证原生 HTTP/SSE 凭据、错误、用量与取消，再抽共同接口。
不把供应商支持表或插件数量变成需要照搬的功能清单。
