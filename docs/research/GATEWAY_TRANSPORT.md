# 通用网关的网络设计：取舍与可吸收部分

本轮围绕 NGINX、HAProxy、Envoy 核心与 Pingora，目标是改进 ciallopa 对 Codex 为主的长流、WS turn 和多层重试的承载。它们提供传输机制，不直接证明理解 Responses 的终止事件、账号切换或计费用量；这些仍属于 AI 协议适配层。本篇不作吞吐排名，也不建议为了“网关化”更换当前 Rust 数据面。

## 证据范围

抓取日期 **2026-10-23 UTC**；外部版本固定如下，ciallopa 的现行契约见[架构](../ARCHITECTURE.md)。标记含义：**文档**是官方明确约定；**源码**是该固定版本的实现观察；**既有实测**只引用本仓库已记录实验；**建议**尚未实现或验证。本轮未部署这四款产品对测。

| 产品 | 本轮固定基线 | 来源性质 |
|---|---|---|
| NGINX | `release-1.31.8`，`140aa942711868a8daeb95810bc9a81f272ada72` | 抓取时 GitHub latest release；代理实现固定到该 tag；nginx.org 指令页为滚动文档，按抓取日理解 |
| HAProxy | `v3.4.0`，`64a335366dc1bb5431e28a75e3a22e4226ac9b8f` | 选择稳定 3.4 系列的固定基线，不声称是该系列最新维护补丁；配置与管理手册均取自 tag |
| Envoy | `v1.40.1`，`86af01fdec40c31d025c495a4aa303c4126b2fd0` | 抓取时 GitHub latest release；文档、API 注释与实现固定到 tag |
| Pingora | `0.9.0`，`702f69015e53f7244d6ad2e743de571d859a70a4` | 抓取时 GitHub latest release；Rust 框架，不是同等开箱即用的配置型网关；源码与用户指南固定到 tag |

版本化来源是可复核入口。适配新 Agent/SDK 时重新核查，不把产品名称当作永久行为承诺。

## 1. 流式的核心是传播背压，并保留退出通道

**文档/源码：**

| 产品 | 实际机制 | 对 ciallopa 的启示 |
|---|---|---|
| NGINX | 默认响应缓冲可溢出到临时文件；`proxy_buffering off` 立即转发并受单次 `proxy_buffer_size` 读取限制。WS Upgrade 另走隧道；升级后实现分别维护上游读、上游写、下游写事件计时。[N1][N2][N3] | SSE 前置反代应明确流式配置；“关闭缓冲”仍有 socket/协议缓冲。WS 不能只复用 HTTP 响应超时的概念。 |
| HAProxy | 正常转发与需重放的请求缓冲分开；开启 `retry-on` 的额外条件会复制整请求，装不进单个缓冲的请求不重试。`tune.bufsize` 与并发资源一起决定成本。[H1] | 额外观察、重试资格、必要缓存分别预算；不能为了日志或失败恢复让所有生成结果完整驻留。 |
| Envoy | 流式 L7 filter 可以转发任意长 Body；需要完整 Body 的 filter 会改变内存/拒绝语义。下游超过高水位会暂停上游读取，低水位恢复；上行也传播到下游 codec，并有暂停/恢复计数。[E1][E2] | 一端停读时让生产端减速，而不是持续缓存。观察器应保持有界并允许降级为“未知”，不妨碍原字节转发。 |
| Pingora | H1 代理用两个有界任务通道；先 `try_reserve()`，有容量才继续读请求。源码明确防止上下游同时等待向已满通道发送造成死锁。[P1] | 拆分双向 relay 时，不能只把一个循环变成两个 task；还需保证容量、取消、关闭和最终记录仍有唯一归属。 |

**当前代码：**HTTP `async_stream` 的 `mpsc::channel(16)` 已形成背压，producer 独立监听 Body 关闭与 Force；WS 下行仍在同一 `select!` 分支中等待 `send().await`，上行另有有限预取取消。因此下行写阻塞时，上游读取、idle 与常规关闭通知可能暂时都不能被该循环处理；外层 `finish_on_shutdown` 提供 Force 后的有界退出。[C6][C2]

**当前应用：**上行停读时已通过有限预取处理客户端 Close/FIN；慢读恢复和 Force 有定向验证。
下行写等待仍保留背压。进一步调整以[性能测量](../PERFORMANCE.md)的真实 socket 证据为依据，
不以并发数或通道槽数承诺进程字节上界，也不因合法慢写引入统一短期限。

## 2. 超时按阶段命名，不能用一个 deadline 解释一切

| 语义 | 官方例子 | 与 ciallopa 的关系 |
|---|---|---|
| 建连期限 | NGINX `proxy_connect_timeout`；HAProxy `timeout connect`；Pingora 分 `connection_timeout` 与 `total_connection_timeout`。[N1][H1][P2] | 只覆盖该实现规定的连接阶段，不天然包含账号锁、凭据刷新或全部重试等待。 |
| 收齐请求头/请求体期限 | HAProxy `timeout http-request` 默认仅头，配合 `http-buffer-request` 才含 body；Envoy HCM `request_timeout` 指收齐客户端请求流。[H1][E3] | 名字相近也不等于完整模型调用总时长。不要把上传慢和模型生成慢放入同一个错误码。 |
| 全响应/最大流寿命 | Envoy route `timeout` 从完整请求到完整响应，默认 15 秒；官方特别提示流式接口应关闭或足够放宽。`max_stream_duration` 是另一种绝对生命周期限制。[E3] | 保留当前“准备预算不收正常生成时间”的设计；不引入默认模型整轮硬超时。 |
| 上游无读取进展 | NGINX `proxy_read_timeout` 是相邻读取操作之间，非完整响应时长。[N9] | 当前 HTTP idle 包住 `up.next()`；WS idle 在等待 turn 时基于上游帧重置。两者都不能声称覆盖所有下游写停滞。 |
| 双向流无活动 | HAProxy `timeout tunnel` 在升级后覆盖 client/server 超时，针对双向都无活动；Envoy stream idle 观察上下游流活动。[H1][E3] | Codex 业务 turn 的上游 idle 与通用 WS 连接空闲有不同边界。长连接两轮之间空闲，不等于模型卡住。 |
| 写入期限 | NGINX `proxy_send_timeout` 是相邻上游写操作之间；向客户端发送的 `send_timeout` 是另一方向。[N10][N4] | `timeout(T, sink.send(whole_frame))` 限制整次发送。即使底层持续传字节，也可能超时；不能命名为 write-progress timeout。 |

**重要的源码差异：**Pingora 0.9.0 H1 的 `write_timeout` 包住整次 `write_all(header)` 或 `do_write_body(buf)` future，并非每次底层成功写字节就重新计时。其 API 名称不能作为给 ciallopa 整帧 `send()` 套超时的依据。[P3] Envoy HCM 的 `stream_idle_timeout` 也不应未经 codec 层核查就说成 TCP ACK 进度计时器。

**建议：**保留当前共享 `PreparationBudget`，把正常传输阶段的诊断分为“等上游数据”和“等下游接收”，先通过内部日志/探针辨别，不急着增加用户可调参数。若以后需要真正的写入无进展期限，必须在能观测实际写进度、且取消不会重发部分帧的层实现，并同时测试持续慢速前进与完全停滞。[C3]

## 3. 重试资格、单请求预算和全局重试负载是三件事

**文档/源码：**

- NGINX 只能在尚未向客户端发出内容时转下一个上游；默认不重放已发给上游的非幂等请求，`non_idempotent` 是明确放宽。请求关闭缓冲且 body 已开始上送时，也不能自由重放；`proxy_next_upstream_tries` 与 `proxy_next_upstream_timeout` 分别限制次数和可转发时间。[N11][N12]
- HAProxy 默认 `retry-on conn-failure`，额外重试条件要求应用能容忍重放或具备防重机制。其手册明确指出：对昂贵处理做 response-timeout 重试会放大故障。[H1]
- Pingora 默认 `error_while_proxy` 对非幂等方法或截断的重试缓冲禁用重试；覆盖 hook 即自行接管安全判断。连接失败 hook 与已连接后的失败 hook 分开。[P4]
- Envoy route deadline 包含全部重试与 backoff，`per_try_timeout` 只用于尚未向下游提交响应的尝试；cluster `retry_budget` 则约束**并发重试数量**，默认按 active+pending 请求比例计算，可另设窗口，和“剩余几秒”不是一类预算。[E3][E4][E5]

**对本项目的取舍：**保留已实现的“输出提交前才允许协议认可的回放、同一准备预算跨尝试累加”。但**尚未向客户端输出，只是回放的必要条件，不证明上游未执行或未计费**。Responses POST/WS `response.create` 的回放属于 Codex 语义适配，不能泛化成所有厂商所有 POST 的透明重试。

**当前应用：**已记录失败握手候选与当前 turn 的终止元数据。更完整的 WS 尝试链和阶段耗时由具体排障需求推动，不补造尚未采集的字段。Agent、前置代理与网关重试分别计数；外层代理不对模型 POST 配置泛化 5xx 重放。只有观察到故障期尝试放大，才评估全局并发重试预算。

## 4. 连接池按身份与传输配置划界，别把缓存上限当并发上限

**文档/源码：**NGINX 的 keepalive 数是每 worker 保留的空闲连接上限，并非总连接上限。HAProxy 按地址、协议、连接名/SNI 等属性判断复用资格；`safe`/`aggressive`/`always` 的区别包含陈旧空闲连接重用风险。Envoy 的 pool 还受 worker、协议、优先级和 transport socket options 等分片影响；H2 可多路复用，但仍要区分 connection、active stream 和 pending request 上限。Pingora connector 优先找兼容的 H2/H1 可复用 session，`Peer::reuse_hash` 是复用边界。[N5][H2][E6][P2][P5]

**当前代码：**ciallopa 每账号持有独立 reqwest Client，出口/连接配置更换后重建 pool，而在途请求保留旧 client；配置代次还用于 WS 重连。这个边界比“全局一个连接池”更符合当前账号隔离与 Codex 传输一致性目标。[C4]

**建议：**先观察新建连接/复用、准备等待与出错阶段，不凭低 TPS 直接调整 H2 window、keepalive 或连接数。连接池重构必须保留身份、Cookie、出口和 TLS 行为边界；本轮没有证据支持迁移到 Pingora，也没有重跑 TLS/H2 指纹实验。

## 5. 过载与优雅退出：先保护在途，再保证进程最终能退出

**文档：**HAProxy 在 server `maxconn` 之后可排队，并用 `timeout queue` 有界等待；soft-stop 可由 `hard-stop-after` 收尾。Envoy 将上游 circuit breaker（connections、pending、requests、retries）与进程 overload manager 分开；过载动作包括新请求立即 503、停止 accept、关闭空闲连接、缩短指定超时，不能认为一个 limit 能替代其他资源约束。[H1][E4][E7]

NGINX QUIT 与 reload 会让旧 worker 停止接受新连接、处理既有连接，并提供 `worker_shutdown_timeout`。Envoy listener draining 与进程退出是不同动作，官方说明默认直接 shutdown 不能等同于提前执行 graceful drain；HTTP/2 GOAWAY 与 WS/CONNECT 隧道处理也有区别。Pingora 的 graceful upgrade 通过传递监听 FD 接替新连接，并给旧请求 grace period，**不迁移正在生成的流或 WS 会话**。[N6][N7][E8][P6]

**当前代码：**`admission` 从 Body 提取前一直持有租约到 HTTP EOF/drop 或 WS 关闭；无隐藏准入队列，满额返回 503 和 `Retry-After: 1`。`Drain` 以 Running → Draining → Force 跟踪请求、WS 和 producer；这与项目轻部署目标相符。该上限按 HTTP 请求/WS 连接计数，不是 HAProxy 物理连接数、Envoy HTTP/2 stream 数或 WS 活跃 turn 数的可互换指标。[C1][C5]

**当前应用：**保留 fail-fast，活动 SSE/WS 的正常 drain 与 Force 已有 Compose 探针。
镜像替换、数据恢复和活动流取消分别验收；不把它们合称为无损迁移。见 [TESTING.md](../TESTING.md)。

## 最少但有用的观测与后续顺序

HAProxy 默认在 stream 结束时记日志，将排队、连接、响应头等待、总活跃时长分开；NGINX 提供 upstream connect/header/response 时长并可保留多次上游尝试；Envoy 单独计重试成功/溢出及 flow-control 暂停/恢复。[H3][N8][E2][E5] 这些不是 token 首延迟：HTTP 响应头到达不能替代 TTFT。

对 ciallopa，优先围绕现有 SQLite 请求详情与少量聚合指标扩展，不为每条请求创建高基数 metric label。请求 ID、账号、上游 request ID 留在记录/日志；面板使用在途数、拒绝数、结果分类、可测时长与明确样本量。未观测到的阶段用空值，不能记 0，也不能由总耗时相减“算出”模型计算时间。

双向停读、活动流关停、握手候选、客户端恢复和基本终止诊断已经有相应验证。
接下来由持续混合负载和实际排障需求决定字节预算、上游并发隔离或连接池调整，
不因通用网关规模更大而更换当前框架。现行契约见 [ARCHITECTURE.md](../ARCHITECTURE.md)。

[N1]: https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_buffering
[N2]: https://github.com/nginx/nginx/blob/release-1.31.8/src/http/ngx_http_upstream.c#L3713-L3903
[N3]: https://nginx.org/en/docs/http/websocket.html
[N4]: https://nginx.org/en/docs/http/ngx_http_core_module.html#send_timeout
[N5]: https://nginx.org/en/docs/http/ngx_http_upstream_module.html#keepalive
[N6]: https://nginx.org/en/docs/ngx_core_module.html#worker_shutdown_timeout
[N7]: https://nginx.org/en/docs/control.html
[N8]: https://nginx.org/en/docs/http/ngx_http_upstream_module.html#variables
[N9]: https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_read_timeout
[N10]: https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_send_timeout
[N11]: https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_next_upstream
[N12]: https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_request_buffering
[H1]: https://github.com/haproxy/haproxy/blob/v3.4.0/doc/configuration.txt
[H2]: https://github.com/haproxy/haproxy/blob/v3.4.0/doc/configuration.txt#L9082-L9175
[H3]: https://github.com/haproxy/haproxy/blob/v3.4.0/doc/configuration.txt#L28795-L28945
[E1]: https://github.com/envoyproxy/envoy/blob/v1.40.1/docs/root/faq/configuration/flow_control.rst
[E2]: https://github.com/envoyproxy/envoy/blob/v1.40.1/source/common/router/upstream_request.cc#L791-L840
[E3]: https://github.com/envoyproxy/envoy/blob/v1.40.1/docs/root/faq/configuration/timeouts.rst
[E4]: https://github.com/envoyproxy/envoy/blob/v1.40.1/api/envoy/config/cluster/v3/circuit_breaker.proto
[E5]: https://github.com/envoyproxy/envoy/blob/v1.40.1/docs/root/configuration/http/http_filters/router_filter.rst
[E6]: https://github.com/envoyproxy/envoy/blob/v1.40.1/docs/root/intro/arch_overview/upstream/connection_pooling.rst
[E7]: https://github.com/envoyproxy/envoy/blob/v1.40.1/docs/root/configuration/operations/overload_manager/overload_manager.rst
[E8]: https://github.com/envoyproxy/envoy/blob/v1.40.1/docs/root/intro/arch_overview/operations/draining.rst
[P1]: https://github.com/cloudflare/pingora/blob/0.9.0/pingora-proxy/src/proxy_h1.rs#L531-L617
[P2]: https://github.com/cloudflare/pingora/blob/0.9.0/pingora-core/src/upstreams/peer.rs
[P3]: https://github.com/cloudflare/pingora/blob/0.9.0/pingora-core/src/protocols/http/v1/client.rs#L120-L181
[P4]: https://github.com/cloudflare/pingora/blob/0.9.0/pingora-proxy/src/proxy_trait.rs#L577-L622
[P5]: https://github.com/cloudflare/pingora/blob/0.9.0/pingora-core/src/connectors/http/mod.rs#L101-L136
[P6]: https://github.com/cloudflare/pingora/blob/0.9.0/docs/user_guide/graceful.md
[C1]: ../../crates/forward/src/admission.rs
[C2]: ../../crates/forward/src/ws.rs
[C3]: ../../crates/forward/src/budget.rs
[C4]: ../../crates/upstream/src/lib.rs
[C5]: ../../crates/upstream/src/drain.rs
[C6]: ../../crates/forward/src/proxy.rs
