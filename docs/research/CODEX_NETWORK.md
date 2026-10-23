# Codex 网络设置与网关契约

研究基线为 2026-10-23 的官方资料及固定源码：
Codex 0.163.0 `83086188ae0614bd882818ff1aa23f3194962012`、
0.164.0 `6aaac694803004a63c6f393d153c96d22521a6b9`。
本地实际客户端探针使用官方 0.164.0；项目传输依赖/指纹参考仍单独固定，不能由客户端烟测推导兼容迁移完成。

本文保留可复核的设置、协议与恢复成本，不维护每轮执行日志。
入口见 [RESEARCH.md](../RESEARCH.md)，运行方法见 [TESTING.md](../TESTING.md)，
当前故障语义见 [RECOVERY.md](../RECOVERY.md)。没有连接真实模型供应商。
滚动官方文档不自动代表所有旧版行为；比较的默认 retry 值相同也不意味着两版网络实现完全相同。

## 一、必须分开的网络平面

1. **模型请求**：`model_provider` / `model_providers.<id>` 决定 API 基址、身份、协议、请求头和部分超时。
2. **Codex 自身其他服务**：登录、模型目录、遥测、插件、应用、语音 sideband 等各有路径；改模型基址不等于全部经网关。
3. **MCP 与工具**：远程 MCP 有自己的 URL、凭据及超时；本地命令在执行环境联网。
4. **沙盒命令网络**：`features.network_proxy`、permissions 网络域规则、管理员 `experimental_network`
   针对受支持的执行路径，不是模型网关设置，也不是所有工具出站的总开关。

[官方网关兼容说明][compat]明确写道：模型网关不会路由 Codex 发出的每一条连接。
因此“模型请求走 ciallopa”和“Agent 所有流量走代理”应分别配置、验收。
本研究不建议通过关闭沙盒或扩大工具网络权限来修复模型连接。

## 二、最相关的设置与实际语义

依据[配置参考][reference]、[高级配置][advanced]及固定 [provider 源码][provider]。

| 设置 | 固定版本语义 | 对 ciallopa 的影响 |
| --- | --- | --- |
| `model_provider`、`model_providers.<id>.base_url` | 选择独立 provider；自定义 ID 不得复用保留 ID `openai` / `ollama` / `lmstudio` | 使用 `cpa` 等独立 ID 和 `/v1` 基址 |
| `wire_api` | 当前只支持 `responses`，也是省略时默认值 | 只有 Chat Completions 的网关不能直接替代 |
| `env_key` | 从进程环境取得网关 Bearer 凭据；缺失/空值报错 | 客户端密钥与网关持有的上游账号分开 |
| `requires_openai_auth` | 自定义 provider 默认 false；true 启用 OpenAI/ChatGPT 身份路径 | 现有 ciallopa 账号池模式保持 false，不能直接套企业透传配置 |
| `auth.command` / `timeout_ms` / `refresh_interval_ms` | 命令 stdout 提供 Bearer；默认 5s 命令期限、300s 主动刷新，刷新间隔0表示只在认证重试后刷新 | 可作为未来短期网关凭据接入，不应同时配置 `env_key` 或 `requires_openai_auth` |
| `http_headers` / `env_http_headers` | 静态头 / 从环境变量读取头值；环境变量缺失或为空时不加对应头 | 不把上游密钥写进普通路由 header；记录 header 时单独考虑脱敏 |
| `supports_websockets` | 自定义 provider 默认 false；内置 OpenAI 为 true；会话 fallback 可以再次禁用 WS | 显式启用后仍需检查实际协议，不能由配置值推定没有回退 SSE |
| `websocket_connect_timeout_ms` | 固定源码有此字段，默认 **15000ms**；包住一次 WS 连接调用（含握手），之前的 header 准备在计时外 | 可能先于网关内部 30s 拨号/600s 准备预算到期，不能解释为客户端全部准备时间上限 |
| `stream_idle_timeout_ms` | 默认 **300000ms**；SSE 等待完整事件，WS 等待 pump 交付的消息，单纯 Ping/Pong 不刷新这层 idle；同一值还包住 WS 请求整次 `send` | 不是统一的“每收到一点字节就重置”，也不是每 token 超时 |
| `request_max_retries` | 默认4，源码上限100；底层循环包含首次，总计最多5次该层 HTTP 尝试 | 不等于网关 `max_attempts=1`；两者计数口径不同 |
| `stream_max_retries` | 默认5，上限100；外层流/采样恢复策略使用，WS 失败也可进入此层 | 不只是 SSE TCP 重连，也不能单独作为整个 turn 总请求上限 |
| `features.unbounded_connection_retries` | 0.163/0.164 源码默认开启；特定顶层采样 `ConnectionFailed` 可按5s起、倍增至60s持续重试 | `stream_max_retries=0` 不等于所有网络错误立即停止；隔离故障实验须显式关闭该特性 |
| `capabilities.remote_compaction` | 自定义 provider 需明确能力；`v2` 在 Responses 内传递压缩请求 | 这是协议能力，不是压缩网络字节；保留会话与加密内容归属 |

provider 设置放用户级配置。当前官方文档明确：项目 `.codex/config.toml` 中的 provider、认证等机器级设置会被忽略；
配置解析成功、项目里有字段，不证明它实际控制模型连接。管理策略和承载应用还可施加约束。
固定源码有 `websocket_connect_timeout_ms`，但抓取时的滚动参考表未逐项列出它；以部署版本 schema/源码核对。
连接计时的实际应用见 [client.rs][connect-call]；持续断网重试的默认开关见 [features][features]，
错误是否终止与服务端建议解析分别见 [error.rs][error-policy]、[retry_after.rs][retry-after]。

### 适用于当前账号池模式的最小配置

用户级 `$CODEX_HOME/config.toml`（默认 `~/.codex/config.toml`）：

```toml
model_provider = "cpa"

[model_providers.cpa]
name = "ciallopa"
base_url = "http://127.0.0.1:8317/v1"
env_key = "CPA_KEY"
wire_api = "responses"
requires_openai_auth = false
supports_websockets = true
```

`CPA_KEY` 由启动该客户端的环境提供，模型选择使用该部署实际支持的名字。
这里展示接入结构，不在研究中修改用户配置、客户端版本或账号。
`supports_websockets=false` 可用于独立验证 HTTP/SSE；远端地址使用实际 TLS 入口。
普通接入先保留默认重试和时限，出现已定位问题后再调；不要先把所有超时放大。

可重复故障实验中才使用 `request_max_retries=0`、`stream_max_retries=0`，并在 `[features]`
里关闭 `unbounded_connection_retries`。该配置用于看清单次失败，不作为日常推荐配置。

## 三、重试不是一个数字

[HTTP retry 源码][http-retry]的 `max_attempts` 字段实际以 `0..=max_attempts` 循环；
provider 将 `request_max_retries` 传入它，所以默认4表示首次加4次重试。
该层默认重试5xx和传输错误，**不直接重试429**；429之后是否再次采样取决于上层错误类型，
`usage_limit_reached` 等终止错误不能等同于短时 rate limit。

[外层恢复][stream-retry]又处理流错误、服务端建议、WS→HTTP fallback 和特定断网恢复。
WS预算耗尽后切到HTTP会重置该层计数；WS建连返回HTTP426也有直接fallback分支。
只在“纯HTTP、同一可重试错误、每次耗尽内层、没有额外路径”的简化场景，默认配置才可能形成
`(4+1) × (5+1) = 30` 次请求。它不是所有 turn 的固定值或总上限；工具后续采样也不能混算为一次网络重试。

服务端 `Retry-After` 是有语义的建议；固定客户端能解析秒数和 HTTP-date，并用期限避免同一次建议重复等待。
`server_is_overloaded` / retry-limit 无服务端建议时，上层可直接终止；加一个建议可能改变整个恢复行为。
当前 ciallopa 对客户端429/5xx仅保留1–20秒的整数建议，长值/日期等移除；内部等待还有不同上限。
这是一项已有产品取舍，不应写成所有客户端的通用最佳实践。

**首轮吸收方向**：先实测 Codex + 网关的总尝试数/等待时间，再决定策略；同一次客户端操作的诊断应能区分
客户端重发、网关本地尝试与账号切换。复用 HTTP attempt 元数据，补 WS 最终候选归因。
后续候选的握手取消归因与实际恢复成本见第七节；没有因此添加全局重试引擎或统一关闭日常客户端恢复。

## 四、SSE 心跳、WS 写入与反向代理

[SSE 处理器][sse]先将字节流转为 `eventsource`，再给 `stream.next()` 加 idle timeout。
仅发送 `: keepalive` 注释或不完整 `data:` 分片，可以让网关的逐字节/chunk时钟继续走，却不能保证 Codex 的事件时钟刷新。
因此反向代理不停收到字节、网关没有 idle 错误，客户端仍可能报 `idle timeout waiting for SSE`。

本轮官方0.164.0 CLI的隔离对照将 idle 设为250ms，两条流都先发 `response.created`，之后每50ms写一次：

| 输入 | 实际结果 |
| --- | --- |
| 仅 SSE 注释心跳，计划0.9s后完成 | 客户端 exit1，报告 SSE idle；服务端约0.35s观察到断开，仍有多次成功写入 |
| 完整 `event: ping` + `data: {}`，0.9s后返回正常终止 | 客户端 exit0，收到最终 `probe ok` |

这是事件/字节边界的功能证据，不是延迟SLO。不要为了“保活”向真实协议捏造业务事件；
应观察真实上游事件、合理设置客户端等待，并说明这些计时器的不同。

[WS 处理器][ws]也把同一个 idle 数值包住一次 `send(Message::Text(...))`，这是**整次发送期限**，
并非每次写入进度重新计时。不能用这个实现为网关统一整帧短超时背书。
其底层 pump 自行处理 Ping/Pong，不将它们送入等待响应消息的队列；单纯控制帧心跳也不保证这层 idle 重置。
网关的 WS 上行停读、客户端取消、慢读恢复应各有小型 socket 验收；详见[通用传输研究](GATEWAY_TRANSPORT.md)。
普通停止与 `response.interrupt` 不能直接等同：后者有调用路径和模型能力条件。
后续真实客户端分别验证了停止后的断连接，以及能力开启时 steering 的原生消息；详见第七节。

## 五、代理、证书与认证边界

固定源码的[出站代理解析][proxy]区分 HTTP、HTTPS 与 WSS：环境分支中，HTTPS优先`HTTPS_PROXY`再`ALL_PROXY`，
WSS另有`HTTP_PROXY`回退，HTTP/WS使用`HTTP_PROXY`再`ALL_PROXY`；支持相应大小写变量及`NO_PROXY`。
这是具体解析分支，不保证所有路径都忽略系统代理：`respect_system_proxy` 默认关、
`system_proxy_fallback` 默认开但只适用于受限 bootstrap 路径，OS与管理策略还会影响选择。
不把 SOCKS、PAC、Windows认证或各种 `NO_PROXY` 写法都声明为已实测。

[自定义CA源码][ca]支持 `CODEX_CA_CERTIFICATE`，回退到`SSL_CERT_FILE`；HTTP与WS有共享的CA构建路径。
它解决信任根，不是客户端 mTLS 身份配置。没有验证企业代理、mTLS、真实TLS或各OS系统代理。

[官方 ChatGPT 经企业网关方案][chatgpt-gateway]另有双凭据模式：
`Authorization` 保留用户ChatGPT token，网关凭据用另一个header或native gateway OAuth cookie，
网关验证并剥离自己的凭据后透传用户身份。当前 ciallopa 则验证自己的客户端key并替换为账号池身份，
二者的账号归属和信任模型不同。**当前不支持该企业透传方案**，不要只把 `requires_openai_auth` 改成true。
voice / WebRTC / sideband也不因普通Responses通了就自动支持。

## 本地恢复成本与对话契约

恢复场景均显式关闭 `unbounded_connection_retries`，网关 `transient_cooldown_secs=0`；
除表中“默认 hold”对照外，设 `cooldown_wait_secs=0` 以分离客户端恢复与网关等待。
表中的 HTTP/stream 数字分别是 `request_max_retries` / `stream_max_retries`，不是总尝试数。
这些是特定错误序列和配置下的功能证据，耗时含进程启动与本机调度，不是生产延迟 SLO。

| 场景及主要配置 | 实际结果 |
| --- | --- |
| HTTP 一次503后成功，1/0 | 两次上游请求、两条记录，成功 |
| HTTP 持续503，1/1 | 四次请求后失败，约0.77s |
| HTTP 持续503，保留常规默认4/5 | 三十次请求后失败，约24.79s；仅证实这个纯HTTP耗尽场景 |
| HTTP 503后成功，1/0，`Retry-After: 1` | 两次请求；服务端建议的约1s等待得到遵守 |
| 同一HTTP场景，建议120秒或远期HTTP-date | 网关移除该建议后，客户端采用自身短退避，两次请求约0.37s完成；不表示 Codex 本身不支持日期 |
| 普通429，0/1，建议1秒，网关不 hold | 网关冷却下限为10s；客户端约1s后重发得到本地 `all_cooling` 503，最终失败。两次客户端请求、一次上游请求 |
| 同一普通429，恢复默认 `cooldown_wait_secs=20` | 第二次客户端请求在网关等剩余冷却，约10.24s完成；两次客户端请求、两次上游请求 |
| `usage_limit_reached` 429，1/1，建议1秒 | 一次请求即终止；不能与普通429的恢复语义混用 |
| WS 一次503后成功，0/0 | 网关进行了两次握手，业务成功；成功握手不另外计一条业务记录 |
| WS 持续503后回退，0/1 | 十五次上游握手：预热五次、业务两轮各五次；随后一次SSE成功。共四条网关请求记录，约9.34s |
| WS 426，0/0 | 一次握手后直接回退SSE并成功 |
| WS 延迟握手，客户端connect=250ms，0/0 | 预热与业务各一次取消，随后SSE成功；两个握手取消记录及上游准备终止均验证 |
| WS 延迟16s，保留客户端默认15s connect，0/0 | 两次握手分别约14995/14997ms取消，随后SSE成功，总计约30.16s；不是整个turn只有15s预算 |
| WS 503建议120s，网关 preparation=1s，0/0 | 预热与业务各因准备预算耗尽结束，记录 `preparation_timeout`；随后SSE成功，总计约2.17s |

握手取消不只检查最终成功：断开后必须释放准入槽、停止假上游正在等待的握手，并写入
`transport=ws`、`status=0`、`error=client_closed`、`err_source=client` 与已选账号。
准备预算到期则保留 `504 / preparation_timeout / network`，不能误记为客户端取消。
两者最后的SSE成功行不能替代握手行的验收。

这批结果支持保留当前恢复分工，并补齐取消与尝试归因；没有依据统一放大超时或叠加新的重试层。
短建议与网关冷却下限的差异是已观察到的配置取舍，关闭 hold 并不保证同一客户端操作仍能成功恢复。

### 工具、多轮与停止

通过官方 binary 的 `app-server --stdio` 建立临时 thread；本地 driver 回答 `test_echo` 动态工具，
没有运行真实 shell 工具。SSE 与 WS 均完成工具调用→工具结果→续答→第二轮→停止→再次续答。
验收实际协议、非空且一致的 session、body 中的 turn ID、匹配的 `call_id`，以及 WS
每一次 `previous_response_id` 与上一条真实响应 ID 的精确关系，不以“字段非空”代替关联正确。

**普通 `turn/interrupt`**：0.164 本次实测中，WS 连接被关闭，usage 记录 `client_closed`；
下一轮新建 WS，不带旧 `previous_response_id`，携带完整历史。SSE 同样中止当前流后重新请求。
因此用户停止不应被描述成必然发送 `response.interrupt`。

**能力开启的 `turn/steer`**：固定源码的[主会话处理][turn-interrupt]仅在模型
`use_responses_lite=true` 时发送 `ResponseInterrupt::Drain`；[内置模型目录][model-catalog]
中的 `gpt-5.5` 为 false。本次用显式临时 `model_catalog_json` 声明一个 synthetic 能力画像，
由真实 CLI 触发该分支。mock收到匹配响应ID的 `response.interrupt`，发出
`response.interrupt.accepted` 与 `response.incomplete(reason=interrupted)`；
真实客户端随后在同一连接用准确的响应ID增量续答。此处未单独观测ack的下行到达，
不能用续答成功推断客户端强制验收了ack。这个场景验证该原生分支的本地往返，不是日常配置建议或默认行为证明。

`Drain` 与 `Cancel` 还要区分：[WS 实现][ws]只在 `Cancel` 路径强制等待匹配的中断确认并施加清理期限；
主会话 steering 使用的 `Drain` 不把缺少 ack 本身视为同一种取消失败。
本次mock提供了匹配ack，但没有独立断言其下行到达，也没有验证后台 `Cancel` 的全部期限、拒绝或缺失ack分支。

steering 的中间采样记录为 `status=200`、`error=null`、`incomplete=interrupted`，保留上游用量；
同一个 Codex turn 内的下一次采样随后成功。不能因为客户端发过中断消息，就把整个 turn 推断为失败，
也不能把多个采样与一次HTTP请求或一个WS连接等同计数。

## 验证边界

这些场景只证明固定客户端与假上游的契约。占用、取消、调用 ID 和历史保留有实际检查；
伪造的 encrypted_content 不证明供应商加密有效性或跨账号可迁移。
真实模型、计费、远程 compaction、企业代理和长时间在线恢复仍需针对性验收。
新一轮执行结果由探针 manifest 保存，不把历史 PASS 表作为当前源码的持续保证。

[reference]: https://learn.chatgpt.com/docs/config-file/config-reference
[advanced]: https://learn.chatgpt.com/docs/config-file/config-advanced
[compat]: https://learn.chatgpt.com/docs/enterprise/gateway-compatibility
[chatgpt-gateway]: https://learn.chatgpt.com/docs/enterprise/sign-in-with-chatgpt-through-a-gateway
[provider]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/model-provider-info/src/lib.rs
[http-retry]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/codex-client/src/retry.rs
[stream-retry]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/core/src/responses_retry.rs
[sse]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/codex-api/src/sse/responses.rs
[ws]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/codex-api/src/endpoint/responses_websocket.rs
[proxy]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/http-client/src/outbound_proxy.rs
[ca]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/http-client/src/custom_ca.rs
[connect-call]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/core/src/client.rs#L1333
[features]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/features/src/lib.rs#L1468
[error-policy]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/protocol/src/error.rs#L413
[retry-after]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/http-client/src/retry_after.rs#L20
[turn-interrupt]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/core/src/session/turn.rs#L2801
[model-catalog]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/models-manager/models.json
