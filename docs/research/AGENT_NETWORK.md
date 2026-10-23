# Agent 网络调研：OpenCode 与 Claude Code

调研日期：2026-10-23。主项目决策仍以 Codex 客户端为先；本文深入 OpenCode，
以 Claude Code 的公开契约作辅助。它们不是同一个协议的三种皮肤。

证据分为 **官方声明**、**固定源码推断**、**本地实测**。没有使用真实供应商凭据，
没有请求付费模型，也没有把 localhost 模拟服务验收写成真实上游兼容验收。
Claude Code 按官方文档研究；公开 issues 仓库及 Anthropic SDK 均不当作 CLI 完整源码。

## 版本与最有影响的结论

- OpenCode：GitHub API 确认当前官方仓库为 [`anomalyco/opencode`](https://github.com/anomalyco/opencode)，
  默认分支 `dev`；本次固定最新 stable release
  [`v1.19.4`](https://github.com/anomalyco/opencode/releases/tag/v1.19.4)，发布于 2026-10-22，
  commit **`148842ef7cb1bc34192b3fff57d0f9721ab9c801`**。
  官网是滚动文档，首页已有 v2 提示，不能用当前官网描述替代这个固定 release 的实现。
- OpenCode 的 **Responses 与 Chat Completions 必须分开选 SDK**；官方 release 实测
  `@ai-sdk/openai` 请求 `/v1/responses`，`@ai-sdk/openai-compatible` 请求 `/v1/chat/completions`。
- OpenCode `openai` 的 **OAuth 插件可能绕过配置的 baseURL**：它按路径将请求改写到
  `https://chatgpt.com/backend-api/codex/responses`。API key 路径和 OAuth 路径不能混写成一份网关教程。
- 这版 OpenCode 默认会话调用主动把 AI SDK 重试设为 `0`，外层负责最多 5 次重试；
  localhost `503` 实测为 **1 次初始请求 + 5 次重试**。不能再叠加 SDK 文档的默认 2 次。
- Claude Code 当前官方契约将响应头、网络字节、可解析事件、body idle 分开；
  `API_TIMEOUT_MS` 是每次尝试等响应头的期限，**不是整个生成的总时限**。
- 修改模型 base URL 不会接管 MCP、OAuth、更新、插件下载、工具访问及所有诊断网络。
  ciallopa 应承诺“模型网关”，不把它描述为 Agent 全流量代理。

## OpenCode：配置如何变成线上请求

### Provider、协议和凭据

[v1.19.4 provider 配置](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/core/src/v1/config/provider.ts#L98)
支持 provider `npm`、`options.baseURL`、`options.apiKey`、headers 和 models；
[resolveSDK](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/provider/provider.ts#L1788)
合并模型 headers、base URL、凭据并安装 timeout fetch，再创建实际 provider SDK。
[官网 custom provider](https://opencode.ai/docs/providers/#custom-provider)也明确了两个 OpenAI SDK 包的端点区别。

| 路径 | 生效方式 | 对 ciallopa 的含义 |
| --- | --- | --- |
| 单独 provider ID + `@ai-sdk/openai` + gateway key | HTTP Responses；实测内置插件启用时仍走配置的 loopback baseURL | 可以隔离内置 `openai` OAuth 的 URL/凭据改写，但请求体仍需 Codex 兼容核对 |
| 单独 provider ID + `@ai-sdk/openai-compatible` | HTTP Chat Completions | 当前 ciallopa 没有该路由，不能仅因名字“compatible”就采用 |
| 内置 `openai` + API key | `sdk.responses()`；内置插件添加 originator/session，并移除 `maxOutputTokens` | 是另一条候选接入路径，须确认实际凭据类型未落到已保存的 OAuth |
| 内置 `openai` + OAuth | 动态刷新 access token；加 `ChatGPT-Account-Id`；重写 `/v1/responses` 或 `/chat/completions` 到 Codex endpoint | 仅设置 baseURL 不保证走网关；不能把网关 key 和 ChatGPT access token 混为一物 |

OAuth 依据：[Codex 插件](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/plugin/openai/codex.ts#L328)。
URL 改写位于该文件 L418 起，插件参数 `codexApiEndpoint` 是实现入口，
本次没有发现它作为普通 provider 配置字段对用户开放；不建议靠内部测试参数写正式接入说明。
独立 provider ID 的办法避免命中 `providerID === "openai"` 的插件钩子，
代价是同时失去这些专门为 Codex 准备的行为。

**端点对不等于请求体对。** 本地真实 binary 记录到：独立 Responses provider 发出
`store:false`、`include:["reasoning.encrypted_content"]`、`prompt_cache_key`，
也发出 `max_output_tokens:4096`，没有顶层 `instructions`。
[request preparation](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/session/llm/request.ts#L80)
只在 OpenAI OAuth 路径把 system 文本放入 `instructions`；普通 API key 路径将其放入输入消息。
这说明“支持 Responses”仍有原生 Codex 请求形态的差异，不能把网关变成悄悄删字段的兼容补丁箱。

另一项 wire-only 检查使用内置 `openai`、临时假 API key、配置 baseURL，
并显式设模型 `options.instructions:""`：请求仍到本地 `/v1/responses`，
没有 `max_output_tokens`，有 `instructions:""`。**这是已通过本地契约的画像，仍不是可宣称真实后端完整支持的配置。**
仍需真实 Codex 上游验证模型 ID、developer/input 语义、工具、连续轮次和加密 reasoning 回传。
不发布一份未经真实后端验收的“复制即可使用”示例，也不要求用户注销已有 OAuth 来试错。

### HTTP、SSE 与 WebSocket

[插件开关](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/plugin/index.ts#L62)
规定 stable release 默认不用 Responses WS，`OPENCODE_EXPERIMENTAL_WEBSOCKETS=true` 才打开；
`local/dev/beta` 构建默认打开。因此只记录版本号还不够，兼容报告需记安装 channel 和实验开关。

WS 适配器把 Responses WS 事件转换成给 AI SDK 消费的 SSE Response，
不是模型已切换到另一套语义。它仅用于 `POST */responses`、`stream:true`、有 session affinity 的请求；
标题生成、没有 affinity、同一 session 已忙或已进入 fallback 时走 HTTP。
[ws-pool](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/plugin/openai/ws-pool.ts#L27)
固定默认 connect **15 秒**、idle **5 分钟**、最大连接年龄 **55 分钟**、`streamRetries=5`。
失败计数先递增，再以 `streamFailures > streamRetries` 判断，因此第6次计入的流失败才进入 fallback；
正常终止或 abort 会重置计数，不能解释成任意连续6次网络请求失败。
池以会话分隔，收到 1009 可直接进入 HTTP fallback；这不是一个额外嵌套的同步 6 次重试循环，
不要机械地把它与 session 的 6 次请求再相乘。

[ws.ts](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/plugin/openai/ws.ts#L72)
有连接中止、流中止和 idle 检查；传入 signal 后终止 socket/reader。
WS first-event 等待也位于外层 fetch 的响应头等待之内；其配置含义并不等同于纯 HTTP 握手。
本轮没有对这条实验 WS 路径进行真实 wire 验收。

还有单独的 `OPENCODE_EXPERIMENTAL_NATIVE_LLM`：
[LLM 调用处](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/session/llm.ts#L226)
先尝试 native runtime，再按能力 fallback 到 AI SDK。
下面默认值和实测仅针对本次 stable 默认 AI SDK 路径，不延伸到未来 v2/native runtime。

### 等待与重试：以实际调用链为准

| v1.19.4 AI SDK 路径 | 实际行为 |
| --- | --- |
| `options.headerTimeout` | 默认 `300000ms`，`false` 关闭；外层 fetch 开始计时，返回 response 时停止，包含自定义 fetch 的准备时间 |
| `options.chunkTimeout` | 默认 `300000ms`，仅对 `text/event-stream`，每次 reader.read 等待设置计时；`false` 关闭 |
| `options.timeout` | 仅用户给值时加入 `AbortSignal.timeout`，持续覆盖该次 fetch/流；未配置无这个总时限 |
| 默认会话 `streamText.maxRetries` | 显式 `input.retries ?? 0`，不是 AI SDK 自身默认值 |
| 会话外层重试 | `SessionRetry.policy` 最多 5 次；匹配可重试 API/network 错误，5xx 也被纳入 |
| 退避 | 初始 2 秒、指数因子 2、加 0–25% jitter；优先 `retry-after-ms`，然后 `retry-after` 秒数或 HTTP date |
| 退避上限 | 没有 response headers 时为 30 秒；有 headers 时并非统一 30 秒上限，最终受 JS timer 上限约束 |
| 标题生成 | 显式 `retries:2`，不能用默认主会话的重试数字代表所有辅助调用 |

来源：[timeoutFetch](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/provider/provider.ts#L94)、
[streamText](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/session/llm.ts#L280)、
[session retry](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/session/retry.ts#L26)、
[processor](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/session/processor.ts#L643)、
[title](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/session/prompt.ts#L226)。

**滚动文档存在不同口径。** 抓取日的[官网 config](https://opencode.ai/docs/config/#models)
写 `timeout` 默认 300000、`chunkTimeout` 默认 900000；上述固定源码与配置 schema 是
`timeout` 未设置不添加总计时、`chunkTimeout` 300000。这里不推断官网一定错误，
可能对应新版本；兼容验收须固定 binary，并在需要稳定行为时显式写配置。

这些 fetch 计时器通过 AbortSignal 通知取消，并非独立的强制 Promise 截断；
OAuth 插件中的 refresh fetch 没有接收外层 signal，因此不能由计时器的存在推导整个凭据刷新链严格有界。

OpenCode 的默认等头 300 秒，与 ciallopa 当前等头 300 秒相近，短于网关累计准备预算 600 秒。
所以不能向使用者承诺“客户端一定等完网关所有准备重试”；应先测取消传播与等待归因，
再决定是否分配时间余量，不能简单把所有 timeout 调大。

### 代理、证书、取消和观测

- **官方声明**：[network](https://opencode.ai/docs/network/) 支持 `HTTP_PROXY`、`HTTPS_PROXY`、
  `NO_PROXY`，以及 `NODE_EXTRA_CA_CERTS`。TUI 依赖本地 HTTP server，
  文档要求把 `localhost,127.0.0.1` 放入 NO_PROXY；IPv6 回环应按实际部署追加验证。
- **固定源码**：Bun 的 WS 不会自动继承 HTTP(S) proxy，OpenCode 显式把 `ws/wss` 转成
  `http/https` 后调用 [ProxyEnv](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/util/proxy-env.ts#L42)，
  支持大小写 env、对应协议代理、`ALL_PROXY` fallback 和 host/port NO_PROXY 匹配。
  Node 非 Bun 的这条构造分支不传该 proxy 参数，不能据此承诺所有运行形态代理完全一致。
- **mTLS**：本轮未找到与 Claude 相当的公开一等 client-cert/key 配置契约。
  SDK/custom fetch 可扩展不等于普通用户配置已支持；列为需要时再验证的能力，
  不建议默认关闭 TLS 校验或引入自定义插件来掩盖差异。
- **取消**：主调用把 `input.abort` 传给 `streamText.abortSignal`；SSE wrapper 的 cancel
  同时 abort 控制器并 cancel upstream reader。已静态核对，未把它当作全过程 wire 证明。
- **身份与追踪**：[request headers](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/session/llm/request.ts#L187)
  包含 `x-opencode-session-id`、`x-session-affinity`、`x-session-id`；父会话另有 headers。
  内置 openai 插件另发 `originator:opencode`、`session-id`。这些是客户端声明的关联标识，
  不是认证身份，也不是每次上游尝试的 request ID；不要直接作为高基数指标标签。

### MCP 和其他出站流量

模型 provider 的 options 不控制 MCP transport。
[remote MCP](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/mcp/index.ts#L236)
有自己的 URL、headers、OAuth，先尝试 Streamable HTTP 再试 SSE，连接默认 30 秒，
每服务器 `timeout` 可覆盖；local MCP 是独立子进程。
[tool call](https://github.com/anomalyco/opencode/blob/148842ef7cb1bc34192b3fff57d0f9721ab9c801/packages/opencode/src/mcp/catalog.ts#L53)
传入 abortSignal，并启用 `resetTimeoutOnProgress:true`，这与模型流的 byte idle 不同。
工具调用 timeout 还可来自 server 配置和 experimental MCP 配置，不能把连接 30 秒解释为统一工具时限。

此外还存在 OAuth 授权/刷新、models.dev 模型目录、插件/npm、LSP 下载、分享、WebFetch 和工具子进程。
provider baseURL 不覆盖这些；进程代理 env 也不能保证任意子工具遵守同一代理或证书机制。
本地试验关闭自动更新/模型抓取/外部 skills/LSP 下载，使用临时 XDG 路径及 `OPENCODE_TEST_HOME`，
未触碰用户客户端配置或真实凭据。

## OpenCode 本地验证记录

执行官方 `opencode-linux-x64.tar.gz` 的 release binary，日志 User-Agent 确认为
`opencode/1.19.4 ... runtime/bun/1.4.4`。
二进制 SHA-256：`538fcb1b6408c8d4608822acee19f7ff5b3a3eb3383c0633c4f06170857312a7`。
发布包 SHA-256：`849bd6b3d959a1d2ef200ed1634a51c79a45ca236aa26665d75debd694805cce`，已与官方 release API digest 核对。
全部模型目标为 Python loopback mock，key 为测试假值；不调用 ciallopa 实例或真实模型。

| 场景 | 观测 | 边界 |
| --- | --- | --- |
| 自定义 `@ai-sdk/openai` | 1 次 `/v1/responses`、Bearer key、stream true、session headers | mock 故意返回400止步，不证明成功生成 |
| 自定义 `@ai-sdk/openai-compatible` | 1 次 `/v1/chat/completions`、messages、stream_options.include_usage | 明确不是 ciallopa 当前路由 |
| 自定义 Responses，内置插件开启 | 仍请求配置的 `/v1/responses`，不被 openai OAuth 插件重写 | 隔离配置中无真实 OAuth，不测试已有用户 profile 的混合优先级 |
| 内置 openai，API key，instructions 空字符串 | 本地 `/v1/responses`，instructions 存在，max_output_tokens 缺失 | 仍是 wire 检查，未验证 Codex 上游接受 |
| 持续503 + Retry-After:0.01 | 6 次 POST；相邻请求约13–22ms；主会话无额外 SDK 重试放大 | 仅此错误路径；未覆盖流中断、工具、标题、WS fallback |

前五个场景中 mock 刻意返回400/503，CLI exit1符合预期；通过的是请求形状与尝试计数，不是成功生成。

### 本地工具与多轮契约：OpenCode → ciallopa → 假上游

同日实施轮继续使用官方1.19.4，独立API-key配置与临时目录。内置 `openai`、`@ai-sdk/openai`、
模型 `options.instructions:""`，保留内置插件；没有已存OAuth。真实网关后共捕获4次Responses请求：
工具调用、工具结果后的续答、同session第二轮、取消中的活动流。

- 客户端实际执行一次 `read`，仅读取临时目录的标记文件；下一请求的 `function_call_output`
  保留 `call_id` 与读取结果，随后正常输出。第二轮输入携带先前文本与工具历史，session保持一致。
- 已接收活动输出后给该CLI进程发SIGINT，CLI因信号退出，上游收到断开，网关占用归零；
  三条成功记录及一条 `status=200,client_closed/client`，未把取消写成成功。
- 捕获到的请求均为 `/backend-api/codex/responses`，有空 `instructions`、`store:false`，
  不带 `max_output_tokens`，保留 `reasoning.encrypted_content` 的include请求。

这将证据从wire形状推进到了完整本地工具/多轮链路，**仍未验证真实Codex后端或真实加密reasoning**。
SIGINT进程取消不等于已测OpenCode所有TUI取消入口；实验WS也未在此候选画像中启用。
现有OAuth profile与上述隔离API-key画像不能直接混用，本轮不新增完整支持声明。

## Claude Code：辅助参考的公开契约

以下均是 **2026-10-23 抓取的官方滚动文档声明，未运行 Claude Code 验收**。
文档已包含 v2.1.307 的条款，不倒推到旧安装版本；没有拿 Anthropic SDK 的默认行为冒充 CLI 行为。

### 端点、凭据与能力保留

[Gateway protocol](https://code.claude.com/docs/en/llm-gateway-protocol) 明确：
`ANTHROPIC_BASE_URL` 使用 Anthropic Messages，推理通常为 `/v1/messages?beta=true`，
可选 `/v1/messages/count_tokens`；还有独立的 Bedrock InvokeModel 和 Google rawPredict 接入格式。
**ciallopa 当前只有 Codex-native Responses 等路由，不能由设置 ANTHROPIC_BASE_URL 变成 Claude Code 网关。**

[Credential setup](https://code.claude.com/docs/en/llm-gateway-connect#set-the-credential-variable)：
`ANTHROPIC_AUTH_TOKEN` 发 `Authorization: Bearer`，`ANTHROPIC_API_KEY` 发 `x-api-key`，
`apiKeyHelper` 可发两者。仅改变 baseURL 而不提供网关凭据，保存的 claude.ai OAuth 仍可能是活动凭据；
网关凭据激活后则替代该订阅凭据。这与 OpenCode OAuth 重写 URL 的机制不同。

保留 `anthropic-version`、`anthropic-beta` 和未识别扩展字段，是协议兼容的一部分。
尤其 OAuth capability 也在 beta header 中；任意删掉未知 beta 值会破坏认证或新功能。
流需要及时透传并保留末尾 `message_delta`/`message_stop`，不能把 HTTP200 或干净 EOF 自动算成功。
把 Bedrock 的二进制 eventstream 改标为 SSE 同样会破坏客户端。

### 代理、CA、mTLS 与设置作用域

[Network configuration](https://code.claude.com/docs/en/network-config)：

| 项 | 官方契约 |
| --- | --- |
| 代理 | HTTP(S) proxy；优先顺序 `https_proxy`、`HTTPS_PROXY`、`http_proxy`、`HTTP_PROXY`；NO_PROXY 支持逗号或空格；不支持 SOCKS |
| 回环 WS | localhost、::1、127.0.0.0/8 的 WS 自动绕代理；不能将此特例套用到全部 HTTP |
| CA | 默认 bundled Mozilla + system；系统库读取依赖 runtime，npm 安装需 Node22.15+；`NODE_EXTRA_CA_CERTS` 追加 CA |
| mTLS | `CLAUDE_CODE_CLIENT_CERT`、`CLAUDE_CODE_CLIENT_KEY`、可选 passphrase；启动/应用设置时读取 |
| 证书轮换 | 新版在符合条件的连接/TLS失败后重新读取，HTTP错误本身不触发；OTLP exporter 仍需重启 |
| 后台进程 | supervisor 继承启动时环境；官方建议 settings.env 保证后台会话拿到网络设置 |
| 宿主差异 | Claude Desktop 宿主管理 provider 时忽略项目级 TLS/proxy 设置；云会话也有受宿主控制的范围，终端示例不能原样套给所有产品形态 |

对 ciallopa 的借鉴是明确设置作用域和快照/轮换时机，不是立即加入企业证书管理面板。

### Timeout 与 retry

[Streaming watchdogs](https://code.claude.com/docs/en/network-config#streaming-idle-watchdogs)
和 [retry tuning](https://code.claude.com/docs/en/errors#tune-retry-behavior) 的关键区别：

| 项 | 当前公开默认/范围 |
| --- | --- |
| `API_TIMEOUT_MS` | 600000ms，每次尝试等响应头；不限制之后的完整流时长 |
| first-byte deadline | 直连 Anthropic 默认180秒，部分平台300秒，并考虑请求体大小；普通自定义 ANTHROPIC_BASE_URL 默认不启用这个机制 |
| event-level watchdog | 默认300秒，所有 provider；识别解析事件进度，keep-alive 字节只在特定条件下有限延长 |
| byte-level watchdog | 直连默认180秒、自定义 gateway默认300秒；保留 SSE ping 对它有意义 |
| body idle | 部分 provider 默认5分钟，计入等头；可用 API_FORCE_IDLE_TIMEOUT 改变，独立 watchdog 不随之全部关闭 |
| 普通重试 | 默认最多10次，CLAUDE_CODE_MAX_RETRIES可改；部分529、流中断、已完成输出等有更早退出规则 |
| Retry-After | 官方网关指引要求整数秒；普通模式 >60秒会直接停止重试显示错误，不能套用 OpenCode 接受 HTTP-date 的行为 |
| unattended watchdog | CLAUDE_CODE_RETRY_WATCHDOG 可让容量429/529长期等待，另有每请求等待上限；不能把普通10次视为所有模式的总上限 |

CLI 对流中断的处理依赖已完成的 thinking/text/tool 阶段，有重试、保留部分输出并提示、终止等区别。
这里保留这个状态边界，不尝试把闭源恢复流程简化为通用“5xx重试10次”。
网关自身重试、客户端重试、模型续写及工具执行也不是同一计数。

### 关联信息和模型流量之外

官方 [request headers](https://code.claude.com/docs/en/llm-gateway-protocol#request-headers)
有 `x-claude-code-session-id`、agent/parent-agent ID；新版可选 gateway hints 进一步区分
main/subagent/workflow/compaction/auxiliary 和 prompt ID。
自定义 baseURL 默认关闭这些 hint headers，需 `CLAUDE_CODE_GATEWAY_HINT_HEADERS=1`；
traceparent 传播也有独立 `CLAUDE_CODE_PROPAGATE_TRACEPARENT` 开关。
这些是值得借鉴的“请求角色”信息，缺字段不应伪造主会话或零工具时间，也不能拿它代替客户端 key 身份。

MCP 有自己的 stdio、HTTP、SSE，当前文档还列出单独 WS 配置；
MCP 的 WS 并不证明模型推理也使用 WS。
[MCP timeout](https://code.claude.com/docs/en/mcp)
和 `MCP_TIMEOUT`/`MCP_TOOL_TIMEOUT`/`CLAUDE_CODE_MCP_TOOL_IDLE_TIMEOUT` 是独立范围。
模型网关不承接工具服务器流量，progress notification 也不能直接计为模型 TTFT。

官方还明确 fast-mode availability 和 WebFetch domain safety 检查可直接访问 `api.anthropic.com`，
不跟随 ANTHROPIC_BASE_URL；OAuth、更新、telemetry、浏览器/远程控制也各有目标。
网关日志没有记录某次活动，不足以证明 Agent 没有外发请求。

## 吸收到 ciallopa 的设计取舍

1. **先固化客户端画像，再谈通用兼容。** 验收记录安装版本/channel、认证模式、provider SDK、
   endpoint、HTTP/WS、关键实验开关；Codex 继续为第一优先级，OpenCode 保留本地成功画像，真实后端独立验收。
2. **按阶段解释时间。** 保持准备预算、等头、流 idle、写入背压、停机分离。
   UI 展示超时来源和当前阶段，避免一个“超时”旋钮让用户把客户端600秒与网关600秒理解为同一总额度。
3. **尝试链应跨边界可读。** 网关展示每次本地选择/尝试的原因、最终账号、上游request ID；
   客户端 session/prompt 作为可选关联字段。不要把一次 Agent prompt 的多次请求混为一次模型请求，
   也不要从客户端 retry header 推定已经去重或不再有副作用。
4. **稳定关联，保留协议扩展。** 在白名单观测字段和原样转发之间划清界限；
   不为省解析工作删未知 body/beta 字段，不把高基数 session 放进普通聚合指标。
5. **取消需要端到端证据。** 已有网关 cancellation 测试之外，挑实际客户端做一次按停止、
   静默流取消、WS断线/HTTP fallback 检查；证明释放的是上游工作与租约，不只是 UI 按钮恢复。
6. **扩展由真实需求驱动。** 暂不加入 Messages 转换层、通用 MCP 网关或企业代理控制台。
   Claude 的 watchdog、设置作用域、请求角色值得参考，不代表其全部企业策略适合个人网关。

## 按需求验证

真实供应商、实验 WS、企业代理/CA 和客户端版本升级分别选择最小完整场景；
先核对凭据模式、实际请求地址、事件契约、取消和尝试次数，再增加支持声明。
当前没有 Claude Messages 适配器，不通过替换 baseURL 冒充支持。
本地执行方法见 [TESTING.md](../TESTING.md)，研究总览见 [RESEARCH.md](../RESEARCH.md)。
这些客户端及真实账号不成为每次 CI 的必备依赖。
