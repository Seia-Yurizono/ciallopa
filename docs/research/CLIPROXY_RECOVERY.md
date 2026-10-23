# CLIProxyAPI 反馈复核：断流、再次生成与重复输出

后续实施见 [恢复边界](../RECOVERY.md)：计数工具断线恢复、Caddy 延迟关闭对照，
以及最小终止诊断。下文保留外部调研当时的版本、证据与结论边界。

调研日期：2026-10-23。核对
[CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI)公开issues正文、评论及相关源码。
源码固定为`v8.0.40` / `d946c4fba6fc9a5cfbca1f3d85184d62ad2fb683`；
issue状态是抓取时快照，不把关闭、有人提交PR或PR有测试合并SHA当作已经修复发布。

**确实有类似反馈，但至少包含三种不同问题：断流后客户端再次发起生成、协议翻译重复输出、
以及尚未定位的Agent循环。我们的Caddy实验属于第一类，不能据这些反馈认定共同根因就是Caddy。**

## 1. 最有用的反馈与实际证据

### 429、重连与长上下文放大：#4536

[#4536](https://github.com/router-for-me/CLIProxyAPI/issues/4536)最初报告v7.2.97在单凭据429后，
通过1012触发全量重放、重复重连和高CPU。关键是后续
[维护者隔离复核](https://github.com/router-for-me/CLIProxyAPI/issues/4536#issuecomment-5067397645)：
官方Codex0.144.6 + CPA `8f490669` + 假上游，在工具结果续轮持续429、约950K字符输入下，
相对成功对照，上游WS连接1→3、报告中的generation次数2→4、请求字节1,104,766→3,156,014。

这支持“恢复会增加再次生成请求及长上下文处理量”。但该复核约3.77秒后报重试耗尽，
没有无限循环；首次实际观察是1006/connection reset，并非1012。
CPU为12秒采样窗中的50→170ms，不能仅用相对倍数证明生产高CPU。
维护者不建议据此禁止单凭据恢复：同账号刷新后或冷却结束后也可能恢复。
最后一条用户评论另指向其他客户端的修复，不能算作CLIProxyAPI/Codex的通用修复证明。

与ciallopa相关的是**客户端预算、网关尝试与全量请求字节需要分开计数**；
当前0.164的实测不能用旧0.144.6结果替代。也不能将该历史场景的有界恢复推广为所有错误的统一上限。

### 重复状态文本/Agent循环：#2590、#2903

[#2590](https://github.com/router-for-me/CLIProxyAPI/issues/2590)报告Codex VS Code扩展反复说
“准备检查/正在检查”；[#2903](https://github.com/router-for-me/CLIProxyAPI/issues/2903)报告
Codex0.121.0反复搜索、状态消息和长turn，用户称启用WS后改善。
两者都被以`not_planned`关闭，维护者没有从已提供材料确认网关缺陷。

这些是确实存在的用户观察，**没有足够请求/事件链证明是传输重试、同一事件重复转发或模型自身重复**。
“WS改善”也不是本项目统一关闭SSE或强制WS的依据；我们的故障恰是活动WS被代理重载关闭。

### 真正的转换器重复：#4901，以及不同的ID问题

[#4901](https://github.com/router-for-me/CLIProxyAPI/issues/4901)有具体事件序列：
Responses的`output_item.added`没有item ID，后来的`output_item.done`补了ID，
查找未命中新ID时没有回退到已有`output_index`状态，于是再次输出工具名，客户端拼成
`TaskCreateTaskCreate`。它走`/v1/chat/completions`翻译链，流可正常HTTP200结束却让工具失败。

这是**同一次响应内部的身份关联/翻译问题**，和断线后的第二次生成请求不同。
当前固定源码已能在item ID未命中后继续按output index查找；见后续源码核对。
[#3603](https://github.com/router-for-me/CLIProxyAPI/issues/3603)的输入历史合并重复ID、
[#6588](https://github.com/router-for-me/CLIProxyAPI/issues/6588)的跨响应fallback item ID碰撞，
也不能混称为“重复文本”。ciallopa当前原生Responses路径没有上述Chat翻译或网关历史拼接层。

### 流未完整结束与诊断：#3055、#4527、#6469

[#3055的后续报告](https://github.com/router-for-me/CLIProxyAPI/issues/3055#issuecomment-5674438562)
描述OpenCode → Chat Completions → Codex：已经输出大量文本，约25.3秒后缺少终止事件，
HTTP仍为200，末尾错误却是`invalid_request_error`；用户观察到客户端停止。
这是对错误分类的有效线索，但报告没有捕获客户端内部重试判断，断流根因也未定。

[#4527](https://github.com/router-for-me/CLIProxyAPI/issues/4527)被`not_planned`关闭，
维护者解释408是统一的不完整流错误。**`request_timeout`/408本身不证明某个计时器超时。**
其中另有首个`event:`前缀提前提交、阻止bootstrap恢复的贡献者复现与PR线索，
不能拿“首事件前的恢复修复”解释已经输出后的中断。

[#6469](https://github.com/router-for-me/CLIProxyAPI/issues/6469)仍open：分别报告22秒和约900秒的
不完整流，明确未证明同一原因，也没有排除出口代理和大上下文等因素。
它提出只记录事件类型/数量、终止状态、读错误类别及时间的诊断，避免保存全部对话正文。
关联[#6497](https://github.com/router-for-me/CLIProxyAPI/pull/6497)仍open、未合并；
当前固定版本不能被描述为已经具有这套诊断。

### 当前源码中的边界与实际修复

- [连接连续性][cpa-continuity]要求增量续轮继续使用原上游socket；失去原连接时要求客户端完整重放。
  [handler][cpa-replay]仍会把绑定续轮的401/429转为请求客户端恢复；没有照搬#4536的“单凭据禁用”建议。
- [输出前缓冲][cpa-bootstrap]确有已合并实现：[#5115对应提交][cpa-bootstrap-commit]，默认关闭，
  当前保留量有48帧/行及1MiB界限。只保留允许的元数据/空公告；内容、服务端工具活动或未知事件会提交输出。
  它不能解决已输出后的重放，也不能承诺首字节之前没有上游计算成本。
  #4580与整次生成缓冲提案#4643仍未合并；整次缓冲还会把首输出拖到完成时，不能撤销服务端工具副作用。
- #4901有实际[修复提交][cpa-tool-fix]；[当前状态查找][cpa-tool-state]和[done处理][cpa-tool-done]
  已避免上述工具身份重复发送。这里只确认修复存在于v8.0.40，未追溯其首次发布版本。
- 输入历史去重的[#3620](https://github.com/router-for-me/CLIProxyAPI/pull/3620)、
  [#3657](https://github.com/router-for-me/CLIProxyAPI/pull/3657)已合并，
  [当前代码][cpa-input-dedupe]按item/call身份维护历史与工具配对；它不对新response的文本去重，也不是请求幂等。
- [当前SSE结束处理][cpa-eof]把原始scanner错误写到受request-log开关控制的日志，再生成统一不完整流错误；
  #6497提出的末事件、事件数和脱敏读错误分类尚未落入这里。不能拿提案当已发布能力。

## 对 ciallopa 的影响

Caddy 重载可使 Codex 发起新生成；固定文本 mock 的不同 message ID 下可能出现同文 delta，
最终 turn 只保留后一条。它不证明真实模型逐字重复、重复计费、TUI 永久显示两份或工具重复执行。
本地计数工具和延迟关闭对照的当前边界见 [RECOVERY.md](../RECOVERY.md)。

已经吸收的做法是有限终止元数据、thread/turn 关联、独立的工具结果边界测试，以及明确的重载代价。
不按文本前缀盲目去重，不伪造 completed，不透明拼接另一个账号的回答。
模型输出、工具副作用、客户端呈现和计费没有共享一个可由网关保证的恰好一次边界。

本研究只读取公开资料和固定源码，没有运行 CLIProxyAPI 对照或请求真实供应商；
未取得把上述用户报告直接定位到 Caddy reload 的完整证据。issue/PR 状态按抓取日解释，
不把关闭 issue、出现 PR 或未打开的截图当作独立修复与运行证明。

[codex-ws]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/codex-api/src/endpoint/responses_websocket.rs#L945
[codex-retry]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/core/src/responses_retry.rs#L141
[codex-client]: https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/core/src/client.rs#L1719
[cpa-continuity]: https://github.com/router-for-me/CLIProxyAPI/blob/d946c4fba6fc9a5cfbca1f3d85184d62ad2fb683/internal/runtime/executor/codex_websockets_stream.go#L98
[cpa-replay]: https://github.com/router-for-me/CLIProxyAPI/blob/d946c4fba6fc9a5cfbca1f3d85184d62ad2fb683/sdk/api/handlers/openai/openai_responses_websocket.go#L849
[cpa-bootstrap]: https://github.com/router-for-me/CLIProxyAPI/blob/d946c4fba6fc9a5cfbca1f3d85184d62ad2fb683/internal/runtime/executor/codex_executor_terminal.go#L475
[cpa-bootstrap-commit]: https://github.com/router-for-me/CLIProxyAPI/commit/4b9d404fb04f3445a1435e21027257687b89f274
[cpa-tool-fix]: https://github.com/router-for-me/CLIProxyAPI/commit/e9c44ae256c5ebbc3960bcda6f64ea603b8c6b35
[cpa-tool-state]: https://github.com/router-for-me/CLIProxyAPI/blob/d946c4fba6fc9a5cfbca1f3d85184d62ad2fb683/internal/translator/codex/openai/chat-completions/codex_openai_response.go#L780
[cpa-tool-done]: https://github.com/router-for-me/CLIProxyAPI/blob/d946c4fba6fc9a5cfbca1f3d85184d62ad2fb683/internal/translator/codex/openai/chat-completions/codex_openai_response.go#L402
[cpa-input-dedupe]: https://github.com/router-for-me/CLIProxyAPI/blob/d946c4fba6fc9a5cfbca1f3d85184d62ad2fb683/sdk/api/handlers/openai/openai_responses_websocket_requests.go#L626
[cpa-eof]: https://github.com/router-for-me/CLIProxyAPI/blob/d946c4fba6fc9a5cfbca1f3d85184d62ad2fb683/internal/runtime/executor/codex_executor_stream.go#L498
