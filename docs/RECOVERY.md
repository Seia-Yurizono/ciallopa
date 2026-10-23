# 故障恢复与诊断

本页说明当前恢复与诊断边界。客户端行为使用官方 Codex 0.164.0、本地 ciallopa、假上游，
以及 Caddy 2.11.7 验证；没有连接真实模型供应商。外部问题的来源与区别见
[CLIProxyAPI 反馈分析](research/CLIPROXY_RECOVERY.md)。运行命令见 [TESTING.md](TESTING.md)。

## 工具结果提交之后断流

`scripts/cli-smoke.py --suite tool-recovery` 覆盖 SSE / WS，各在工具结果续轮的首输出前、后断流。
Codex 通过 app-server 请求动态工具 `test_echo`，驱动实际更新本地计数文件并返回带序号的结果。
假上游收到包含该结果的下一次生成请求后等待屏障；输出后场景还要等到 Codex 实际报告文本 delta，
才由驱动触发断流。恢复预算显式设为 `request_max_retries=0`、`stream_max_retries=1`。

四个场景均观察到：

- 本地工具实际执行 **1 次**，`call_id=call_fixture`；恢复输入保留唯一的原调用及完全相同的结果。
- 业务生成请求 **3 次**：第一次生成工具调用，第二次收到工具结果后被中断，第三次恢复完成。
  网关记录为两次成功、一次故障，各行 `retries=0 / switches=0`。
- WS 续轮原本携带 `previous_response_id`；断线恢复后不再携带旧引用，改发完整历史。
  同一账号、session、thread/turn 关联保持，最终连接占用归零。
- 首输出前断流只收到一组最终文本 delta；输出后断流收到两个不同 message ID 的同文 delta，
  最终完成的 turn 只保留一个最终答案。文本相同是固定假上游的安排。

这证明 **结果已经提交到后续生成输入** 的这个边界没有重复调用工具。它不证明工具执行中断、
副作用已发生但结果未确认、客户端崩溃或模型重新生成另一条调用时的恰好一次执行。
也不以假 encrypted reasoning 内容的保留证明真实供应商加密兼容。
目前没有据此增加网关工具去重或按文本消重的依据。

## 工具结果尚未确认与客户端崩溃

`--suite tool-faults` 在 SSE/WS 各验证四个窗口。工具调用已被 Codex 分派，但驱动暂不返回结果；
每次使用独立目录和固定的本地计数工具。崩溃场景使用持久线程，重启同一个临时 CODEX_HOME，
通过 `thread/resume` 继续，不能用新的临时对话冒充恢复。

- **上游断开、工具待回复**：网关占用归零，但 Codex 的动态工具回调在两秒观察窗内仍待回复，
  没有因此自动执行工具。随后显式 `turn/interrupt` 才完成本次中断；两秒观察不承诺之后永久不变化。
- **显式取消、工具未执行**：实际计数为0，原 call ID 保留，持久历史含取消结果
  `dynamic tool call was cancelled before receiving a response`。后续 turn 正常完成。
- **客户端崩溃、工具未执行**：计数为0，持久历史保留调用而没有结果。恢复后的下一次生成输入
  为原 call ID 补入 `aborted`；不把补入请求的内容当作已写回历史的工具结果。
- **工具已执行、回复前崩溃**：固定 worker 将本地计数写入并 fsync，随后在返回结果前被杀死，
  app-server 也被杀死并恢复。实际计数为1，持久历史同样没有结果，下一次输入同样是 `aborted`。

因此 `aborted` 不能证明工具没有产生副作用。这组实验没有重发成功工具回复，也没有让网关重复执行工具；
真实模型仍可能在后续生成另一条调用。需要幂等的副作用应由工具自己的操作 ID、结果查询或确认机制处理，
网关不能按相同文本或调用历史猜测是否安全重做。现有取消与客户端恢复策略保持不变。

## Caddy 重载与延迟关闭

`scripts/caddy-smoke.py` 将首输出前、后分别与三种重载行为交叉，共六个场景。
重载必须让独立 HTTP 探针读到新的 revision，不能只以 reload 命令退出成功判定配置生效。

- **默认关闭**：旧响应在终止事件前断开，Codex 恢复；两次生成、一次中断记录和一次成功记录。
- **3 秒延迟，窗口内完成**：释放假上游后，原 response 完成，只发生一次生成。客户端保持打开时，
  旧空闲 WS 仍占 `ws_connections=1 / inflight=1`；到期才释放，没有多出业务失败行。
- **1 秒延迟，窗口耗尽**：假上游继续等待，到期仍断开并恢复，最终仍是两次生成。

这里的 1 / 3 秒是用于可复现短实验的参数，不是建议的生产配置。客户端及 mock 的响应非常快，
不能用它们选择真实长生成所需的延迟。探针记录的关闭时刻是轮询观察时间，不是精确的网络时间。
三种行为在首输出前、后都应保持相同的生成次数区别；输出后的默认关闭和窗口耗尽仍可产生两组 delta。

Caddy 2.11.7 [固定源码](https://github.com/caddyserver/caddy/blob/v2.11.7/modules/caddyhttp/reverseproxy/streaming.go#L463)
在旧 handler 卸载时设置一次固定延迟计时器，并非按每次活动续期。
已升级连接继续属于旧 handler；本实验验证新请求看到新 revision，以及旧连接的存活和释放，
没有声称旧连接立即服从新路由、权限撤销或能挺过进程退出。

**部署默认值保持不变。** 对可容忍旧连接继续存活的计划配置变更，可以按业务最长流时长选择有限的
`stream_close_delay`，先按同样的本地对照验证。需要及时撤销旧路由、连接名额紧张、进程替换等场景，
仍应先停止新流量、排空活动请求。延迟只能减少部分重载导致的再次生成，不能提供无损迁移或计费幂等。

## 最小终止诊断

Responses SSE 和活动 WS turn 的异常上游结束新增一条结构化 warning：
`upstream stream ended without an observed terminal event`。
它关联已有的 account、record/thread/turn ID，以及可取得的 upstream request ID，区分
`eof / read_error / close_frame / write_error / idle_timeout / observation_limit`。
新增字段是末事件类型、事件数、`terminal_received`、`activity_received`、
`activity_idle_ms`、`event_idle_ms`、`observation_limited`。

事件数只统计完整、带字符串 `type` 的 JSON 事件，按一次 HTTP 尝试或 WS turn 的当前上游尝试计算。
末事件采用有限类型表，未知类型一律记 `unknown`，不把上游任意字符串或正文写到新日志。
SSE 非空块（包括注释）和 WS 数据/控制帧活动更新活动时钟；只有完整事件更新事件时钟。
这能区分“仍观察到心跳，但没有完整事件”和“没有观察到新活动”，并非 socket 原始字节计量；
本地调度、读取暂停及下游背压也可能影响间隔，不能单凭它定位网络故障。
尚无对应观察时，间隔从本次尝试开始算；缓冲前缀不会重复计数或刷新已记录的时间。

`terminal_received=false` 表示没有观察到终止事件；若 `observation_limited=true`，
终止可能位于超出既有观察上限的大事件中，不能据此认定上游漏发终止。
正常完成保持安静；客户端主动取消、Caddy 关闭下游和 Force 沿用既有请求分类，
不增加上游故障 warning。关闭 usage 持久化仍有诊断，但没有 record/thread/turn 关联字段。
WS upstream request ID 来自握手，不是上游给每一轮重新分配的 response ID。
普通发送和 quota/overload 内部重连后的发送失败均有 `write_error` 诊断。
重连先重置观察状态，使用当前账号及新握手 ID；旧尝试的终止事件不能抑制新失败的警告。
主动客户端取消保持安静，Force 保留 shutdown 归因。这仍不是完整的逐阶段尝试链。

本轮不改数据库、UI、重试/超时默认值、转发内容或故障后的恢复策略。
需要调查时按 record ID 查请求行，再用 thread/turn ID 合并客户端新请求；
不要把客户端再次生成算作某行的网关内部 `retries`。

## 隔离与复现

命令见 [TESTING.md](TESTING.md)。所有服务只监听 `127.0.0.1`，客户端用新的 `CODEX_HOME`、
只读临时工作目录、假环境密钥和自定义本地 Responses provider；网关使用假账号与本地上游，
关闭出口代理和后台额度刷新。子进程不继承用户的代理、认证或应用配置环境变量。
关闭 Codex analytics、feedback、web search 和 plugins；不安装或下载客户端。
这属于明确配置的本地集成测试，不宣称提供操作系统级出网隔离。

只结束本轮创建的子进程。每次运行保留版本、二进制摘要、配置、合成请求、app-server 事件、
计数文件、网关记录和退出状态；失败运行也保留证据。`TMPDIR` 可指定仓库外的证据磁盘，
避免大二进制副本占满 `/tmp`。读取正在追加的 JSONL 时只解析已换行的完整记录，
完整行格式损坏仍报错，不吞掉损坏记录。

产品日志与这些合成请求快照是两回事：生产诊断不应保存用户 prompt 或工具结果。

