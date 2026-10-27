# 请求指标与查询口径

这些指标回答“用了多少、哪里失败、等待多久、接下来查哪条请求”。数据来自本地 usage 记录，
不需要 Grafana、Prometheus 或外部服务。它们是个人使用和排障的观测，不是计费账本。

## 同一范围，再逐步查看请求

概况、分组、趋势和请求列表共用 `from` / `to`（Unix 毫秒，均包含端点）及
`account` / `model` / `key` / `instance`。`key` 是密钥标签，不是原始密钥。
一次刷新先确定同一对绝对时间，再发送各个查询；不要让三个 `range=24h` 各自使用不同的当前时间。

内置面板默认不含预热：概况 Token、费用、趋势、分组、账号页的 24 小时用量和请求列表都带 `exclude_prewarm=1`（账号列表为默认行为，`include_prewarm=1` 计入）；
在筛选范围勾选「含预热」（保存在本浏览器 `localStorage`）后计入。API 默认仍包含预热，脚本行为不变。
请求列表“全部结果”跟随该开关；“业务请求（不含预热）”始终对应 `exclude_prewarm=1`，“预热”结果始终可选。
点击概况请求总数时采用该条件，使列表 total 与同范围的 `count` 采用相同分母。
请求列表可继续按 `outcome`、`source`、`error`、`retried`、`tier`、`downgraded` 缩小范围。
这些是请求调查条件，不改变上方概况的分母。点击失败数只应筛出相同范围的失败请求；
点击时间桶只收窄请求列表时间，并保留账号、模型等共同条件。

统计按**请求开始时间**归入窗口；只有已经结束并成功写入的请求/WS turn 才出现。
在途请求、未记录的透传端点（如 `/models`）和丢弃的记录不在其中。
因此这里的请求数不能充当即时进入网关的 QPS，也不能证明当前没有在途请求。

## 八类指标

令 `N = count`，表示范围内已记录的非预热请求数。`prewarm_count` 单列。

| 指标 | JSON 字段与公式 | 空样本及边界 |
| --- | --- | --- |
| 请求量 | `count`，趋势桶的 `count` | 有效空查询为 0；读取失败和关闭记录不是 0 |
| 请求结果 | `failures`、`cancelled`、`incomplete`、`unknown`；`failure_rate = failures/N`、`cancel_rate = cancelled/N` | N=0 时新比率为 `null`；HTTP 200 的流内错误仍是失败；观察不完整单列未知，不推断成功或失败 |
| 首输出等待 | `stream_ttft_p50_ms` / `stream_ttft_p90_ms`、`stream_ttft_samples` | 仅非预热 SSE/WS 且有 `ttft_ms` 的记录；缺样本为 `null`，不补 0 |
| 总耗时 | `duration_p50_ms` / `duration_p90_ms`、`duration_samples` | 所有非预热结束记录，包括失败与取消；不是只统计成功请求 |
| 用量与缓存 | `input_tokens`、`output_tokens`、`usage_samples`；`cache_ratio`、`cache_samples`、`cache_input_tokens` | 缓存只对 input 和 cached 都已知且 `0 ≤ cached ≤ input` 的同一组样本计算 `Σcached/Σinput`；分母为0则 `null` |
| 输出速度估算 | `generation_tps = 1000 × Σoutput_tokens / Σ(duration_ms − ttft_ms)`、`tps_samples` | 仅正常完整结束、非预热 SSE/WS，且 output>0、TTFT已知、duration>TTFT；无样本为 `null` |
| 参考费用 | `cost_usd`、`unknown_price`、`unknown_price_models` | 本地价格表按 token 计算的名义金额；无可定价样本为 `null`，不是免费 |
| 记录状态 | 响应中的 `recorder.accepted/dropped/written/write_errors/purged` | 当前进程自启动累计值，独立于窗口与筛选；不据此显示该窗口“100%完整” |

首输出与总耗时从各路径的 `Tracker` 起点计时，并非统一的入口端到端时间。
Responses 不含入口请求体读取及前置 JSON 检查；Images 不含 Body 提取，但包含之后的解析；
WS 业务记录从 turn 开始，失败握手记录则从握手开始，包含凭据、拨号与重试等待。
首输出包含文本、推理摘要/文本或工具参数等模型输出事件，不是响应头，也不一定是屏幕上的首字。
首输出样本包括后来失败或取消的流：已经实际观测到的等待不应只因后续失败而消失。
分位数沿用 nearest-rank；只提供 p50/p90 和样本数，不给小样本制造“精确 p99”的印象。

`generation_tps` 是网关观测估算，并非模型纯解码速度。output 中包含 reasoning tokens，
其中有些可能在可见首输出前生成；总时长也包含尾部传输和收尾。现有记录不能复原逐 token 时间或
最后一个 token 的时间，不能把此值宣称为 TPOT、token 间隔或供应商推理性能基准。

`usage_samples` 统计 input 或 output 至少一项已知的记录，包含预热。
token 求和只累加已知值；缺少 usage 的失败/取消不代表没有消耗。缓存读 token 已包含在输入中，
reasoning token 已包含在输出中，不能重复相加。这里的缓存比率是**供应商输入 token 的缓存读占比**，
不是网关响应缓存命中请求比例；当前并没有响应缓存指标。

首屏通常只需请求、失败/取消、首输出、用量和参考费用；总耗时、缓存与速度放在详情或展开区。
账号可用性和出口是简洁的当前状态，不需要再扩成一排监控图表。

### 结果分类

分类顺序如下，预热独立于其余五类：

1. `endpoint=prewarm`：只计 `prewarm_count`、用量、缓存和费用，不计 N、失败率或任何延迟/速度样本。
2. `error=client_closed` 或 `err_source=client`：`cancelled`。表示终止事件前客户端断开；并非上游失败。
3. `error=observation_incomplete` 且 `err_source=local`、HTTP status 未达 400：`unknown`。
   响应已透传，但超出有界事件观察范围，无法确认最终业务结果；上游返回同名错误码仍按失败处理。
4. 其余 `error` 非空或 HTTP status≥400：`failures`。包括 HTTP 200 流内错误、截断、超时和本地错误。
5. 其余 `incomplete` 非空：`incomplete`。例如达到输出 token 上限，是合法终止，但不能标成完整回答。
6. 其余：正常完整结束，可由 `N − failures − cancelled − incomplete − unknown` 得到。

`outcome=completed|failed|cancelled|incomplete|unknown|prewarm` 与上述分类一致。
失败包含请求内容错误和本地错误；它不是单独的“上游服务故障率”。进一步查看来源即可区分。
重试后成功按最终结果计一次请求，尝试链和 `retries/switches` 保留调查依据。
存在 `unknown` 时，失败率仍以全部 N 为分母，不能把“没有已知失败”解读成全部成功。
未知结果保留实际观测到的耗时与首输出样本，但不参与生成速度估算；未知 Token 仍为空。
后续若在同一流中观察到正常的小终止事件，则使用其实际结果。网络/idle 故障与真实客户端取消优先于观察不足。

### 请求列表的问题分组

请求列表按用户能感知的问题分组，规则只在后端 `crates/usage/src/problem.rs` 实现一次，
列表筛选、计数和每行字段共用，测试校验 Rust 与 SQL 两套判定一致。

| 分组 | `problem` | 含义 | `cause` |
|---|---|---|---|
| 报错 | `error` | 客户端收到错误，即 `outcome=failed`；计入失败率 | `quota` 额度、`rate_limit` 限流、`upstream` 上游、`network` 网络代理、`gateway` 网关（`local`/`other`）、`stream` 流中断 |
| 上游出错 | `upstream` | 上游侧（`err_source` 为 upstream/stream/network）至少失败过一次，含被重试或换号救回的请求 | `unrescued` 未救回（最终报错）、`rescued` 已救回（`retries>0` 或 `switches>0` 后正常完成） |
| 没答完 | `unfinished` | 客户端断开、上游 `response.incomplete`、未确认结束；**不计入失败率** | `client_closed` 客户端断开（原“取消”）、`truncated` 上游截断（原 incomplete）、`unconfirmed` 未确认结束（原“结果未知”） |
| 本地拒绝 | `refused` | 本地上限在生成请求记录前直接返回 503，数据来自拒绝表 | `inflight`、`ws`、`body`、`ws_msg` |

“上游出错”与“报错”可以重叠（未救回的上游错误两边都算），不是第二个失败率。
概况的“上游失败率”口径不变，没有新增比率；已救回的请求最终正常完成，不计入失败率。
没有“慢”分组：慢的阈值因模型和推理强度差异太大，暂不界定。

每行另有 `rescued`（被重试或换号救回）、`upstream_fault`（属于上游出错）和 `next`
（建议的下一步，最有用的在前）：`refresh_quota`、`account`、`cooldown`、`attempts`、`egress`、
`accounts`、`new_session`、`detail`。界面只用它们跳转或打开已有视图，不触发写操作；
刷新额度走账号页的普通刷新按钮，从不使用重置卡。

`retries` 只统计传输层重试。同一账号的过载等待后重试（WS `wait+retry`）会保存尝试链，
但不改变 `retries/switches`，因此不标为“已救回”。

### 参考费用

沿用现有价格计算：

`[(input − cached − cache_write) × P_input + cached × P_cache + cache_write × P_write + output × P_output] / 1,000,000`

价格还取决于模型、实际档位、日期、长上下文阈值及本地表的倍率规则，见 [USAGE.md](USAGE.md)。
订阅账号实际不按这个金额收费。当前查价采用记录的请求模型；响应模型不同时，元数据仍可查看，
不能把它当成已核验的供应商账单。价格表更新也会改变历史名义金额。
`unknown_price>0` 表示有用量但未定价，金额只是已定价部分；缺失 usage、丢记录和其他客户端用量
也不在金额内。不要将 `unknown_price=0` 解读为费用完整。

## API 增量与兼容

无需数据库迁移。现有字段保留：`errors/error_rate` 仍把取消及带诊断码的观察不足算入旧“错误”口径，空窗口的旧比率仍为0；
`ttft_p50_ms/ttft_p90_ms` 仍是所有非预热记录的旧混合时间口径；`tps` 仍保留旧算法，
包括缺TTFT时按0代入的行为。新界面采用上表明确的新字段，旧客户端不会突然改变数字。

历史 Responses/compact 非流式请求曾被标记为 `sse`，且 `ttft_ms` 实际是响应头时间。
新写入的实际非流式回复标记为 `http`，新流式分位与速度排除它们；旧记录无法可靠区分，
没有依据回填或伪造“修正后”的历史。分析旧窗口时仍须考虑这项限制。

`GET /usage/v1/records/{id}` 返回 `{ "record": RecordRow }`，适用于任何已保留记录，
包括没有错误明细的正常请求。旧的 `/{id}/detail` 仍返回可选的脱敏错误/尝试详情。
ID 不合法返回400；不存在、被清理或未落盘返回404。这个 ID 是本地**记录 ID**，
不是独立于 usage 开关的入口 trace ID，也不保证多实例写同一数据库的唯一性。

HTTP/SSE 的 idle、缺终止事件 EOF 与 bootstrap idle 警告会附上现有 `record_id`，
以及该次响应已提供的 `upstream_request_id`；WS 活动 turn 的传输错误/idle 警告只附当前记录 ID。
可把16位 ID直接放入页面“按 ID 查找”。usage关闭时没有该ID；记录尚未落盘、被丢弃或清理时仍可能404。
这些字段不增加日志条数，也不代表每一阶段已有完整追踪。

`GET /usage/v1/records` 新增：

- `before_id=<16位十六进制>`：排他游标，查询 `id < before_id`；与 offset 同时传时忽略 offset。
- `next_before_id`：还有下一页时为本页最后一条记录 ID，否则为 `null`。
- `outcome`：上述五种结果。旧 `errors=1` 仍含取消和错误预热记录，不等同于 `outcome=failed`。
- `problem=all|error|upstream|unfinished` 与 `cause=<上表原因>`：问题分组筛选；`problem=refused`
  返回400（拒绝记录在 `/usage/v1/refusals?reason=`）；未知值返回400；`cause` 需与 `problem` 同传。
- `thread=<会话 ID>`：只看一个 Codex 会话。可传完整 ID，或 ID 中任意位置不少于 8 个字符的片段
  （忽略大小写和开头的 `…`）；片段对应多个会话时返回400。响应带 `thread`（解析出的完整 ID）。
- `GET /usage/v1/records/counts`：与列表相同的筛选（忽略 `problem`、`cause` 和分页），返回
  `{all, error, upstream, unfinished, refused, causes:{error:{…}, upstream:{unrescued, rescued},
  unfinished:{client_closed, truncated, unconfirmed}, refused:{inflight, ws, body, ws_msg}}}`。
  `refused` 按相同时间、密钥和实例统计；设置了账号、模型、会话或其他仅请求有的条件时为 `null`，
  因为拒绝发生在这些信息确定之前。
- `exclude_prewarm=1`：排除 `endpoint=prewarm`，匹配概况 `count` 的业务请求范围；默认不排除。
  与其余请求条件取交集，因此与 `outcome=prewarm` 同传时返回空列表。分页与 `total` 沿用此条件。

旧 `offset` / `next_offset` 保留；使用 before_id 的页不返回可继续使用的 next_offset。
`total` 始终是当前整个时间/过滤范围的数量，不是游标之后的剩余数量。
客户端应依 `next_before_id` 决定是否继续，不应用“已展示行数 < total”判断游标是否结束。

列表是**弱一致**的：固定 from/to 固定的是请求开始时间，不是数据库插入快照。
翻页过程中，旧请求可能刚结束并落盘。游标可避免新插入的高 ID 把后页挤出重复行；
高于已越过游标的迟到记录在刷新后可见，低于游标的迟到记录可能出现在后页。
相同毫秒内的多个 ID 不丢失。清理、迟到写入和不同读查询之间都可能使 total 改变；
不为个人排障持有长 SQLite 事务来假装不可变快照。

### 时间桶

`bucket=hour|day` 及 `tz` 沿用现有接口。每桶有 `start_ms`（含）、`end_ms`（不含），
以及 count/errors 和新增 failures/cancelled/incomplete/prewarm_count。
没有记录的桶仍省略，界面可在查询范围内补零计数，但延迟、比率与未知费用不补零。

滚动24小时通常横跨25个整点桶；7天同样可能有首尾两个不完整日。
应画查询范围覆盖的所有桶，不应另从浏览器当前时间只生成“当前+前23小时”而丢掉最早的部分。
钻取时使用 `from=max(query.from_ms,bucket.start_ms)`、
`to=min(query.to_ms,bucket.end_ms−1)`。切换按小时/按天只改分桶粒度，不改全局时间范围。
`tz` 是固定UTC偏移，不能当成带夏令时规则的时区标识。

时间桶包含 `unknown` 结果数和 `usage_samples`（input/output 至少一项已知，包含预热）。
真正无记录的空桶可显示 0；有记录但没有 usage 样本时 Token 显示“未报告”，
明确报告 0 时才显示 0；混合样本仅累计已报告部分，并显示样本数量。

## 没有采集的数据

当前记录可以精确重算记录范围内的结果、已有时间样本、用量和估价；不能复原活动请求数、
入口到首输出的完整耗时、排队/连接/推理各阶段耗时、逐 token 间隔、实际账单或真实响应缓存命中。
这些需要分别采集，不能用 N、ttft 或缺失字段推导。记录丢失计数是进程累计，历史窗口的完整率也未知。
未来有真实需要时再加计量点；今晚不因此引入完整 trace context、OTel 导出或监控服务。

## 参考来源

2026-10-23 通过独立浏览器标签阅读的官方资料：

- [Grafana dashboard best practices](https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/best-practices/)：RED关注请求体验，USE关注资源原因；图表回答明确问题、减少认知负担、从概况向具体证据钻取。
- [Grafana Logs Drilldown](https://grafana.com/docs/grafana/latest/visualizations/simplified-exploration/logs/)：通过时间、标签和字段探索记录，无需先学查询语言。
- [Cloudflare AI Gateway Analytics](https://developers.cloudflare.com/ai-gateway/observability/analytics/)：请求、token、错误、费用和响应缓存随时间分析。其响应缓存与本项目的输入token缓存不是同一概念。
- [OpenTelemetry GenAI client inference](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/client-inference.md) 与 [model server metrics](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-metrics.md)：区分端到端时长、流式首chunk、逐chunk时间与服务端token指标；约定仍为Development。这里借鉴语义区分，不宣称现有首输出/TPS等价于其所有指标。
