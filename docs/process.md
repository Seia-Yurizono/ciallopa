# 进度与接续

2026-10-23：按用户选择，将当前文件快照整理为单个初始提交，重建`main`与`v0.1.0`并重新构建预发布。
以下时间线保留重写前的提交号与验收记录，用于解释设计和已做取舍；这些提交号不再属于新的分支历史。
旧历史已保存为仓库外的本地Git bundle。本次构建结果以[v0.1.0预发布](https://github.com/Seia-Yurizono/ciallopa/releases/tag/v0.1.0)
及对应[release工作流](https://github.com/Seia-Yurizono/ciallopa/actions/workflows/release.yml)为准；
构建后不追加单独的日志提交，以保持一个根提交。

同日后续镜像维护：按用户要求将运行时升级为Distroless `static-debian13:nonroot`，
固定官方索引摘要`sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88`，
合入同一个初始提交并重建v0.1.0预发布。后续按用户要求调整镜像发布规则：通过校验的标签构建
（含预发布）同步更新`latest`；`0.1`等次版本别名仍只由正式发布更新。
此次构建将`latest`与`0.1.0`指向同一镜像；旧Compose若仍使用`0.1`，需要主动更换标签并拉取、重建容器。
GitHub发布继续标记为Pre-release，不设为GitHub的Latest release。
历史Docker验收记录中的Debian 12镜像摘要不代表此次Debian 13构建，当前产物以预发布与工作流为准。

## 2026-10-23 夜间迭代（已暂停）

- 时间：新加坡 00:58 开始，04:55 收尾，按约定在05:00左右暂停。对应 UTC 2026-10-22 16:58–20:55。
- 起点：`0621c1f`，上一轮 5 个本地提交，尚未推送；工作区开始时干净。
- 用户取向：轻量 AI 网关、个人开发者的从容审美；必要的测试用于把持方向，不建繁重流程。
  允许有依据的大改，但先由新一组 subagents 独立讨论总体方向，再分批实现。
- 本轮暂停，等待下一轮继续。最后产品改动为`ccd5e73`，整合验证树为`c9f3b05`；04:07后仅收口文档。
  所有子任务已结束，自建临时服务已退出；改动保存在本地提交中，未推送或发布。

### 接续速览

- **骨架保留，生命周期补齐。** 仍是单二进制、模块化单体与本地SQLite；响应头/准备预算、有限错误体、
  Body/整条WS连接准入、有界停机和取消记录已落地，连接池热更新与出口归因竞态已修复。
- **数据可信，页面轻量。** 区分失败/取消/incomplete/预热，首输出与加权速度带样本口径，未知用量不补零；
  概况→趋势→请求→详情使用同一调查范围，ID游标/深链与键盘返回连贯。设置保留草稿，账号部分读取失败保留快照。
  整体层次参考Linear、Stripe Workbench和Vercel；Grafana只提供部分指标与钻取思路。
- **部署与定位。** README已改为AI网关方向，诚实说明目前只有Codex适配；容器非root/目录挂载/有限停机、
  systemd reload、发布dry run及备份边界已收口，未推送或发布。许可已按用户选择统一为MIT。
- **证据入口。** 最终产品整合检查为315项workspace + 5项loadbench、clippy、fmt和3份Node检查；
  真实CLI四场景与Caddy链路见[ACCEPTANCE](ACCEPTANCE.md)，资源成本见[PERFORMANCE](PERFORMANCE.md)，
  UI浏览器结果见[UI_REVIEW](UI_REVIEW.md)。
  本机结果不是生产SLO；夜间收尾时缺Docker/真实systemd运行证据，锁文件唯一已知漂移未解决。
  后续基础部署CI已通过，见本文末尾的v0.1.0预发布记录。

下一轮优先看本文末尾“下一轮从这里继续”，不必从时间线重新推导已做取舍。

### 方向讨论

新一组 `direction_council`、`metrics_council`、`test_council` 独立审阅代码后互相质询，约 01:10 达成以下方向：

1. **等待与缓冲有界。** 补响应头等待、错误体与 models 响应体限制；兼容默认宽松，避免截断合法非流推理。
   累计准备预算若能自然落地则做，正常生成不消耗后台倒计时；不为预算制造新的生命周期框架。
2. **数字可信、上下文连续。** 用现有 SQLite 与记录 ID 打通查询、全行详情、深链；不做数据库迁移。
   全局时间/账号/模型/密钥/实例影响概况、趋势和记录；结果/错误来源等是明确标出的记录钻取条件。
   失败、取消、incomplete 分开，首输出与生成速率附有效样本口径，未知不是零。
   记录采用 ID keyset 翻页，保留旧 offset API；仍说明迟到写入的弱一致性。
3. **有余力再补并发与停机。** 只有进程总量限制与有限 drain 值得考虑，需先验证当前生命周期；
   本轮不做每 key 公平调度、复杂排队或 WS 配额体系。

明确不做：接入整套 Grafana/OTel、入口 RequestContext/traceparent 框架、多 provider 抽象、
没有容量证据的复杂限流、重浏览器测试依赖、硬件相关的性能阈值。
借鉴 Grafana 的 RED/Drilldown 思路与统一查询上下文，保留现有轻量单页和克制颜色。

测试分层：开发时只跑受影响 crate/场景与 Node UI 状态检查；一批结束验证相关范围，
最终整合运行一次 fmt、all-target clippy、workspace tests、loadbench tests。
普通 PR/push 增加轻量共享 CI；Docker、指纹、人工 CLI E2E 与性能实验保持按需。

### 01:24 已交付与正在进行

- `9be339e`：指标/查询后端。失败、取消、incomplete 分开；流式首输出和生成速度另给有样本数的新口径，
  保留旧字段；完整记录元数据、ID keyset 翻页、结果筛选、时间桶结束边界；无库迁移。
  usage 27 项与 API 2 项测试通过，相关 clippy 通过，独立复审通过。
- `4e9f5c0` 网络边界：响应头 300s、累计准备 600s（可热更）；正常生成不消耗准备预算；错误体 1MiB/10s，
  models 成功体 8MiB/10s，模型缓存最多 64 项且总计 16MiB。
  core/forward 单测、forward 75 项集成、额外边界 7 项与相关 clippy 通过；独立复审无阻塞。
- 新增普通 PR/main/master 轻量 CI，release 复用同一 checks job；actionlint 和 shell 语法检查通过。
- 用户进一步明确：Grafana 只参考部分指标思想。新体验组阅读 Linear、Stripe Workbench、Vercel 官方案例，
  结论写入 [DESIGN_REFERENCES.md](DESIGN_REFERENCES.md)。请求列表应优先于额度矩阵，详情支持连续巡阅与深链。
- UI 正在真实 demo 上迭代：统一绝对范围、全局/请求筛选分层、单指标图表、部分时间桶、全行详情与 ID 深链。
  新浏览器实测暴露的窄屏范围控件、表格长文本、详情操作尺寸和深链导航时序正在修正。
  Node 状态脚本已覆盖共享窗口、25 桶、ID 翻页与迟到页隔离，不引入浏览器测试依赖。

### 01:55 阶段检查点

- `ed621cc`：设置草稿保护。刷新、密钥操作、412 冲突保留原始输入；确认前显示最新服务器 diff。
  PUT 成功但后续 GET 失败明确提示已保存，同时保留草稿；过期基线不会吞掉后续“改回旧值”的输入。
  Node 回归与浏览器验证通过，退出会清除一次性密钥显示。
- `e5d1608`：全局转发准入默认 64，0 仅禁用拒绝；HTTP 持位到 Body 结束，WS 持位到整连接关闭。
  满额立即 503 `gateway_busy` + Retry-After 1，不排队，不读取新请求体，管理/健康/用量路由不占槽。
  HTTP/SSE/WS 统一有限关停，正常 60s + 关闭 3s + flush 5s + runtime 2s，第二个信号提前 Force。
  `max_body_mb=16` 现在真正执行 16MiB，移除旧隐藏 20MiB 下限。配置 API 返回当前 runtime 快照。
  相关库测试、80 项转发、11 项设置、3 项 HTTP、独立进程停机及 clippy 通过。
  独立复审发现阻塞 WS send/connect 可能晚于 flush 释放，已补外层关闭余量及真实 1001/阻塞 future 验收。
- `9331805`：概览统一时间/筛选和一次 summary 查询，可信指标、ID 游标、成功/失败详情、上下条与深链。
  请求前移，更多筛选与 ID 查找按需展开；窄屏保留范围和自动刷新，实时占用用一行文字表达。
  指标复审补上旧范围图柱/卡片不能钻取、全局卡片清局部时间桶、无 usage 样本显示未知。
  Node 状态回归和 Rust 共享块/CSP 测试通过。第三轮真实 demo 375px 请求区约 y=717、首条约 y=811。
- 性能报告已形成：100 万记录/30 天 summary 中位约 344ms，查询进程峰值约 43MiB；51 条首页约 0.24ms。
  临时索引增库约 44%，未普遍改善汇总，未引入产品。数据为 WSL2/tmpfs 暖缓存，不是生产容量承诺。
  正补最小可重复网络驱动与 direct mock 对照，避免把之前缺对照的并发数字当作网关开销。
- 后续确凿问题：账号页 partial 读取失败会清空旧额度且谎报更新成功；独立体验组正在修复。
  Images 记录在响应头时提前结束，direct Images SSE 还误套 Responses 终止规则；核心组正在小批修复。
  普通非流 HTTP 的 transport/terminal 状态也有相同耦合，已要求一起验证 Body 中途取消。
- 本轮尚未执行最终完整 workspace gate；目前记录的是各批相关验证。未推送远端，临时 demo 仍在运行。

### 02:15 阶段检查点

- `162cbb5`：账号页分开保留额度/重置卡快照，503、403/404、首次失败与恢复均有真实说明。
  三页请求函数隔离旧登录的迟到401/403；账号页序号与退出清理同步补齐。
  Node状态检查、375px真实浏览器及独立复审通过。
- `f478a3d`：普通PR/push与release复用轻量检查，测试文档明确开发时按影响范围运行，
  保留按需的浏览器、网络指纹、Docker和性能实验，不把硬件数字变成CI门槛。
- `1d4760b`：修复连接池热更新竞态，旧配置的迟到构建不再写回新缓存。
  确定性回归先在旧实现复现，再验证新出口实际可用；upstream14项与clippy通过，独立审查通过。
- `0c6b112`：普通HTTP和Images的记录跟随Body实际结束；直通图像SSE不再套用Responses终止事件。
  Images未知Token/TTFT保持null，保留账号、尝试链和上游诊断头。空Body及4MiB真实HTTP复核通过；
  forward85项集成、37项库测试与相关clippy通过。
- 固定`0c6b112`一次release构建后运行新`scripts/bench-local.py`默认6场景及cap1生命周期检查。
  32/64并发均无错误或拒绝；direct首输出p50约104/107ms，网关usage关闭约258/537ms，开启约261/518ms。
  进程全部回收，结果保留在`/tmp/ciallopa-bench-local-4q68jant`。
  先定位暖机后仍增长的首输出延迟，不归因于SQLite或擅改网络参数；两个独立审查任务分别看产品与测量。
- 架构文档已将完成的生命周期修复从路线图移到现状，下一步聚焦有证据的资源成本、请求配置一致性，
  以及由第二个真实上游验证最小供应商边界。

### 02:25 验证与性能归因

- `cf2fc46`：提交最小可重复网络驱动与完整性能报告，包含机器、二进制hash、负载、错误与资源口径。
  同实例连续64并发第二轮首输出降至108/116ms（p50/p99），首轮502/771ms；不是稳态每请求约400ms开销。
  关闭压缩无改善，跳过显式IPv4 bind的临时配置明显更快；独立socket探针测得本机单次同步bind约22ms。
  证据只说明当前WSL2的首次连接扩张成本，未改产品默认值或网络兼容参数。
- 当前整合树完整验证通过：workspace308项测试、loadbench5项、workspace/all-target clippy、fmt、
  三个Node状态脚本和actionlint。没有实际Docker引擎，未声称镜像构建/smoke已在本地验证。
- 新发现的账号设备码交互正在独立修正：取消/退出/切换身份后的迟到响应隔离，清除代码及导入文本；
  同时把低频添加表单收敛到明确入口。后续只补该范围验证，不机械重复全部测试。

### 02:55 阶段检查点

- `4417710`：HTTP准备、响应头、idle三项时限在dispatch一起捕获，沿用至Body结束和Images。
  通知屏障回归验证旧请求跨reload仍用旧idle，新请求采用新值；相关16项集成、38项库测试及clippy通过。
- `5568fd3`：图像收集只保留最终会使用的图像对象，借用非空completed数组，保留原顺序与fallback语义。
  同负载的有限异常Images对照，最高取样阶段RSS从66.59降至59.18MiB；CPU粗测均240ms。
  取消后inflight归零，记录仍为client_closed且图像Token/TTFT未知。
  本例completed数组为空，只能支持去除无用对象保留的收益；详见 [PERFORMANCE.md](PERFORMANCE.md)。
- `4fb184d`：添加账号收拢为原地展开入口，文件导入保持直接可用；设备码开始/轮询绑定身份与代次，
  退出或停止等待后旧响应不能复活代码。明确页面停止等待不等于撤销服务端授权。
  Node回归、Rust UI检查及1280px/375px虚构demo通过，独立复审通过。
- `979678d`：概况请求卡钻取使用`exclude_prewarm=1`，列表、total与分页同口径；默认列表仍含预热。
  混合fixture、API、实际卡片回调/URL恢复/翻页Node检查及相关clippy通过；独立复审通过。
  重建demo实测概况115条与钻取列表115条一致，预热另有1条。浏览器发现置顶栏挡住钻取标题，正在小修定位。
- `1d3202d`：HTTP连接与其实际出口属性成对保留；proxy关闭的reload不能把旧CONNECT失败误标成direct连接失败。
  屏障回归先红后绿，修后proxy_connect且仅1次尝试；upstream14项与clippy通过，独立复审通过。
- SSE复审复现正文出现`response.completed`会误报终止、metadata字面量会制造TTFT、done遗漏等问题。
  修正完整事件的顶层type识别正在整合；超大事件保留原尽力观察限制，不顺手造通用词法状态机。
  五个真实HTTP语义回归已通过，另做大事件透传与精确旧版本的小范围CPU/延迟对照。
- 更新行业资料核实至当前日期：Agent Router更名与部署形态、Cloudflare路由/WS兼容范围、用量与费用口径。
  具体来源及“何时值得引入”的条件已写入架构文档；未调用竞品真实API，不把能力清单当成本项目路线图。
- 新一轮总体复审认为主线仍足够轻量。本轮只继续修真实错误和契约表述；下一轮优先代表性字节成本、
  实际部署连接成本及真实CLI回归准备，不新增路由语言、全量tracing或多实例协调层。

### 03:20 阶段检查点

- `31eec10`：概况卡片/图柱钻取按置顶栏实际高度定位请求区，不再把标题和筛选藏在导航下。
  重建demo验证1280px与375px、窄屏自定义范围换行、焦点、URL恢复和第二页；Node与Rust UI检查通过。
- `42e8056`：SSE终止与首输出改为完整事件的顶层type，不再被正文中的事件名或metadata字面量误导；
  `response.done`与带空白的error正确识别。Tracker和额度观察共享有界缓冲，分片只扫描新增尾部。
  真实HTTP覆盖五类语义及9MiB合法事件透传；超出8MiB后的观察仍是尽力行为，不声称精确统计。
- `70cdbeb`：普通输出事件跳过无用的额度Value解析，保留无法识别type时的旧fallback。
  `6740617`：已有HTTP/SSE/WS警告附记录ID，HTTP另附已有上游ID，不增加日志条数或新的关联协议。
  独立复审和相关回归通过；本地截断流探针验证一条警告可反查同一条stream_truncated记录。
- 三版精确release对照已完成，192次正式请求与192次暖机全部成功，24个自建子进程全部回收。
  没有稳定CPU或延迟收益；初版usage开启时的CPU下降未在最终版重现，未改网络默认参数。
  大事件usage关闭时的观察上限反而从1MiB升为8MiB，不能宣称所有负载内存减少。
  原始数据与限制见 [PERFORMANCE.md](PERFORMANCE.md)。
- 当前`6740617`整合检查通过：workspace314项、loadbench5项、workspace/all-target clippy、fmt、
  三个Node状态脚本、actionlint及文档本地链接检查。没有Docker引擎，镜像实测仍留给发布dry run。
  临时浏览器demo、性能和日志探针均已停止；当前没有持续负载。正在进行最后一轮独立只读复核。

### 03:35 阶段检查点

- `8cc0cd2`：WS向上游发送失败归为upstream_closed/network，保留实际原因；客户端Close和Force停机保留原归因。
  15项相关WS/准入测试与clippy、fmt通过，独立复审通过。写错误分支本身未动态命中，依据明确的静态控制流修正；
  没有为这段小修增加relay抽象或测试专用生产接口。
- `c671481`：systemd unit支持reload，现有deb smoke等last_reload确认应用并核对PID不变；文档说明信号异步语义。
  purge始终保留系统user/group，删除不可靠的TOML路径猜测，只删除原有两处包目录；README与smoke同步。
  隔离rm stub验证remove/purge分支、shell语法检查及交叉复审通过。没有执行实际卸载。
  systemd静态检查升级权限后仅报未安装/usr/bin/ciallopa；完整Debian/systemd与Docker smoke仍待发布dry run。
  healthcheck文档补充磁盘配置listen与运行地址可能不同的边界，没有另造运行态地址文件。
- `3ab1ecf`：新增按需真实CLI smoke，Python标准库、4个case、独立目录/假凭据/自有PID，不加入普通CI。
  Codex 0.163.0的SSE/WS正常完成及正式请求额度重放全部通过；协议/账号链、CLI JSON与退出、usage互相核对。
  首轮确实抓到WS预热先换号的假通过；仅新增mock opt-in标记让故障落在业务turn，产品逻辑未为测试改变。
  最终产物/tmp/ciallopa-cli-smoke-3pt_m95h，13个自建进程全回收；显式构建确认测试二进制包含8cc0cd2，
  前后hash一致。实现、运行与配置隔离边界经独立审阅，mock相关clippy通过；详见 [ACCEPTANCE.md](ACCEPTANCE.md)。
- `9111dd0`：下一版发布说明改为当前AI网关定位、实际UI/指标和升级影响，明确16MiB/64并发与有限停机。
  移除旧的笼统WCAG/指纹一致性保证；没有发布、改版本或推送。
- 发布文案复核发现既有许可冲突：LICENSE为MIT，README禁止商业使用和分发。当时已异步向用户请求选择，
  等回答后再统一；此检查点没有更改许可条款。后续选择与处理见下方“04:58 许可收口”。
- 后续做有限的较大上下文/慢消费者资源观察，先由测量与架构组约定负载；不增加产品旋钮或性能门槛。

### 03:55 阶段检查点

- `8ca90e7`：README的Caddy示例改为最小reverse_proxy；官方Caddy2.11.7验证旧`encode off`无效，
  无需额外flush/read/write设置。真实Caddy→网关→mock验证首输出及时转发、头前/首输出后取消与槽位释放、
  随后成功请求字节一致；2次取消+1次成功，所有自建服务已停止，详见ACCEPTANCE。
  官方文档和固定版本源码关于负flush取消行为有差异，不作跨版本保证；本次是loopback HTTP，未覆盖TLS/CDN。
- `105cb28`：选中账号即更新现有Tracker，头前取消保留候选；无账号仍为空，未完成attempt不伪造。
  Images回归先红后绿，Responses同阶段、Force、无账号、重放与超时等7项定向集成通过；
  forward/server全目标clippy、fmt和独立复审通过。UI将status0显示为未知，JSON原值不改。
  现有Node与Rust UI检查通过；root在1280px和390px浏览器确认0/200两个取消详情与相邻导航，无页面横向溢出。
  Caddy探针的旧9111dd0产物仍保留原account=null，不将后续修复追写为历史结果。
- 字节成本四组完成，精确9111dd0 release、c4、usage开启，256KiB/1MiB输入与每请求约12MiB输出。
  准备阶段RSS约28/40–41MiB，两个暂停组观察到真实上游写阻塞；14完成+2主动取消，全部名额释放，
  38次health点测200、无记录丢失，8个自建子进程回收。详见PERFORMANCE及/tmp/ciallopa-byte-cost-qvfnwy_1。
  单轮和采样粒度不足以拆解析/压缩成本、证明长期稳定或生产容量；没有据此重写网络层或改默认参数。
- 最后有限复核转向日常测试是否假绿、部署指令是否可照做、请求详情是否保留上下文；不再扩展性能矩阵。

### 04:07 整合检查点

- `ac1a470`：账号Node检查补5秒watchdog；未决Promise原先可能无PASS却exit0，内存故障注入已确认，
  修后按期exit1；正常运行通过。没有新增测试框架或把浏览器/真实CLI加入普通CI。
- `a6260e0`、`c9f3b05`：Docker手动导入明确宿主UID适用范围，65532推荐页面导入；核对实际挂载路径，
  区分原生loopback监听与容器内部0.0.0.0、宿主loopback发布端口。
- `ccd5e73`：在途列表刷新重建行后，关闭详情按原记录恢复焦点；原行不再匹配则返回请求结果筛选。
  独立浏览器先复现Esc落BODY，Node先红后绿，Rust UI资源/CSP2项通过；root浏览器验证同条重建与被筛掉两种情况。
  全局modal机制、上下条与ID查找语义未重做。新截图保存在本会话browser-artifacts，演示数据不是实际用量。
- 在`c9f3b05`产品树上完成最终整合检查：workspace **315项**、loadbench **5项**全部通过，
  workspace/all-target clippy（deny warnings）、fmt与3份Node状态检查通过。
  日志为/tmp/ciallopa-workspace-final-0405.log、/tmp/ciallopa-clippy-final-0406.log；后续仅文档收口不重复Rust构建。
  最后契约复核纠正USAGE旧TPS公式漏×1000、Tracker计时起点和README停机只提WS的旧概括。
- 有限尝试精确对齐quinn-proto：`cargo update --offline --dry-run -p quinn-proto --precise 0.11.18`
  因缺失官方精确版本归档而exit101；未生成候选，Cargo.lock字节级未变，没有下载、改manifest或放宽allowlist。
  默认normal/build 227包及含dev 232包的包/feature图hash均与既有基线一致，均未启用quinn。
  完整命令、缺失版本与下一轮条件见/tmp/ciallopa-quinn-candidate-1efb0mlo/REPORT.md。
- 所有本轮自建demo、CLI/Caddy/mock/性能子进程已按各自句柄退出；没有使用宽泛pkill，没有推送或发布。

### 04:58 许可收口

- 用户明确选择以MIT为准。README中英文移除个人/非商业、禁止分发及公开/付费服务限制，
  明确允许商业使用、修改和分发，保留MIT声明要求、无担保说明及第三方服务条款提示。
- LICENSE与Cargo许可元数据保持MIT；发布说明、架构文档和接续待办同步统一。
  本次仅改文档，检查差异和链接，不重复Rust测试；夜间迭代仍保持暂停，未推送或发布。

### 当前约束与已知事项

- 保留单二进制和现有配置/数据；使用本地 fake upstream/demo 验证，不调用真实账号。
- 实际上游仍只有当前适配器，不提前承诺任意厂商支持。
- Rust 工具链 1.95.0 与依赖已可用。集成测试监听 loopback 需要工具沙箱升级；此前自动审查已允许。
- Git 提交使用命令级 `-c user.name=Kisaki -c user.email=127221882+Seia-Yurizono@users.noreply.github.com`，
  沿用仓库已有署名，不改全局配置。
- 本机环境无 Docker/Podman；后续v0.1.0发布已在GitHub runner通过基础Docker和systemd smoke，见末尾记录。
- 既有 lock-check 漂移：`quinn-proto 0.11.15`，官方 `rust-v0.163.0` 参考为 `0.11.18`；
  当前默认编译依赖图未启用它。详情见 [ARCHITECTURE.md](ARCHITECTURE.md)。

### 下一轮从这里继续

先读本文本轮概况及 [ARCHITECTURE.md](ARCHITECTURE.md) 的四项方向，查看`git status`和最近提交；
不要把历史验收表或旧二进制当成新checkout的结果，也不需要原样重做全部研究和性能矩阵。

1. **扩展部署验收。** 基础release流水线已在后续v0.1.0预发布通过。
   继续以少量操作确认Compose首次启动、现有数据重建/恢复、活动SSE/WS关停与Debian旧版升级；当前smoke不覆盖这些全部场景。
2. **单独维护锁文件。** 参考官方rust-v0.163.0，补齐quinn-proto0.11.18、rand0.10.1、chacha20.10.2等精确归档后，
   用Cargo解析并检查全部传递版本，要求默认包/feature图不变、lock-check无新漂移。不要手工拼锁或扩大allowlist。
3. **按真实使用选择下一项。** 第二个实际API-key上游先完成一条HTTP/SSE路径，再抽象两者真正不同的部分；
   性能下一步优先真实Linux/TLS、代表性上下文与持续负载/查询争用，不用WSL首次bind成本改变默认网络行为。
   UI先看实际长时间调查是否还有打断，再考虑侧栏或新指标；不增加空的provider字段、假阶段瀑布图或大监控系统。

开发时按TESTING选择受影响检查；只有相关变更/失败/新疑点才重复已通过的检查。旧wait_rows存在嵌套轮询，
触及时再合并为一个期限，不为此次收尾批量重写旧测试。临时证据目录可能被系统清理；持久结论已写入相应docs。

## 后续补充：单HTML与UI框架评估

用户询问单HTML承载与框架的性能/表现力收益。对`8a44d0e`源码、Svelte/Vue官方文档及独立子代理意见复核后，
建议保留Rust静态嵌入交付，前端源码转向组件和状态模块；优先用Svelte + TypeScript + Vite做请求列表/详情的局部对照，
熟悉Vue时可选择Vue。三页约250 KiB，当前证据主要是复制、全表重建与手工状态管理成本，尚无框架性能对照结果。
迁移范围、构建/CSP适配及必要验证见 [UI_REVIEW.md](UI_REVIEW.md)“源码组织与框架选择”。
本次仅完成评估与文档更新，没有新增前端依赖或修改产品；原夜间迭代保持暂停。

## 2026-10-23 v0.1.0预发布重建

- 用户明确要求强制推送、清理旧artifacts/release并重新构建v0.1.0预发布。main已使用精确
  `--force-with-lease`推送，v0.1.0标签重新指向`17525d070d4a10aa11a08000f1f61dd84ae8b179`。
  已删除旧正式release、18个旧Actions artifacts和首轮失败尝试的3个artifacts；保留成功构建的3个新artifacts。
- `4a7a57a`：发布工作流将v0.1.0明确标为Pre-release且不设为latest；镜像只发布精确`0.1.0`标签，
  不移动`0.1`/`latest`。Compose已使用精确版本，说明明确同名旧版本需重新下载/拉取。
- 首次真实CI通过检查、deb/systemd和Docker smoke，但在发布镜像标签前停止：带attestation的Buildx输出
  没有`containerimage.config.digest`。`17525d0`改为读取上传索引中唯一linux/amd64 manifest的config摘要，
  与smoke构建及本地加载镜像比较；保持校验失败就停止发布。
  GHCR实际manifest已核对一致；本地6个针对性检查覆盖成功、两类摘要不符、manifest重复/缺失和非法digest。
- [最终发布流水线](https://github.com/Seia-Yurizono/ciallopa/actions/runs/39885578733)全部成功，
  [同提交分支检查](https://github.com/Seia-Yurizono/ciallopa/actions/runs/39885575068)成功。
  [v0.1.0](https://github.com/Seia-Yurizono/ciallopa/releases/tag/v0.1.0)于新加坡09:47发布，
  API确认`isPrerelease=true`、`isDraft=false`。
- 已重新下载tar.gz、deb和SHA256SUMS；两份附件校验通过，实际二进制报告`ciallopa 0.1.0`，
  Debian包为`0.1.0-1 / amd64`。本地核验文件在/tmp/ciallopa-prerelease-17525d0。
- 镜像`ghcr.io/seia-yurizono/ciallopa:0.1.0`实际revision匹配发布提交，用户为`65532:65532`；
  索引摘要为`sha256:7cb6efbec2cb89092d86fa81e5d9409f906773a841815cb74139a700083d957f`。
  本次补齐基础真实部署证据，未将其扩大为升级恢复、所有活动流关停或生产容量已验收。

发布标签保留在已验证的`17525d0`；后续发布记录提交仅更新文档，不再次移动版本标签。
