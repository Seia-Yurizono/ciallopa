# Release notes — v0.4.0

本版本基于 v0.3.0，新增账号额度的重置记录与每周期容量估算（usage 数据库 schema v15），在账号页加入 30 天重置时间线和阶梯折线图，修正首页筛选，并将固定的 Codex 版本对齐到 0.167.1。

**额度重置记录**

- 按 5h 和 7d 两个窗口分别识别重置，分为 `natural`（自然重置）、`manual_card`（手动用卡）、`auto_card`（快到期时自动用卡）、`external_card`（其他端用卡）和 `early_reset`（提前重置，合并官方额外重置与付费即时重置）。仅 `resets_at` 后移不算重置；`resets_at` 未变而用量百分比下降记为 `limit_change`（额度变化），也不算重置。
- 重置卡因到期而减少（额度没有恢复、`resets_at` 未变）不会被记为重置或 `external_card`。卡的 `expires_at` 从不作为窗口的 `resets_at`，参见 [openai/codex#60664](https://github.com/openai/codex/issues/60664)。
- 重置事件和刚结束周期的估算，与触发它的 `quota_obs` 行在同一个 SQLite 事务中写入，请求热路径上没有额外写入。成功用卡后，网关会把随即拉取的 `/wham/usage` 快照经现有写入队列记录下来。数据随 usage 保留期（默认 90 天）一同清理。

**每周期容量估算**

- 容量以 credits 为单位（官方 Standard credit 费率，Fast ×2.5、Ultrafast ×8），不换算为美元。估算采用按 Δ% 加权的 Theil–Sen 斜率，只使用百分比相差至少 5 pp 的观测对；区间由整数百分比的量化边界求交集得出。交集为空时视为可能有其他端用量，退回相邻斜率的中位数或只给下界；数据不足时不显示，而不是显示 0。
- 每个已结束的周期另存一份 `api_cost_usd`，即同样的请求按 API 价格计算的费用（与现有 `cost_usd` 同一公式）。跨周期不做平滑，超出历史中位数 0.7–1.4 倍的周期会被标出。
- 新增只读接口 `GET /usage/v1/quota/history?account=<id>&days=30`；`GET /usage/v1/quota` 的 `estimate` 新增 `capacity_credits`、`remaining_credits`、`capacity_lo` / `capacity_hi`、`coverage_gap`、`confidence` 和 `api_cost_usd`，原有基于 token 权重的字段保留。字段定义见 [docs/USAGE.md](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/USAGE.md)。

**管理页面**

- 账号页每行新增「额度记录」：「重置记录 · 30 天」按 5h / 7d 两行显示重置时间线，下方「周期容量 · credits」用阶梯折线加范围带画出每个周期的容量，虚线为历史中位，空心点表示低置信度，圆环标出偏离中位的周期；单独的「API 价格」面板以美元显示每周期费用。完整数值收在「明细」里。时间线与图表共用同一横轴，浅色和深色主题均可读；账号表格横向滚动时面板保持在可见区域内。
- 首页的模型、Key 标签和实例筛选改为不区分大小写的子串匹配，与下拉建议一致；账号仍为精确匹配。`%`、`_` 和反斜杠按字面匹配，宽泛的匹配也不会触及 SQLite 的绑定变量上限。
- 为 Codex 新增的 8 个上游终止错误码（如 `content_policy_violation`、`budget_reached`、`codex_workspace_access_denied`）加上中文短标签，错误分类不变。

**价格与 Codex 对齐**

- Ultrafast 的默认倍率改为 Standard 的 6 倍（官方 API 价格：GPT-6 Astra 与 GPT-6.1 Sol 的 Ultrafast 为 Standard 的 6 倍）。此前未单独设置 `ultra_x` 时，Ultrafast 请求显示为未定价，升级后会按此倍率计算费用；`prices.toml` 中按模型或 `[defaults]` 设置的 `ultra_x` 仍然优先。
- 固定的 Codex 版本从 0.166.0 经 0.167.0 对齐到 0.167.1：默认 User-Agent、`client_version` 以及指纹测试夹具同步更新。与 fp-survey 抓包对比，JA4、Akamai h2、密钥交换组和各端点请求头顺序保持一致。

**升级**

- 首次启动时，usage 数据库自动从 schema v14 升级到 v15。此次迁移只为 `quota_obs` 新增可空的 `cards` 列，并新增 `quota_reset_events` 和 `quota_cycle_estimates` 两张表，不改写已有数据；重置记录从升级后观测到的快照开始积累。配置文件无需修改。
- 旧版本镜像不会降级已迁移的数据库。如需回滚，请在升级前按[备份与恢复](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/DEPLOYMENT.md#backup-and-restore)保存数据目录和当前镜像的离线归档。
- 发布后，`latest` 指向 0.4.0，并新增 `0.4` 别名。按[容器清理策略](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/CONTAINER_CLEANUP.md)，GHCR 只保留最新的两个标签名，固定在 `0.3.0` 或 `0.3` 的部署之后可能无法再拉取该镜像。拉取和重建前，请先更新固定的镜像标签，见[部署指南](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/DEPLOYMENT.md#image-tags-and-upgrades)。

**尚未包含**

- 容量估算只统计本数据库记录的请求。同一账号在其他客户端或其他 ciallopa 实例上的用量会使估算偏高，这类周期会标记为可能有其他端用量，但无法精确扣除。没有公开 Standard credit 费率的模型不计入 credits。

**验证**

- 本地 workspace 测试 555 项全部通过，涵盖重置分类（含卡到期不记为重置）、Theil–Sen 估算与量化区间、v14→v15 迁移、单事务写入以及 `/wham/usage` 快照映射。loadbench 示例 5 项通过。严格 Clippy、格式检查，以及 UI、设置、账号和请求观测渲染四项 Node 检查均已通过；账号页的时间线位置与阶梯折线几何由 Node 检查断言。指纹检查（fp-check）通过。安装包与容器的冒烟测试在标签工作流中运行。
- 依赖锁文件只更新了本仓库各 crate 的版本号，第三方依赖没有变化（`rusqlite` 作为已有依赖，新增为 `ciallopa` 的开发依赖，供 UI 演示生成合成数据）。验证全部使用合成数据和本地模拟上游，未调用真实模型上游。
