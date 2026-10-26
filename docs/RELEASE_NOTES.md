ciallopa v0.2.3

This release follows v0.2.2. The admin pages (概览, 账号, 设置) get a lighter, calmer layout: a
sidebar shell, KPI cards, a line trend chart and a compact accounts table. It also fixes the 卡
column on the overview quota card, which showed empty buttons in v0.2.2. Wording, switches,
confirmations, the API, the configuration and the usage database are unchanged. The implemented
upstream adapter is still the Codex backend.

**Layout**

- Light sidebar shell with a page title, softer colour tokens and card shadows. Every layout has
  nav icons, and phones get an icon-over-label tab bar.
- Between 1024 and 1359 px the sidebar collapses to an icon rail (links keep their titles) so
  wide tables keep their room.
- 设置: on wide desktops the group nav joins the app sidebar instead of a second left column.
- Light theme colours: the orange warning and cooling states (冷却中, the cooling and price
  banners, 未定价, 已拒绝, 满载, 未报告, stale-data marks, quota bars between 20% and 50% left)
  become a neutral grey; errors (失败率, 次错误, 令牌失效, quota bars under 20%) use a true red;
  the 额度用尽 failure class becomes a warm grey. Dark mode is unchanged.

**Overview**

- KPI cards of equal height with headline numbers, the top two failure classes under 失败率
  and the outcome links under 请求. Sub lines wrap after a separator instead of being cut off on
  phones.
- The time range is a segmented control on desktop; phones keep the select.
- The trend is a line chart with a fading area, a quieter dashed previous period, a peak label,
  a solid 0 baseline and a count axis on round steps. A partial bucket (the first or current
  hour) is a hollow dot joined by a faint dotted link; the line breaks at buckets without usage
  reports, and flat runs of zero 失败/取消 are not drawn.

**Accounts**

- Status counts as pills; compact rows with a switches column, a one-line status and quiet text
  actions (删除 keeps its colour).
- From 761 to 1279 px the table fits without horizontal scroll: row actions in a 2×2 grid,
  保存 under the priority input and tighter cells.

**Fixes**

- 卡 column on the overview quota card: the v0.2.2 problem chips reused the function name
  `chip`, which replaced the reset-card chip builder, so the column rendered empty buttons and
  the quota percentages picked up the pill style. A test now rejects a function declared twice
  in one page script.
- docs/USAGE.md: `reset_credits_auto_lead_mins` is back inside the `[ui]` reset-card table.

**Validation**

Checked with the workspace tests (432 passed, twice), the UI, settings and account state
checks, rustfmt and clippy with warnings denied. Screens were reviewed on the demo server with
fake data only. No real model provider or account was used.

**Upgrade**

- Image tags: `ghcr.io/seia-yurizono/ciallopa:0.2.3`, `0.2` and `latest`, for `linux/amd64` and
  `linux/arm64`. Change a pinned `0.2.2` Compose tag before pulling and recreating.
- No configuration change is required and the usage database is not migrated (still schema
  v11).
- No behaviour change outside the admin pages' look; existing links and API parameters keep
  working.
