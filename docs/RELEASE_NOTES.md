ciallopa v0.2.1

This release follows v0.2.0. It records requests refused by local limits and per-minute
occupancy, so `max_inflight_requests`, `ws_max_connections` and the buffer budgets can be tuned
from history. An idle WebSocket no longer holds an inflight slot. Failures are classified (quota,
rate limit, upstream, network, stream, local) and the overview breaks them down in place, with
an upstream failure rate, a previous-period comparison and latency percentiles. 刷新额度 now
clears a usage-limit cooldown when upstream reports the quota restored, and it can run in the
background for accounts that opt in. The implemented upstream adapter is still the Codex backend.

**Admission and WebSocket**

- A WebSocket now holds one `max_inflight_requests` slot per running turn (from the client
  request to its terminal event, cancel or close), not for the whole connection. Idle
  connections and the handshake are outside the cap; connections stay bounded by
  `ws_max_connections`. A turn at capacity gets a 503 `gateway_busy` error frame, is not
  forwarded, and the connection stays open.
- Every admission 503 `gateway_busy` (inflight cap, body buffer budget, WebSocket connection cap
  and the WebSocket busy frame) now advertises `Retry-After: 3` (`retry-after-ms: 3000`)
  instead of 1, so Codex's fast 5xx retries space out.
- GET `/v1/models`, `/backend-api/codex/models`, `/backend-api/wham/usage` and
  `/api/codex/usage` are exempt from the inflight cap. Codex polls them; refusing them made the
  client look broken while the cap was full.

**Refusals**

- Requests refused by a local limit (`inflight`, `ws`, `body`, `ws_msg`) are counted per minute
  in a new `refusals` table (schema v10), grouped by reason, transport, key, route, User-Agent,
  instance and limit, with the highest occupancy seen at a refusal. Counters are flushed every
  5 s and follow `retention_days`. Refusals while shutting down are not counted.
- Logs carry at most one WARN per reason per minute with `refused=<n>`; sum `refused` or use the
  API instead of counting lines.
- New `GET /usage/v1/refusals`; `GET /usage/v1/summary` returns `refusals` for the same range,
  key and instance; `GET /admin/v1/config` → `runtime.rejected_by` gives per-reason
  process-lifetime counts.
- The 请求 tile shows 「已拒绝 N」 with a per-reason breakdown; 「查看记录 ›」 sets 结果 = 已拒绝 and
  lists the per-minute refusal rows.

**Occupancy history**

- About once a second the process samples inflight, WebSocket connections and buffered body
  bytes, including the exact peak between samples, and stores one row per minute and instance in
  a new `occupancy` table (schema v11). Read it at `GET /usage/v1/occupancy`.
- The 实时 row shows only 转发 / WS now, plus the range peak and 满载 minutes; clicking it switches
  the trend to 占用与拒绝 (peaks against the dashed limits, refusals as bars).

**Failures**

- Every failure gets one class: `quota` (usage-limit codes, including the local
  `all_quota_exhausted` / `first_quota_reset`), `rate_limit` (other 429s), then `upstream`,
  `network`, `stream` or `local` by error source, `other` for old rows. The summary returns
  `failure_classes`, `upstream_failures` / `upstream_failure_rate`, retry / account-switch /
  downgrade counts and p99 latencies; `group=error` and `group=err_source` are new groups; the
  records API filters by `failure_class`.
- The upstream failure rate counts only error HTTP responses from the upstream itself
  (including its 429s), not local, network or in-stream errors.
- `compare=1` on the summary returns the previous equal-length window.

**Overview**

- Failure analysis sits in the tiles instead of a separate panel: the 失败率 tile shows 「上游
  x%」, a bar by failure class and the top three classes as links to their records.
- New trend metrics 失败原因 (stacked by class), 延迟分位 (首输出 p50 / p90 / p99) and 占用与拒绝.
  对比上一周期 (on by default) adds KPI deltas and a dashed previous-period line.
- 分布 adds 接口, 推理, 传输, 档位, 错误码 and 错误来源 groups and 上游 / 重试 / 降级 columns. Error
  codes show a Chinese label with the raw code on hover.
- The request list has a 失败原因 filter; choosing one sets 结果 to 失败. `first_quota_reset` is
  shown as 额度用尽.

**刷新额度 and cooldowns**

- A successful 刷新额度 (`GET /wham/usage`) now reads `rate_limit.allowed`. When upstream reports
  `allowed` and not `limit_reached`, the account's usage-limit and rate-limit cooldowns are
  dropped the same way a used reset card drops them (`cooldown_cleared` in the response; the
  page says 已解除额度冷却). A missing `allowed` never clears.
- New per-account switch 「自动刷新额度」 (off by default) on the accounts page. Accounts with it
  on are refreshed in the background every `[ui] quota_refresh_mins` minutes (default `0` =
  off; 1–60, the settings page lands on 5 when switched on) through the same path as the manual
  button: four at a time, the 30 s per-account throttle still applies, disabled accounts
  included, accounts that need a new login skipped. The switch needs `allow_config_write` and
  `[usage]`, where it is stored. New `POST /admin/v1/accounts/{id}/quota-refresh/auto`.
- 设置 › 账号与重置卡 has a new 后台刷新 section with both refresh intervals.

**Validation**

Checked with the workspace tests (419 passed), the UI, settings and account state
checks, clippy with warnings denied and the container cleanup rule tests. No real model provider or account
was used.

**Upgrade**

- Image tags: `ghcr.io/seia-yurizono/ciallopa:0.2.1`, `0.2` and `latest`, for `linux/amd64` and
  `linux/arm64`. Change a pinned `0.2.0` Compose tag before pulling and recreating.
- **Behavior change:** `[ui] reset_credits_refresh_mins` now defaults to `0` (reset cards are
  listed only on demand) instead of 60. Set it explicitly to keep a background listing refresh.
  Both `reset_credits_refresh_mins` and `quota_refresh_mins` must be `0` or 1–60; a larger value
  is now a configuration error.
- `max_inflight_requests` counts running WebSocket turns instead of open connections, so the same
  value admits more idle Codex sessions. Clients that relied on `Retry-After: 1` from
  `gateway_busy` now see 3 s.
- The usage database is upgraded in place to schema v11 (two new tables only).
