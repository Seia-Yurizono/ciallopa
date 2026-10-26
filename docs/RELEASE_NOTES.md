ciallopa v0.2.2

This release follows v0.2.1. Both background refresh timers (刷新额度 and the reset-card
listing) take a minimum–maximum range in minutes instead of a fixed interval, and every
opted-in account draws its own random next refresh, so accounts are no longer asked upstream in
lockstep. The request list on the overview is reorganised around problems a user can act on:
报错, 上游出错, 没答完 and 本地拒绝, each with a count, a cause breakdown and suggested next steps.
The implemented upstream adapter is still the Codex backend.

**Background refresh ranges**

- New `[ui] quota_refresh_max_mins` and `[ui] reset_credits_refresh_max_mins`. The existing
  `quota_refresh_mins` / `reset_credits_refresh_mins` are now the shortest gap and the new keys
  the longest; each next run is drawn uniformly in [min, max] minutes. `*_mins = 0` is still
  off. `*_max_mins = 0` (the default) means the same as `*_mins`, so a config with only the old
  single value loads unchanged as a fixed interval. `*_max_mins` must be `0` or 1–60 and not
  below `*_mins`; otherwise the config is rejected with an error naming both keys.
- 刷新额度 schedules every account whose 「自动刷新额度」 switch is on independently. The first
  refresh after server start, after an account's switch is turned on, or after the range is
  turned on also waits a random [min, max]. The reset-card listing draws one random gap per round
  over all accounts. Next times are kept in memory only; a range change applies on the next look
  at the config (within 30 s) and redraws pending times from the start of their gap.
- The refresh path itself is unchanged: four accounts at a time, the 30 s per-account throttle,
  disabled accounts included, accounts that need a new login skipped, and the usage-limit
  cooldown cleared when upstream reports the quota restored. A manual 刷新额度 does not move an
  account's next automatic refresh.
- 设置 › 账号与重置卡 › 后台刷新 shows each timer as a switch plus 最短–最长 inputs, filled with
  5–10 when switched on; equal values give a fixed interval and 最短 > 最长 is flagged.
  `GET /usage/v1/reset-credits` also returns `refresh_max_mins` and `quota_refresh_max_mins`.

**Request list**

- The list opens with problem chips 全部 / 报错 / 上游出错 / 没答完 / 本地拒绝, each with its
  count, and a cause row under the chosen chip:
  - 报错: the client got an error (counted in 失败率), by 额度, 限流, 上游, 网络代理, 网关 and 流中断.
  - 上游出错: the upstream side failed at least once, split into 未救回 and 已救回 (a transport
    retry or an account switch rescued it). It can overlap 报错 and is not a second failure
    rate; 上游失败率 keeps its definition.
  - 没答完: 客户端断开, 上游截断 (`response.incomplete`) and 未确认结束 (formerly 结果未知); not
    counted as failures. The former 未完成 label is replaced by these causes.
  - 本地拒绝: refusals by a local limit, by reason; disabled while an account, model or another
    request-only filter is set, because a refusal happens before those are known.
- Account, model and key filters sit next to the chips. 结果, 失败原因, 错误来源, 档位, 错误码, 重试
  and the new 含预热 are under 更多. 全部 excludes prewarm requests unless 含预热 is ticked. The old
  URL and API parameters keep working, so old links still open.
- Each problem row has up to two 下一步 buttons (one on a phone), and the detail panel opens with a
  结论 block that says what happened and whether it counts as a failure, followed by the attempt
  timeline. Next steps only navigate to existing views (account, cooldown, attempt chain, egress
  and limit settings, 刷新额度 on the quota card); nothing is refreshed or spent automatically and
  no reset card is used.
- Classification is implemented once, in `crates/usage/src/problem.rs`, for rows, filters and
  counts. `GET /usage/v1/records` accepts `problem=all|error|upstream|unfinished` and
  `cause=<cause>` (`problem=refused`, an unknown value or a cause without a problem → 400), and
  each row carries `problem`, `cause`, `upstream_fault`, `rescued` and `next`. New
  `GET /usage/v1/records/counts` returns the per-chip and per-cause counts for the same filters,
  with refusals from the refusals table.
- A request that finally succeeded after an HTTP stream account switch or a WebSocket replay now
  keeps its attempt chain in the error detail, so the detail shows the earlier failures. A
  same-account overload wait and retry is recorded as `wait+retry` in the chain but is not marked
  已救回, because `retries` counts transport retries only. Requests without a retry store nothing
  extra. The chain goes through the existing batched writer; the usage schema stays at v11.

**Validation**

Checked with the workspace tests (431 passed), the UI, settings and account state checks,
clippy with warnings denied and the container cleanup rule tests. No real model provider or
account was used.

**Upgrade**

- Image tags: `ghcr.io/seia-yurizono/ciallopa:0.2.2`, `0.2` and `latest`, for `linux/amd64` and
  `linux/arm64`. Change a pinned `0.2.1` Compose tag before pulling and recreating.
- No configuration change is required and the usage database is not migrated (still schema
  v11).
- **Behavior change:** with a background refresh turned on, the first run after a start no longer
  happens about 20 s after start; it waits a random [min, max] minutes (with only the old single
  value, the full interval).
