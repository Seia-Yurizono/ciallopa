ciallopa v0.2.0

This release follows v0.1.2. It asks for confirmation before irreversible account actions,
lets you price or ignore unpriced models, reworks the settings page, adds an opt-in automatic
reset-card use, bounds WebSocket memory and shows memory use on the overview. The overview
now leaves prewarm out by default, suggests filter values, shows the input cache rate and
replaces the grey footnotes under the cards with state that only appears when it matters.
The implemented upstream adapter is still the Codex backend.

**Safety**

- 更新令牌 (token refresh) no longer runs on one click: it opens a danger dialog naming the
  account, because a refresh invalidates the old refresh token (and logs out any other tool
  sharing that auth.json). The refresh API now requires `{"confirm": "<account id>"}`, like
  delete.
- Pasting tokens for an account that already exists used to replace its tokens silently.
  The UI now does a `dry_run` import first and asks before replacing; a dry run writes
  nothing and reports whether the target exists.
- Deleting a client key shows a 不可逆 tag in its confirmation.

**Prices**

- Unpriced models are listed in settings with 设置价格 / 忽略. Ignored models cost exactly 0
  and no longer trigger the unpriced hint. Custom prices are never overwritten by price
  updates. Writes go through `POST /admin/v1/prices/models`, are atomic, throttled to one
  per second, audited, and back up a hand-written table once.

**Settings page**

- Settings are split into grouped pages (转发 / 数据与账号 / 系统) with a left nav, a search
  over every page and 只看已修改. On phones the nav is a drill-down list with a one-line
  summary per page. Old `#g-*` links still land on the right page.
- Rarely used options sit in a collapsed 高级 section per page. Dependent settings are
  indented under their parent and greyed out while it is off (values are kept).
- Non-default rows show a blue bar with 「默认 X · 恢复」. Unsaved switches show diagonal
  stripes in the new value's colour; the nav marks pages with unsaved changes and the save
  bar counts them. Leaving the page with unsaved edits asks first.
- 变更记录 now refreshes when shown and every 15 s while visible.

**Reset cards**

- New per-account switch 「快到期时自动使用」 (off by default) on the accounts page. When it is
  on, a background sweep uses the earliest-expiring available reset card within
  `[ui] reset_credits_auto_lead_mins` (default 10, range 5–1440) of its expiry, and the page
  shows 「下次自动使用 HH:MM」. Turning it on needs `allow_config_write`.
- Manual and automatic use share one per-account lock and re-check the card list, the
  switch and the account state under it. Attempts are logged with key label `(auto)`, so a
  restart retries with the same redeem id; at most 5 tries per card, at least 60 s apart,
  never past expiry. Switch states are kept in the usage database.

**WebSocket memory**

- New hot settings `ws_max_connections` (default 24) and `ws_max_message_mb` (default 16),
  sized for a 2 GB host. Over the connection cap the handshake gets a 503 (Codex falls back
  to HTTP); an oversize message closes the connection with 1009. In a 40 × 12 MiB local test
  the peak RSS dropped from 1384 MiB to 940 MiB with the defaults.
- WebSocket turns now re-check the account's per-model cooldown for each message's model.
  The handshake chose an account before the model was known, so a model cooling on that
  account was still used.

**Memory panel**

- The overview has a 内存 panel: process RSS and peak, jemalloc allocated / resident (when
  jemalloc is the allocator), buffered request bodies against `max_buffered_body_mb`,
  WebSocket connections against the cap with their buffered bytes, and a 15-minute
  sparkline. It reads the new `GET /admin/v1/memory`, sampled every 10 s in memory only.

**Upstream and clients**

- Responses that carry `Retry-After` now also carry `retry-after-ms`, clamped together to
  1–20 s; when the upstream sends only `retry-after-ms`, `Retry-After` is derived from it.
  WebSocket error frames carry both.
- The default `images.main_model` is now `gpt-6.1-sol`.

**Overview and usage**

- Prewarm (Codex WebSocket `generate:false`) is hidden by default: tokens, cost, charts,
  per-account and per-model usage and the request list count business requests only. A
  「含预热」 checkbox in 筛选范围, remembered in the browser, includes it again; the 预热
  outcome filter still lists prewarm rows. The summary, trend and group APIs accept
  `exclude_prewarm=1` and the accounts API accepts `include_prewarm=1`; API defaults and
  stored data are unchanged.
- The 模型 / 密钥 / 实例 filters suggest values from the usage database (most used first,
  with counts and last use) plus the configured keys and instance. They are accessible
  comboboxes that accept free text and work on phones. Suggestions come from the new
  `GET /usage/v1/scope-values` (newest 200k rows, cached 60 s, at most 50 per field).
- The Token card shows the input cache rate (「缓存 46.2%」: cached input tokens / input
  tokens) with a tooltip; it follows the filter scope and the 含预热 setting. The duplicate
  entry under the cards is gone.

**Overview footnotes**

- The grey lines under the overview cards are gone. Explanations that never change (window
  average, sample basis, data source, prewarm scope) moved into tooltips or were dropped.
- Card sub-lines use units: requests show `RPM`, latency shows p90 only. The failure-rate card
  lists only non-zero outcomes (unknown in amber) and turns red at 5 % or more.
- Unpriced models show an amber chip on the cost card that opens 设置 › 价格.
- The Token card shows an amber 「N 未报告」 when finished requests came back without upstream
  token usage, so tokens and cost are undercounted. The count is exact: the summary API returns
  a new `no_usage` field in its totals and groups (prewarm, failed, cancelled and unknown
  requests are not counted).
- Lost or failed usage writes show as a red banner above the cards. Stale data dims the strip
  and chart instead of adding a note.
- Live forwarding and WebSocket connections sit in a 实时 row inside the overview panel: grey
  when idle, green with traffic, amber from 80 % of a cap or when refusing, red while shutting
  down. The `/cap` part shows only from 80 %.
- The display timezone and the internal request User-Agent moved to 设置 › 常规.

**Validation**

Checked with the workspace tests (398 passed), the UI state checks, desktop and phone
browser screenshots against a mock demo server, a 40 × 12 MiB WebSocket memory test and a
WebSocket per-model cooldown reproduction. No real model provider or account was used.

**Upgrade**

- Image tags: `ghcr.io/seia-yurizono/ciallopa:0.2.0`, `0.2` and `latest`, for `linux/amd64` and
  `linux/arm64`. `0.1` stays on 0.1.2. Change a pinned old Compose tag before pulling and
  recreating.
- Scripts that call the token refresh API must now send `{"confirm": "<account id>"}`.
- More than 24 simultaneous client WebSocket connections, or WebSocket messages over 16 MiB,
  are now refused by default; raise `ws_max_connections` / `ws_max_message_mb` (0 disables
  the connection cap) if you need more.
- The new `images.main_model` default applies only if the key is not set in `config.toml`.
- The config/data layout and SQLite schema are unchanged. Keep a data backup and the
  previous image locally: registry cleanup keeps only the newest two tag names.
- Download the .deb or static musl tarball and verify it against the accompanying SHA256SUMS.

[Installation][install] · [Recovery boundaries][recovery] · [Deployment and backup][deploy] · [Documentation][docs]

Licensed under the [MIT License][license].

[install]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.2.0/README.md#quick-start-linux-x86_64--arm64
[recovery]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.2.0/docs/RECOVERY.md
[deploy]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.2.0/docs/DEPLOYMENT.md
[docs]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.2.0/docs/README.md
[license]: https://github.com/Seia-Yurizono/ciallopa/blob/v0.2.0/LICENSE
