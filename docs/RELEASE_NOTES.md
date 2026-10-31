# Release notes

This patch release follows v0.2.4. It improves the management UI's readability,
keyboard navigation and mobile layout, and gives the memory panel a compact default
view. The existing error catalog, streaming behavior and Codex 0.166.0 compatibility
baseline are retained.

**Request investigation and readability**

- Keep account, model and key filters visible on small screens, with “更多筛选” in
  the request heading for less common conditions.
- Adapt overview metric columns to the available width; keep primary values, cache
  rates and upstream rates readable instead of truncating them.
- Give mobile request actions and account controls enough space. Quota navigation
  places its focused refresh button below the fixed time bar.
- Keep response headers and JSON bodies readable in request details. Lock background
  scrolling while dialogs are open and restore the originating row's focus and position.

**Settings and memory**

- Associate Chinese validation messages with their fields. Replace the short
  double-click discard window with an explicit confirmation dialog, defaulting to
  cancel; restore the field's focus and scroll it into view when needed.
- Clarify that configuration read-only mode still permits authorized runtime actions.
- Show RSS, buffer usage and WebSocket connections in a compact memory summary.
  Expand once to see the full metrics and history; refresh preserves the expanded
  state and focus. Threshold warnings remain visible when collapsed, and unlimited
  caps are explicitly labeled “不限” in both views.

**Validation and upgrade**

- The UI candidate was evaluated through successive independent manual browser
  reviews. All 14 cases have passing evidence from the final full review plus a
  targeted recheck of the unlimited-cap wording. The release integrates that candidate
  onto v0.2.4; it does not claim another full manual pass after integration. The
  [experiment report](https://github.com/Seia-Yurizono/ciallopa/blob/v0.2.5/docs/research/UI_HILL_CLIMBING_RESULTS.md)
  records scope, screenshots, measurements and remaining limitations.
- No new automatic UI tests were added. Existing state checks are adapted for the
  browser APIs and text used by the updated UI.
- No configuration migration or database migration is introduced relative to v0.2.4
  (usage schema remains v13). When upgrading from an older release, follow its migration
  notes as well. Update a pinned image tag before pulling and recreating; see the
  [deployment guide](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/DEPLOYMENT.md#image-tags-and-upgrades).
