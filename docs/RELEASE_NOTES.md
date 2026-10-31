ciallopa v0.2.4

This release follows v0.2.3. It reduces sensitive data in stored diagnostics, adds a
Codex session filter, and makes streaming failures and capacity limits easier to
diagnose. The Codex client compatibility baseline is now 0.166.0. The implemented
upstream adapter is still the Codex backend; this release does not add Claude support.

**Diagnostics and privacy**

- Usage records persist stable account aliases instead of raw account identifiers.
  The upgrade rewrites historical account labels, reset-consume records and captured
  error details, and sanitizes historical account keys in the configuration audit log.
- Stored error payloads remove response-content fields, and stored user agents are
  shortened. Configuration errors, proxy usernames and stderr emails receive additional
  redaction.
- Non-admin quota and reset-credit responses hide email addresses and card details.
  Administrative account management remains available to admin keys.

**Session view**

- Filter requests by a full Codex thread ID or a unique fragment of at least eight
  characters. The request and count APIs accept `thread`; ambiguous fragments return 400.
- The UI retains the filter in the URL and shows an active filter chip. The request-detail
  action “只看这个会话” clears the shared scope so a session spanning accounts is shown in full.
- A partial thread index supports the session view without indexing requests that have
  no thread ID.

**Streaming and error handling**

- A terminal response event settles the recorded outcome. A later idle timeout or read
  error no longer turns an already completed response into a failed generation.
- Preserve upstream in-stream error codes and classify incomplete responses by their
  reason. Stream read failures and missing terminal events have separate codes;
  truncated wrapped Images responses return 502.
- Distinguish WebSocket close, EOF, read and send failures, and break connection failures
  down into DNS, TLS, connect timeout and other connection errors. Soft WebSocket error
  frames end a turn only under the conditions used by Codex.
- Treat `preparation_timeout` as a local gateway failure. Record upstream silence when
  a client closes, and warn when the configured stream idle timeout exceeds Codex's
  300-second timeout.
- Unify the error-code catalog and show the English code with its corresponding Chinese
  label in the UI.

**Codex compatibility and capacity**

- Update the default client version and compatibility probes to Codex 0.166.0.
- Handle `policy_capacity_unavailable` as upstream overload and
  `turn_rate_limit_exceeded` as temporary capacity throttling rather than exhausted
  subscription quota. Eligible requests can recover before output is committed.
- Apply model cooldown to in-stream `response.failed` rate-limit errors.

**Validation and publication**

The tag-triggered release workflow runs the shared formatting, UI/state, cleanup,
Clippy and Rust test checks before packaging. Native amd64 and arm64 runners build
and smoke-test the packages and containers. Publication verifies that the uploaded
runtime images match the smoke-tested images before creating the multi-architecture
tags and GitHub Release. These checks use synthetic data and local mocks, not real
provider accounts.

**Upgrade**

- Image tags: `ghcr.io/seia-yurizono/ciallopa:0.2.4`, `0.2` and `latest`, for
  `linux/amd64` and `linux/arm64`. Update a pinned `0.2.3` Compose tag before pulling
  and recreating.
- The usage database upgrades automatically from schema v11 in v0.2.3 to v13. The v12
  migration rewrites historical diagnostic data; v13 adds the session index.
  Back up the data directory and configuration before upgrading. An image rollback
  does not undo the migration; restore the matching pre-upgrade backup when rolling back.
- No configuration-file migration is required. Consumers that parse diagnostic codes
  or depend on raw account labels should account for the refined codes and aliases.
