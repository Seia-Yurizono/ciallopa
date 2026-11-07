# Release notes

This patch release follows v0.2.5. Direct HTTP Responses requests can now ask the model
upstream for gzip while ciallopa stream-decodes the reply before error handling and SSE
observation. Downstream clients continue to receive plaintext, so the upstream leg can use
compression independently of the client's accepted response encodings.

**Upstream response compression**

- New hot setting `[upstream_opts] gzip_responses = true` is enabled by default. It applies
  only to direct `POST /v1/responses` and `POST /backend-api/codex/responses`, including
  requests with query parameters and responses without an explicit streaming hint.
- Set it to `false` to request identity for new requests. In-flight requests, retries and
  pre-output SSE replays retain their original policy; changing the setting does not rebuild
  the shared HTTP clients. A gzip reply is still decoded when the setting is off.
- Both success and HTTP error bodies are decoded before the existing quota, overload,
  authentication and stream classifiers. Transformed bodies no longer carry the compressed
  content length, encoding or representation-specific integrity headers.
- Unsupported, stacked or duplicate response encodings are rejected as
  `unsupported_response_encoding`. Corrupt or truncated gzip is reported as
  `response_decode_error`, separately from ordinary network read failures.
- Decoding is incremental, uses bounded output chunks and follows downstream backpressure
  and cancellation. Existing error-body, SSE bootstrap and observation limits apply to
  decoded bytes. There is no new total response-size or cumulative expansion-ratio limit.

**Compatibility boundaries**

- Request-body zstd compression and already-encoded upload passthrough retain their existing
  behavior and the separate `compress_requests` switch. Compact, models, Images, passthrough,
  OAuth and WebSocket traffic retain their existing encoding behavior.
- The existing completed-event contract is preserved: a terminal event already delivered to
  the client is not followed by another failure event for a late read or gzip-trailer error.
  Business completion does not prove that the entire gzip trailer was read and verified.
- The implementation uses the existing pinned gzip backend; reqwest's automatic response
  compression features remain disabled. The Codex 0.166.0 transport compatibility baseline
  is retained.
- Synthetic local upstreams are the regression boundary. Real model-upstream gzip support,
  flush timing, bandwidth savings and latency are not established by those mocks.

**Upgrade**

- No configuration or database migration is introduced relative to v0.2.5; usage schema
  remains v13. Existing configuration files adopt `gzip_responses = true` by default.
- Use the hot `gzip_responses = false` setting to stop requesting gzip without restarting.
- Update a pinned image tag before pulling and recreating; see the
  [deployment guide](https://github.com/Seia-Yurizono/ciallopa/blob/main/docs/DEPLOYMENT.md#image-tags-and-upgrades).

**Validation**

- Local workspace tests: 505 passed, including incremental gzip output, error classification,
  terminal/trailer ordering, cancellation, hot reload and decoded error-body limits.
- Strict Clippy, formatting, UI/settings/accounts state checks and the loadbench example
  regressions passed. Release package and container smoke checks run in the tag workflow.
- The dependency lock comparison has no drift and the enabled third-party package/feature
  graph matches v0.2.5. No transport builder or reqwest compression feature changed. A new
  TLS/h2/WS wire capture was not performed; the external fingerprint-survey tools are not
  available in this workspace. Real model calls were not used for validation.
