use crate::headers::upstream_headers;
use crate::usage::Tracker;
use crate::{proxy, AppState};
use axum::response::Response;
use ciallopa_core::{AccountId, Outcome, RouteError, RouteRequest};
use ciallopa_routing::parse_quota;
use ciallopa_upstream::retry::{delay_for, is_retryable_status, switch_jitter};
use ciallopa_upstream::ws::handshake_status;
use ciallopa_upstream::UpstreamWs;
use http::{HeaderMap, StatusCode};
use std::time::Duration;

/// Handshake retries for transport errors / 408 / 5xx (Codex `request_max_retries` default).
const WS_HANDSHAKE_RETRIES: u64 = 4;

pub(super) async fn connect(
    st: &AppState,
    path: &str,
    headers: &HeaderMap,
    route: &mut RouteRequest,
    hold_cap: Duration,
    mut tracker: Option<&mut Tracker>,
) -> Result<(UpstreamWs, HeaderMap, AccountId, u64), Response> {
    // This gateway's independent handshake retry budget can switch usable accounts.
    // Codex 0.163/0.164 also has outer stream recovery and HTTP fallback; this is
    // not the only retry layer, nor a bound on the client's entire turn.
    let retries = WS_HANDSHAKE_RETRIES;
    let mut transient = 0u64;
    let mut overload_waited = false;
    let mut last: Option<Response> = None;
    let mut held = Duration::ZERO;
    loop {
        let accounts = st.accounts.accounts();
        let id = match st.router.select(route, &accounts) {
            Ok(s) => s.account,
            Err(RouteError::AllCoolingDown {
                retry_after,
                quota,
                earliest_quota,
            }) => {
                let c = proxy::Cooling::decide(st, retry_after, earliest_quota, held, hold_cap);
                if let proxy::Cooling::Hold(d) = c {
                    proxy::hold(d, &mut held).await;
                    continue;
                }
                return Err(last.unwrap_or_else(|| c.respond(st, retry_after, quota)));
            }
            Err(_) => {
                let code = if accounts.is_empty() {
                    "no_account"
                } else {
                    "no_usable_account"
                };
                crate::errors::warn_unroutable(code, accounts.len());
                return Err(last.unwrap_or_else(|| {
                    crate::errors::local(
                        StatusCode::SERVICE_UNAVAILABLE,
                        crate::errors::SRC_LOCAL,
                        code,
                        // No Retry-After: nothing will change by itself, so the client gives up after its
                        // short request retries instead of waiting out server advice on every reconnect.
                        None,
                    )
                }));
            }
        };
        if let Some(t) = tracker.as_deref_mut() {
            t.accounts(&route.exclude, Some(&id));
        }
        let mut creds = match st.accounts.credentials(&id).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(account = %ciallopa_core::a(&id), error = %e, "credentials unavailable");
                // IO / transient refresh failures are not a revoked token: skip without cooldown.
                if proxy::cools_on_credentials_error(&e) {
                    st.router.report(route, &id, &Outcome::AuthFailed);
                }
                route.exclude.push(id);
                continue;
            }
        };
        let mut refreshed = false;
        loop {
            route.attempt_at = Some(std::time::SystemTime::now());
            // Before the dial reads the egress options (see `egress_gen`).
            let gen = st.upstream.egress_gen();
            match st
                .upstream
                .ws_connect_as(
                    &id,
                    path,
                    upstream_headers(
                        &crate::turn_state::scope(headers, &id, !route.exclude.is_empty()),
                        &creds,
                    ),
                )
                .await
            {
                Ok((ws, resp)) => {
                    st.egress.ok();
                    crate::turn_state::learn(resp.headers(), &id);
                    let quota = parse_quota(resp.headers());
                    st.router.report(route, &id, &Outcome::Success { quota });
                    return Ok((ws, resp.headers().clone(), id, gen));
                }
                Err(e) => {
                    let Some((status, h, body)) = handshake_status(&e) else {
                        let proxy_code = match &e {
                            ciallopa_upstream::UpstreamError::Proxy(_) => Some("proxy_connect"),
                            ciallopa_upstream::UpstreamError::ProxyTimeout => Some("proxy_timeout"),
                            _ => None,
                        };
                        let cause = crate::errors::chain(&e);
                        st.router.report(route, &id, &Outcome::Transport);
                        if let Some(code) = proxy_code {
                            // Shared by every account: no switching, no retry here.
                            tracing::warn!(account = %ciallopa_core::a(&id), error = %cause, code, "egress proxy error (ws)");
                            return Err(crate::errors::network(code));
                        }
                        tracing::warn!(account = %ciallopa_core::a(&id), error = %cause, "upstream ws connect failed");
                        if transient >= retries {
                            return Err(crate::errors::network(crate::errors::ws_connect_code(&e)));
                        }
                        transient += 1;
                        if proxy::others_usable(st, &accounts, route, &id) {
                            route.exclude.push(id.clone());
                            tokio::time::sleep(switch_jitter()).await;
                        } else {
                            tokio::time::sleep(delay_for(transient, None)).await;
                        }
                        break;
                    };
                    let resp = {
                        let mut r = Response::new(axum::body::Body::from(body.clone()));
                        *r.status_mut() =
                            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
                        *r.headers_mut() = crate::headers::downstream_headers(&h);
                        r.headers_mut().remove(http::header::CONTENT_LENGTH);
                        r.extensions_mut()
                            .insert(crate::errors::meta_upstream(status, &h, &body));
                        r
                    };
                    st.router.note_attempt(&id);
                    if let Some(code) = proxy::overload_body(status, &body) {
                        // Account-level throttling: try every other usable account once, no
                        // same-account retry; the last one hands the error to the client.
                        let others = proxy::others_usable(st, &accounts, route, &id);
                        st.router
                            .report(route, &id, &proxy::overload_outcome(st, others));
                        tracing::info!(account = %ciallopa_core::a(&id), code = %code, "ws handshake overloaded");
                        let mut resp = resp;
                        if let Some(m) = resp.extensions_mut().get_mut::<crate::errors::ErrMeta>() {
                            m.code = code;
                        }
                        if !others {
                            if let Some(w) = proxy::overload_retry_wait(st, overload_waited, &h) {
                                overload_waited = true;
                                tracing::info!(account = %ciallopa_core::a(&id), wait_ms = w.as_millis() as u64, "ws handshake overloaded on last usable account; waiting to retry once");
                                tokio::time::sleep(w).await;
                                continue;
                            }
                            return Err(resp);
                        }
                        route.exclude.push(id.clone());
                        last = Some(resp);
                        tokio::time::sleep(switch_jitter()).await;
                        break;
                    }
                    match status {
                        401 if !refreshed => {
                            refreshed = true;
                            if let Ok(c) = st.accounts.force_refresh(&id).await {
                                creds = c;
                                continue;
                            }
                            st.router.report(route, &id, &Outcome::AuthFailed);
                            route.exclude.push(id.clone());
                            last = Some(resp);
                            break;
                        }
                        401..=404 | 429 => {
                            if let Some(blk) = crate::egress::detect(status, &h, &body) {
                                st.router.report(route, &id, &Outcome::EgressBlocked);
                                st.egress.note(&blk, status, &h, &id);
                                return Err(crate::egress::response(&blk, status, &h));
                            }
                            let outcome = proxy::classify_error(
                                status,
                                &h,
                                &body,
                                &st.config.load().upstream_opts,
                            );
                            proxy::log_account_error(&id, &outcome);
                            st.router.report(route, &id, &outcome);
                            route.exclude.push(id.clone());
                            last = Some(resp);
                            break;
                        }
                        s if is_retryable_status(s) => {
                            let others = proxy::others_usable(st, &accounts, route, &id);
                            let secs = st.config.load().upstream_opts.transient_cooldown_secs;
                            let ra = h
                                .get(http::header::RETRY_AFTER)
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| {
                                    ciallopa_core::retry_after::parse(
                                        v,
                                        std::time::SystemTime::now(),
                                        ciallopa_core::retry_after::Format::Http,
                                    )
                                    .map(|d| d.min(ciallopa_routing::quota::MAX_HEADER_RETRY_AFTER))
                                });
                            let cooldown = (others && secs > 0)
                                .then(|| ra.unwrap_or(Duration::from_secs(secs)));
                            st.router.report(
                                route,
                                &id,
                                &Outcome::ServerError {
                                    status: s,
                                    cooldown,
                                },
                            );
                            // NO_MORE_RETRY forbids another try on this account only.
                            if transient >= retries || (!others && proxy::no_more_retry(&h)) {
                                return Err(resp);
                            }
                            transient += 1;
                            if others {
                                route.exclude.push(id.clone());
                                tokio::time::sleep(switch_jitter()).await;
                            } else {
                                tokio::time::sleep(delay_for(transient, Some(&h))).await;
                            }
                            break;
                        }
                        s => {
                            st.router
                                .report(route, &id, &Outcome::ClientError { status: s });
                            return Err(resp);
                        }
                    }
                }
            }
        }
    }
}
