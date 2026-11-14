mod engine;
mod policy;
mod transport;

use engine::Refusal;
pub(crate) use engine::{Accepted, Dialed, Loop, Phase, Transport};
use policy::FinalError;
pub(crate) use policy::{quota_plan, Action, Cooled, Next, OverloadPlan, Policy};
use transport::HttpTransport;

use crate::errors;
use crate::headers::downstream_headers;
use crate::usage::Tracker;
use crate::AppState;
use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use ciallopa_core::{AccountId, AuthError, Outcome, RouteError, RouteRequest};
use http::{HeaderMap, Method, StatusCode};
use std::time::Duration;

/// An outbound request, independent of the account it ends up on.
pub(crate) struct Outbound {
    pub method: Method,
    /// Relative to `upstream.base_url`, with query.
    pub path: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub route: RouteRequest,
    /// The client asked for an SSE stream (`/responses` with `stream:true` or
    /// `Accept: text/event-stream`). chatgpt.com sends SSE without any `Content-Type`, so this
    /// hint decides SSE handling when the upstream omits it.
    pub expect_sse: bool,
    /// Direct Responses HTTP requests return identity downstream, even if gzip negotiation
    /// is disabled. Other routes retain their existing passthrough policy.
    pub decode_response: bool,
}

/// The upstream response chosen for the client (2xx, or a non-retryable / final error).
pub(crate) struct Chosen {
    pub resp: reqwest::Response,
    pub account: AccountId,
    pub route: RouteRequest,
    /// Captured before upstream work; keep the same idle limit through body completion.
    pub idle: Duration,
}

pub(crate) fn buffered(status: u16, headers: &HeaderMap, body: Bytes) -> Response {
    let mut r = Response::new(Body::from(body));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    *r.headers_mut() = downstream_headers(headers);
    r.headers_mut().remove(http::header::CONTENT_LENGTH);
    r
}

/// What `send_info` observed (for the usage log).
#[derive(Default)]
pub(crate) struct SendInfo {
    pub retries: u32,
    /// Accounts tried and abandoned (failover), in order.
    pub tried: Vec<AccountId>,
    /// Every upstream attempt with ciallopa's reaction (bounded).
    pub attempts: crate::errors::Attempts,
    /// The single same-account overload wait-and-retry of this client request is spent.
    pub overload_waited: bool,
    /// Time spent holding the request while every account was cooling down.
    pub held: Duration,
    phase: Phase,
}

/// Charged only while preparing an HTTP attempt or waiting to retry. Successful SSE
/// bootstrap/streaming is deliberately outside this budget, even before its first output.
pub(crate) struct Preparation {
    budget: crate::budget::PreparationBudget,
    header_timeout: Duration,
    pub(crate) idle: Duration,
}

impl Preparation {
    pub(crate) fn new(st: &AppState) -> Self {
        let timing = crate::timing::Timing::capture(&st.config.load());
        Self {
            budget: crate::budget::PreparationBudget::new(timing.preparation),
            header_timeout: timing.header,
            idle: timing.idle,
        }
    }

    pub(crate) async fn run<T>(
        &mut self,
        future: impl std::future::Future<Output = T>,
    ) -> Result<T, Response> {
        self.budget.run(future).await
    }
}

/// What to do when every candidate account is cooling down.
pub(crate) enum Cooling {
    /// Hold the request this long, then select again.
    Hold(Duration),
    /// The first account to recover is out of usage quota: `usage_limit_reached` with its reset.
    Quota,
    /// Answer 503 with a short Retry-After (a WebSocket handshake that cannot be held that long).
    RetrySoon(Duration),
    /// The first recovery is beyond the wait: answer 503 without Retry-After.
    Long,
}

/// Longest hold of a WebSocket handshake: Codex abandons a handshake after 15 s.
pub(crate) const WS_HOLD_MAX: Duration = Duration::from_secs(10);

impl Cooling {
    /// `recovers_in` = until the first account recovers; `held` = already held for this
    /// request; `hold_cap` = longest single hold this transport allows.
    pub(crate) fn decide(
        st: &AppState,
        recovers_in: Duration,
        earliest_quota: bool,
        held: Duration,
        hold_cap: Duration,
    ) -> Self {
        let budget = crate::timing::Timing::capture(&st.config.load()).cooldown_wait;
        let remaining = budget.saturating_sub(held);
        if !budget.is_zero() && recovers_in <= remaining.min(hold_cap) {
            // A little past the recovery instant so the next selection finds the account free.
            return Self::Hold(
                (recovers_in + Duration::from_millis(50)).max(Duration::from_millis(100)),
            );
        }
        if earliest_quota {
            return Self::Quota;
        }
        if !budget.is_zero()
            && recovers_in <= remaining
            && recovers_in.as_secs() < errors::MAX_CLIENT_RETRY_AFTER
        {
            return Self::RetrySoon(Duration::from_secs(recovers_in.as_secs() + 1));
        }
        Self::Long
    }

    /// The client response for a non-`Hold` verdict (`quota` = every account is out of quota).
    pub(crate) fn respond(&self, st: &AppState, recovers_in: Duration, quota: bool) -> Response {
        let wait = st.config.load().routing.cooldown_wait_secs;
        match self {
            Self::Quota => errors::quota_exhausted(recovers_in, quota),
            Self::RetrySoon(d) => errors::all_cooling(recovers_in, wait, Some(*d)),
            Self::Hold(_) | Self::Long => errors::all_cooling(recovers_in, wait, None),
        }
    }
}

/// Hold for `d` while every account is cooling down, adding the time to `held`. Dropping the
/// future (client gone) ends the hold.
pub(crate) async fn hold(d: Duration, held: &mut Duration) {
    tracing::info!(
        wait_ms = d.as_millis() as u64,
        "every account is cooling down; holding the request until the first one recovers"
    );
    let t = std::time::Instant::now();
    tokio::time::sleep(d).await;
    *held += t.elapsed();
}

/// Run the selection / retry loop. `Err` is a ready-made downstream response.
pub(crate) async fn send(st: &AppState, ob: Outbound) -> Result<Chosen, Response> {
    send_info(
        st,
        &ob,
        &mut SendInfo::default(),
        &mut Preparation::new(st),
        None,
    )
    .await
}

/// One preparation sequence for Images calls, retaining the same diagnostics.
pub(crate) async fn send_tracked(
    st: &AppState,
    ob: Outbound,
    trk: &mut Option<Tracker>,
) -> Result<Chosen, Response> {
    let mut info = SendInfo::default();
    let result = send_info(st, &ob, &mut info, &mut Preparation::new(st), trk.as_mut()).await;
    if let Some(t) = trk.as_mut() {
        t.retries(info.retries);
        t.attempts(&info.attempts);
        let last = info.attempts.list.last().map(|a| &a.account);
        t.accounts(
            &info.tried,
            result.as_ref().ok().map(|c| &c.account).or(last),
        );
        if let Ok(c) = &result {
            t.quota(c.resp.headers());
            t.error_headers(c.resp.headers());
        } else if let Err(r) = &result {
            t.error_headers(r.headers());
        }
    }
    result
}

pub(crate) async fn send_info(
    st: &AppState,
    ob: &Outbound,
    info: &mut SendInfo,
    budget: &mut Preparation,
    trk: Option<&mut Tracker>,
) -> Result<Chosen, Response> {
    let mut route = ob.route.clone();
    info.phase = Phase::Selecting;
    let policy = Policy::http(&st.config.load().upstream_opts);
    let transport = HttpTransport {
        st,
        ob,
        header_timeout: budget.header_timeout,
    };
    let result = budget
        .run(
            Loop {
                st,
                transport,
                policy,
                route: &mut route,
                info,
                tracker: trk,
            }
            .run(),
        )
        .await;
    let result = match result {
        Ok(result) => result,
        Err(mut response) => {
            info.phase.timeout(&mut info.attempts, &mut response);
            Err(response)
        }
    };
    info.tried.clone_from(&route.exclude);
    result.map(|(accepted, account)| Chosen {
        resp: match accepted {
            Accepted::Connected(response) => response,
            Accepted::Passthrough {
                status,
                headers,
                body,
            } => rebuild(status, &headers, body),
        },
        account,
        route,
        idle: budget.idle,
    })
}

/// Another enabled account, outside the exclusion list and cooldown, can take the request.
pub(crate) fn others_usable(
    st: &AppState,
    accounts: &[ciallopa_core::Account],
    route: &RouteRequest,
    id: &AccountId,
) -> bool {
    accounts.iter().any(|a| {
        &a.id != id
            && !a.disabled
            && !route.exclude.contains(&a.id)
            && st.router.cooling_until(&a.id).is_none()
    })
}

/// Report an outcome and name a newly applied account cooldown.
pub(crate) fn report_cooling(
    st: &AppState,
    route: &RouteRequest,
    id: &AccountId,
    outcome: &Outcome,
) -> Option<Cooled> {
    let before = st.router.cooling_until(id).is_some();
    st.router.report(route, id, outcome);
    (!before && st.router.cooling_until(id).is_some()).then_some(Cooled::Plain)
}

/// An upstream response whose body was already read, rebuilt for the normal handling.
pub(crate) fn rebuild(status: u16, h: &HeaderMap, b: Bytes) -> reqwest::Response {
    let mut r = http::Response::new(b);
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    *r.headers_mut() = h.clone();
    reqwest::Response::from(r)
}

/// Whether a `credentials()` failure should cool the account down (shared by HTTP and WS).
/// Disk IO and transient refresh failures (network / auth 5xx) are not a bad token.
pub(crate) fn cools_on_credentials_error(e: &AuthError) -> bool {
    !matches!(e, AuthError::Io(_) | AuthError::Refresh(_))
}

/// `x-retry-metadata: NO_MORE_RETRY`: do not retry on the same account (switching is fine).
pub(crate) fn no_more_retry(h: &HeaderMap) -> bool {
    h.get_all("x-retry-metadata")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.to_ascii_uppercase().contains("NO_MORE_RETRY"))
}

/// The router outcome for one overloaded answer on `id` (cooling never hits the last usable
/// account).
pub(crate) fn overload_outcome(st: &AppState, others: bool) -> Outcome {
    let c = st.config.load();
    let o = &c.upstream_opts;
    Outcome::Overloaded {
        streak: o.overload_streak,
        base: Duration::from_secs(o.overload_cooldown_secs),
        max: Duration::from_secs(o.overload_cooldown_max_secs),
        cool: others,
    }
}

#[cfg(test)]
mod stream_lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn exhausted_preparation_budget_does_not_start_another_attempt() {
        let polled = std::cell::Cell::new(false);
        let mut budget = Preparation {
            budget: crate::budget::PreparationBudget::new(Duration::ZERO),
            header_timeout: Duration::from_secs(1),
            idle: Duration::from_secs(1),
        };
        let response = budget.run(async { polled.set(true) }).await.unwrap_err();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(
            !polled.get(),
            "no credential lookup, routing or send after budget exhaustion"
        );
    }
}

#[cfg(test)]
mod quota_tests {
    use super::*;

    #[test]
    fn credential_errors_that_cool() {
        assert!(!cools_on_credentials_error(&AuthError::Refresh(
            "timeout".into()
        )));
        assert!(!cools_on_credentials_error(&AuthError::Io(
            std::io::Error::other("x")
        )));
        assert!(cools_on_credentials_error(&AuthError::ReloginRequired));
    }
}
