use super::policy::{classify, decide, Ctx, Final, RetryWait, Seen, Verdict};
use super::*;
use crate::classify::log_account_error;
use crate::timing::{scale, switch_jitter};
use ciallopa_core::Credentials;
use std::future::Future;
use std::time::SystemTime;

pub(crate) enum Dialed<C> {
    Connected {
        conn: C,
        status: u16,
    },
    Refused {
        status: u16,
        headers: HeaderMap,
        body: Bytes,
    },
    BodyIncomplete {
        status: u16,
        headers: HeaderMap,
        failure: crate::limits::BodyFailure,
    },
    UnsupportedEncoding {
        status: u16,
        request_id: Option<String>,
        message: String,
    },
    Transport {
        code: &'static str,
        cause: String,
    },
}

#[derive(Debug, PartialEq)]
pub(super) enum Refusal {
    Overloaded { code: String },
    Unauthorized,
    Account(Outcome),
    EgressBlocked(crate::egress::Block),
    Server { retry_after: Option<Duration> },
    Client,
}

pub(crate) trait Transport {
    type Conn;
    fn dial(
        &mut self,
        id: &AccountId,
        creds: &Credentials,
        route: &RouteRequest,
        phase: &mut Phase,
        policy: &Policy,
    ) -> impl Future<Output = Dialed<Self::Conn>> + Send;
    fn headers(conn: &Self::Conn) -> &HeaderMap;
}

pub(crate) enum Accepted<C> {
    Connected(C),
    Passthrough {
        status: u16,
        headers: HeaderMap,
        body: Bytes,
    },
}

/// The account/headers of an interrupted operation, without independent optional fields.
#[derive(Default)]
pub(crate) enum Phase {
    #[default]
    Selecting,
    Holding,
    Credentials {
        account: AccountId,
    },
    Dialing {
        account: AccountId,
    },
    ReadingError {
        account: AccountId,
        status: u16,
        request_id: Option<String>,
    },
}

impl Phase {
    pub(super) fn timeout(&self, attempts: &mut errors::Attempts, response: &mut Response) {
        let (account, head) = match self {
            Self::Credentials { account } | Self::Dialing { account } => (account, None),
            Self::ReadingError {
                account,
                status,
                request_id,
            } => (account, Some((*status, request_id))),
            Self::Selecting | Self::Holding => return,
        };
        let mut a = errors::attempt(account, "preparation");
        a.code = Some("preparation_timeout".into());
        a.action = Next::Stop.into();
        if let Some((status, request_id)) = head {
            a.status = Some(status);
            if let Some(meta) = response.extensions_mut().get_mut::<errors::ErrMeta>() {
                meta.upstream_status = Some(status);
                meta.upstream_request_id.clone_from(request_id);
                meta.message = Some(format!("HTTP preparation budget exhausted after upstream HTTP {status}; no complete response forwarded"));
            }
        }
        attempts.push(a);
    }
}

pub(crate) struct Loop<'a, T> {
    pub st: &'a AppState,
    pub transport: T,
    pub policy: Policy,
    pub route: &'a mut RouteRequest,
    pub info: &'a mut SendInfo,
    pub tracker: Option<&'a mut Tracker>,
}

impl<T: Transport> Loop<'_, T> {
    pub async fn run(mut self) -> Result<(Accepted<T::Conn>, AccountId), Response> {
        let mut best = Best::default();
        let mut last_net = None;
        'select: loop {
            self.info.leave_account();
            let accounts = self.st.accounts.accounts();
            let id = match self.st.router.select(self.route, &accounts) {
                Ok(s) => s.account,
                Err(RouteError::AllCoolingDown {
                    retry_after,
                    quota,
                    earliest_quota,
                }) => {
                    let cooling = Cooling::decide(
                        self.st,
                        retry_after,
                        earliest_quota,
                        self.info.held,
                        self.policy.hold_cap,
                    );
                    if let Cooling::Hold(wait) = cooling {
                        self.info.leave_account();
                        self.info.phase = Phase::Holding;
                        hold(wait, &mut self.info.held).await;
                        continue;
                    }
                    return Err(best.or(|| match (&cooling, last_net) {
                        (Cooling::Quota, _) | (_, None) => {
                            cooling.respond(self.st, retry_after, quota)
                        }
                        (_, Some(code)) => errors::network(code),
                    }));
                }
                Err(_) => {
                    let code = if accounts.is_empty() {
                        "no_account"
                    } else {
                        "no_usable_account"
                    };
                    errors::warn_unroutable(code, accounts.len());
                    return Err(best.or(|| match last_net {
                        Some(code) => errors::network(code),
                        None => errors::local(
                            StatusCode::SERVICE_UNAVAILABLE,
                            errors::SRC_LOCAL,
                            code,
                            None,
                        ),
                    }));
                }
            };
            self.info.phase = Phase::Credentials {
                account: id.clone(),
            };
            if let Some(t) = self.tracker.as_deref_mut() {
                t.accounts(&self.route.exclude, Some(&id));
            }
            let mut creds = match self.st.accounts.credentials(&id).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(account = %ciallopa_core::a(&id), error = %e, "credentials unavailable");
                    let revoked = cools_on_credentials_error(&e);
                    self.record(&id, "refresh", |a| {
                        a.code = Some(
                            if revoked {
                                "refresh_revoked"
                            } else {
                                "refresh_transient"
                            }
                            .into(),
                        );
                        a.action = Next::Switch.into();
                    });
                    if revoked {
                        self.st.router.report(self.route, &id, &Outcome::AuthFailed);
                    }
                    self.route.exclude.push(id);
                    continue;
                }
            };
            let mut refreshed = false;
            loop {
                self.route.attempt_at = Some(SystemTime::now());
                self.info.phase = Phase::Dialing {
                    account: id.clone(),
                };
                let dialed = self
                    .transport
                    .dial(&id, &creds, self.route, &mut self.info.phase, &self.policy)
                    .await;
                let seen = classify(&dialed, &self.st.config.load().upstream_opts);
                let others = matches!(
                    seen,
                    Seen::Transport(_)
                        | Seen::Refused(Refusal::Overloaded { .. } | Refusal::Server { .. })
                ) && others_usable(self.st, &accounts, self.route, &id);
                let mut ctx = Ctx {
                    others,
                    transient: self.info.retries,
                    refreshed,
                    refresh_failed: false,
                    overload: OverloadPlan::PassThrough,
                    no_more_retry: false,
                };
                if let Dialed::Refused { headers, .. } = &dialed {
                    ctx.no_more_retry = matches!(seen, Seen::Refused(Refusal::Server { .. }))
                        && no_more_retry(headers);
                    if matches!(seen, Seen::Refused(Refusal::Overloaded { .. })) {
                        // Handshakes have no frame replay counter; exclusion bounds their switches.
                        ctx.overload = self.policy.overload_plan(
                            others,
                            self.info.overload_waited,
                            headers,
                            self.st.config.load().upstream_opts.overload_retry_wait_secs,
                            0,
                            usize::MAX,
                        );
                    }
                }
                let mut verdict = decide(&self.policy, &seen, &ctx);
                if verdict == Verdict::RefreshRetry {
                    refreshed = true;
                    match self.st.accounts.force_refresh(&id).await {
                        Ok(c) => {
                            self.record(&id, "http", |a| {
                                a.status = Some(401);
                                a.action = Next::RefreshRetry.into();
                            });
                            creds = c;
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(account = %ciallopa_core::a(&id), error = %e, "force refresh failed")
                        }
                    }
                    ctx.refresh_failed = true;
                    verdict = decide(&self.policy, &seen, &ctx);
                }
                let backoff = if verdict
                    == (Verdict::RetrySame {
                        wait: RetryWait::Backoff,
                    }) {
                    let headers = match &dialed {
                        Dialed::Refused { headers, .. } => Some(headers),
                        _ => None,
                    };
                    scale(
                        self.st,
                        ciallopa_upstream::retry::delay_for(
                            u64::from(self.info.retries) + 1,
                            headers,
                        ),
                    )
                } else {
                    Duration::ZERO
                };
                let failed = match dialed {
                    Dialed::Connected { conn, status } => {
                        self.st.egress.ok();
                        let headers = T::headers(&conn);
                        crate::turn_state::learn(headers, &id);
                        self.st.router.report(
                            self.route,
                            &id,
                            &Outcome::Success {
                                quota: ciallopa_routing::parse_quota(headers),
                            },
                        );
                        self.record(&id, "http", |a| {
                            a.status = Some(status);
                            a.action = Next::Ok.into();
                        });
                        return Ok((Accepted::Connected(conn), id));
                    }
                    Dialed::Transport { code, cause } => {
                        tracing::warn!(account = %ciallopa_core::a(&id), error = %cause, code, "upstream transport error");
                        self.st.router.report(self.route, &id, &Outcome::Transport);
                        if self.policy.final_error == FinalError::Ranked {
                            last_net = Some(code);
                        }
                        self.record(&id, "transport", |a| {
                            a.code = Some(code.into());
                            a.body = Some(cause.chars().take(300).collect());
                            a.action = verdict.next(&seen).into();
                        });
                        Failed::Network(code)
                    }
                    Dialed::BodyIncomplete {
                        status,
                        headers,
                        failure,
                    } => {
                        self.record(&id, "http", |a| {
                            a.status = Some(status);
                            a.code = Some(failure.code(false).into());
                            a.body = errors::body_snippet(&failure.prefix);
                            a.action = Next::Stop.into();
                        });
                        return Err(failure.response(status, &headers, false));
                    }
                    Dialed::UnsupportedEncoding {
                        status,
                        request_id,
                        message,
                    } => {
                        self.record(&id, "http", |a| {
                            a.status = Some(status);
                            a.code = Some("unsupported_response_encoding".into());
                            a.action = Next::Stop.into();
                        });
                        let mut response = errors::local(
                            StatusCode::BAD_GATEWAY,
                            errors::SRC_UPSTREAM,
                            "unsupported_response_encoding",
                            None,
                        );
                        if let Some(meta) = response.extensions_mut().get_mut::<errors::ErrMeta>() {
                            meta.upstream_status = Some(status);
                            meta.upstream_request_id = request_id;
                            meta.message = Some(message);
                        }
                        return Err(response);
                    }
                    Dialed::Refused {
                        status,
                        headers,
                        body,
                    } => {
                        let cooled = self.report(&id, status, &headers, &seen, others);
                        self.record(&id, "http", |a| {
                            a.status = Some(status);
                            a.action = Action {
                                cooled,
                                next: verdict.next(&seen),
                            };
                            if !matches!(seen, Seen::Refused(Refusal::Client)) {
                                a.code = Some(match &seen {
                                    Seen::Refused(Refusal::Overloaded { code }) => code.clone(),
                                    Seen::Refused(Refusal::EgressBlocked(block)) => {
                                        block.code.into()
                                    }
                                    _ => errors::upstream_code(status, &headers, &body),
                                });
                                a.body = errors::body_snippet(&body);
                            }
                        });
                        if let Seen::Refused(Refusal::EgressBlocked(block)) = &seen {
                            return Err(crate::egress::response(block, status, &headers));
                        }
                        if verdict == Verdict::Accept {
                            return Ok((
                                Accepted::Passthrough {
                                    status,
                                    headers,
                                    body,
                                },
                                id,
                            ));
                        }
                        let mut candidate = Candidate::new(status, &headers, body);
                        if let Seen::Refused(Refusal::Overloaded { code }) = &seen {
                            candidate.rank = 4;
                            if let Some(meta) = candidate
                                .response
                                .extensions_mut()
                                .get_mut::<errors::ErrMeta>()
                            {
                                meta.code.clone_from(code);
                            }
                        }
                        Failed::Response(candidate)
                    }
                };
                match verdict {
                    Verdict::Stop(final_error) => {
                        return Err(failed.respond(final_error, best, self.policy.final_error))
                    }
                    Verdict::Switch { jitter, transient } => {
                        // WS keeps the last account-level refusal; a 408/5xx switch is not one.
                        let server_error =
                            !matches!(seen, Seen::Refused(Refusal::Overloaded { .. }))
                                && matches!(&failed, Failed::Response(c)
                                if matches!(c.response.status().as_u16(), 408 | 500..=599));
                        if !(server_error && self.policy.final_error == FinalError::Latest) {
                            failed.remember(&mut best, self.policy.final_error);
                        }
                        if transient {
                            self.info.retries += 1;
                        }
                        self.route.exclude.push(id.clone());
                        if jitter {
                            tokio::time::sleep(switch_jitter(self.st)).await;
                        }
                        continue 'select;
                    }
                    Verdict::RetrySame {
                        wait: RetryWait::Backoff,
                    } => {
                        self.info.retries += 1;
                        tokio::time::sleep(backoff).await;
                        continue 'select;
                    }
                    Verdict::RetrySame {
                        wait: RetryWait::Overload(wait),
                    } => {
                        // WS keeps only responses that switched accounts as its last error.
                        if self.policy.final_error == FinalError::Ranked {
                            failed.remember(&mut best, self.policy.final_error);
                        }
                        self.info.overload_waited = true;
                        tokio::time::sleep(crate::timing::scale(self.st, wait)).await;
                    }
                    Verdict::Accept | Verdict::RefreshRetry => {
                        unreachable!("accepted/refreshed above")
                    }
                }
            }
        }
    }

    fn record(
        &mut self,
        id: &AccountId,
        kind: &'static str,
        update: impl FnOnce(&mut errors::Attempt),
    ) {
        if self.policy.record_attempts {
            let mut attempt = errors::attempt(id, kind);
            update(&mut attempt);
            self.info.attempts.push(attempt);
        }
    }

    fn report(
        &self,
        id: &AccountId,
        status: u16,
        headers: &HeaderMap,
        seen: &Seen,
        others: bool,
    ) -> Option<Cooled> {
        let outcome = match seen {
            Seen::Refused(Refusal::Overloaded { .. }) => overload_outcome(self.st, others),
            Seen::Refused(Refusal::Unauthorized) => Outcome::AuthFailed,
            Seen::Refused(Refusal::Account(outcome)) => {
                log_account_error(id, outcome);
                self.st.router.report(self.route, id, outcome);
                return None;
            }
            Seen::Refused(Refusal::EgressBlocked(block)) => {
                self.st
                    .router
                    .report(self.route, id, &Outcome::EgressBlocked);
                self.st.egress.note(block, status, headers, id);
                return None;
            }
            Seen::Refused(Refusal::Server { retry_after }) => {
                let base = self.policy.server_cooldown.unwrap_or_else(|| {
                    Duration::from_secs(self.st.config.load().upstream_opts.transient_cooldown_secs)
                });
                let cooldown = (others && !base.is_zero()).then(|| retry_after.unwrap_or(base));
                Outcome::ServerError { status, cooldown }
            }
            Seen::Refused(Refusal::Client) => Outcome::ClientError { status },
            _ => unreachable!("only refusals are reported here"),
        };
        if !self.policy.record_attempts
            || !matches!(
                seen,
                Seen::Refused(Refusal::Overloaded { .. } | Refusal::Server { .. })
            )
        {
            self.st.router.report(self.route, id, &outcome);
            return None;
        }
        let cooled = report_cooling(self.st, self.route, id, &outcome);
        match seen {
            Seen::Refused(Refusal::Server { .. }) => cooled.map(|_| {
                let secs = self
                    .st
                    .router
                    .cooling_until(id)
                    .and_then(|t| t.duration_since(SystemTime::now()).ok())
                    .map(|d| d.as_secs() + 1)
                    .unwrap_or(0);
                Cooled::Secs(secs)
            }),
            Seen::Refused(Refusal::Overloaded { .. }) => cooled,
            _ => None,
        }
    }
}

struct Candidate {
    rank: u8,
    response: Response,
}

impl Candidate {
    fn new(status: u16, headers: &HeaderMap, body: Bytes) -> Self {
        let rank = match status {
            429 if ciallopa_routing::is_usage_limit(&body) => 6,
            429 => 5,
            408 | 500..=599 => 4,
            401 | 402 => 3,
            403 => 2,
            _ => 1,
        };
        let meta = errors::meta_upstream(status, headers, &body);
        let mut response = buffered(status, headers, body);
        response.extensions_mut().insert(meta);
        Self { rank, response }
    }
}

#[derive(Default)]
struct Best(Option<Candidate>);

impl Best {
    fn offer(&mut self, candidate: Candidate, preference: FinalError) {
        if preference == FinalError::Latest
            || self.0.as_ref().is_none_or(|old| old.rank <= candidate.rank)
        {
            self.0 = Some(candidate);
        }
    }

    fn finish(self, candidate: Candidate, preference: FinalError) -> Response {
        match self.0 {
            Some(old) if preference == FinalError::Ranked && old.rank > candidate.rank => {
                old.response
            }
            _ => candidate.response,
        }
    }

    fn or(self, fallback: impl FnOnce() -> Response) -> Response {
        self.0.map(|c| c.response).unwrap_or_else(fallback)
    }
}

// Keeps a real final response available: no synthetic "unreachable" 502 fallback.
enum Failed {
    Response(Candidate),
    Network(&'static str),
}

impl Failed {
    fn remember(self, best: &mut Best, preference: FinalError) {
        if let Self::Response(candidate) = self {
            best.offer(candidate, preference);
        }
    }

    fn respond(self, final_error: Final, best: Best, preference: FinalError) -> Response {
        match self {
            Self::Response(candidate) if final_error == Final::Best => {
                best.finish(candidate, preference)
            }
            Self::Response(candidate) => candidate.response,
            Self::Network(code) if preference == FinalError::Ranked => {
                best.or(|| errors::network(code))
            }
            Self::Network(code) => errors::network(code),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(status: u16, code: &str) -> Candidate {
        Candidate::new(
            status,
            &HeaderMap::new(),
            Bytes::from(format!(r#"{{"error":{{"type":"{code}"}}}}"#)),
        )
    }

    #[test]
    fn final_error_preference_ranks_http_but_keeps_latest_ws_response() {
        for (first, first_code, second, expected_ranked) in [
            (429, "usage_limit_reached", 429, "usage_limit_reached"),
            (429, "rate_limit_exceeded", 503, "rate_limit_exceeded"),
            (503, "server_error", 401, "server_error"),
            (401, "unauthorized", 403, "unauthorized"),
            (402, "payment", 403, "payment"),
            (403, "forbidden", 404, "forbidden"),
            (401, "unauthorized", 402, "later"),
            (404, "not_found", 503, "later"),
            (408, "timeout", 503, "later"),
        ] {
            for preference in [FinalError::Ranked, FinalError::Latest] {
                let mut best = Best::default();
                best.offer(candidate(first, first_code), preference);
                let response = best.finish(candidate(second, "later"), preference);
                let meta = response
                    .extensions()
                    .get::<errors::ErrMeta>()
                    .expect("provenance");
                assert_eq!(
                    meta.code,
                    if preference == FinalError::Ranked {
                        expected_ranked
                    } else {
                        "later"
                    }
                );
            }
        }
        for preference in [FinalError::Ranked, FinalError::Latest] {
            let mut best = Best::default();
            best.offer(candidate(429, "usage_limit_reached"), preference);
            let response =
                Failed::Network("transport").respond(Final::Network("transport"), best, preference);
            assert_eq!(
                response.status(),
                if preference == FinalError::Ranked {
                    StatusCode::TOO_MANY_REQUESTS
                } else {
                    StatusCode::BAD_GATEWAY
                }
            );
        }
    }

    #[test]
    fn timeout_phase_preserves_only_observed_headers() {
        for (phase, account, status, request_id) in [
            (Phase::Selecting, None, None, None),
            (Phase::Holding, None, None, None),
            (
                Phase::Credentials {
                    account: "a".into(),
                },
                Some("a"),
                None,
                None,
            ),
            (
                Phase::Dialing {
                    account: "b".into(),
                },
                Some("b"),
                None,
                None,
            ),
            (
                Phase::ReadingError {
                    account: "c".into(),
                    status: 503,
                    request_id: Some("req-1".into()),
                },
                Some("c"),
                Some(503),
                Some("req-1"),
            ),
        ] {
            let mut attempts = errors::Attempts::default();
            let mut response = errors::local(
                StatusCode::GATEWAY_TIMEOUT,
                errors::SRC_NETWORK,
                "preparation_timeout",
                None,
            );
            phase.timeout(&mut attempts, &mut response);
            assert_eq!(attempts.list.last().map(|a| a.account.as_str()), account);
            if let Some(attempt) = attempts.list.last() {
                assert_eq!(attempt.status, status);
                assert_eq!(attempt.code.as_deref(), Some("preparation_timeout"));
                assert_eq!(attempt.action, Next::Stop.into());
            }
            let meta = response
                .extensions()
                .get::<errors::ErrMeta>()
                .expect("provenance");
            assert_eq!(meta.upstream_status, status);
            assert_eq!(meta.upstream_request_id.as_deref(), request_id);
        }
    }
}
