use super::{Dialed, Refusal};
use crate::{egress, errors};
use ciallopa_core::config::UpstreamOpts;
use ciallopa_upstream::retry::is_retryable_status;
use http::HeaderMap;
use serde::{Serialize, Serializer};
use std::fmt;
use std::time::{Duration, SystemTime};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cooled {
    Plain,
    Secs(u64),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Next {
    #[default]
    Pending,
    Ok,
    Retry,
    RefreshRetry,
    WaitRetry,
    Switch,
    Stop,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Action {
    pub cooled: Option<Cooled>,
    pub next: Next,
}

impl From<Next> for Action {
    fn from(next: Next) -> Self {
        Self { cooled: None, next }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.cooled {
            Some(Cooled::Plain) => f.write_str("cooldown+")?,
            Some(Cooled::Secs(n)) => write!(f, "cooldown({n}s)+")?,
            None => {}
        }
        f.write_str(match self.next {
            Next::Pending => "",
            Next::Ok => "ok",
            Next::Retry => "retry",
            Next::RefreshRetry => "refresh+retry",
            Next::WaitRetry => "wait+retry",
            Next::Switch => "switch",
            Next::Stop => "stop",
        })
    }
}

impl Serialize for Action {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FinalError {
    Ranked,
    Latest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OverloadSwitches {
    AvailableAccounts,
    CapturedLimit,
}

/// Retained HTTP/WS differences, including D1/D2/D3/D5/D6 in forward_policy.rs.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Policy {
    pub transient_budget: u32,
    pub hold_cap: Duration,
    pub retry_same_transport: bool,
    pub retry_same_server: bool,
    pub final_error: FinalError,
    pub record_attempts: bool,
    pub pass_through_client_errors: bool,
    pub note_successful_attempt: bool,
    /// HTTP captures this once; WS reads the current setting at each refusal.
    pub server_cooldown: Option<Duration>,
    overload_switches: OverloadSwitches,
}

impl Policy {
    pub fn http(opts: &UpstreamOpts) -> Self {
        Self {
            transient_budget: opts.max_attempts,
            hold_cap: Duration::MAX,
            retry_same_transport: false,
            retry_same_server: false,
            final_error: FinalError::Ranked,
            record_attempts: true,
            pass_through_client_errors: true,
            note_successful_attempt: true,
            server_cooldown: Some(Duration::from_secs(opts.transient_cooldown_secs)),
            overload_switches: OverloadSwitches::AvailableAccounts,
        }
    }

    pub fn ws_handshake() -> Self {
        Self {
            // Codex's independent handshake retry budget, not HTTP max_attempts.
            transient_budget: 4,
            hold_cap: super::WS_HOLD_MAX,
            retry_same_transport: true,
            retry_same_server: true,
            final_error: FinalError::Latest,
            record_attempts: false,
            pass_through_client_errors: false,
            note_successful_attempt: false,
            server_cooldown: None,
            overload_switches: OverloadSwitches::CapturedLimit,
        }
    }

    pub fn ws_redial() -> Self {
        Self {
            hold_cap: Duration::MAX,
            ..Self::ws_handshake()
        }
    }

    pub fn overload_plan(
        &self,
        others: bool,
        waited: bool,
        headers: &HeaderMap,
        base_secs: u64,
        switches: usize,
        max_switches: usize,
    ) -> OverloadPlan {
        if others {
            return if self.overload_switches == OverloadSwitches::AvailableAccounts
                || switches + 1 < max_switches.max(2)
            {
                OverloadPlan::Switch
            } else {
                OverloadPlan::PassThrough
            };
        }
        if waited || super::no_more_retry(headers) {
            return OverloadPlan::PassThrough;
        }
        ciallopa_upstream::retry::overload_wait(base_secs, Some(headers))
            .map(OverloadPlan::WaitRetry)
            .unwrap_or(OverloadPlan::PassThrough)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OverloadPlan {
    Switch,
    WaitRetry(Duration),
    PassThrough,
}

impl OverloadPlan {
    pub fn next(self) -> Next {
        match self {
            Self::Switch => Next::Switch,
            Self::WaitRetry(_) => Next::WaitRetry,
            Self::PassThrough => Next::Stop,
        }
    }
}

pub(crate) fn quota_plan(switches: &mut usize, max_switches: usize) -> Next {
    *switches += 1;
    if *switches < max_switches {
        Next::Switch
    } else {
        Next::Stop
    }
}

#[derive(Debug, PartialEq)]
pub(super) enum Seen {
    Connected,
    Refused(Refusal),
    BodyIncomplete,
    UnsupportedEncoding,
    Transport(&'static str),
}

pub(super) fn classify<C>(dialed: &Dialed<C>, opts: &UpstreamOpts) -> Seen {
    let (status, headers, body) = match dialed {
        Dialed::Refused {
            status,
            headers,
            body,
        } => (status, headers, body),
        Dialed::Connected { .. } => return Seen::Connected,
        Dialed::Transport { code, .. } => return Seen::Transport(code),
        Dialed::BodyIncomplete { .. } => return Seen::BodyIncomplete,
        Dialed::UnsupportedEncoding { .. } => return Seen::UnsupportedEncoding,
    };
    let refusal = if let Some(code) = crate::classify::overload_body(*status, body) {
        Refusal::Overloaded { code }
    } else if *status == 401 {
        Refusal::Unauthorized
    } else if let Some(block) = egress::detect(*status, headers, body) {
        Refusal::EgressBlocked(block)
    } else if matches!(*status, 402..=404 | 429) {
        Refusal::Account(crate::classify::classify_error(
            *status, headers, body, opts,
        ))
    } else if is_retryable_status(*status) {
        let retry_after = headers
            .get(http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                ciallopa_core::retry_after::parse(
                    v,
                    SystemTime::now(),
                    ciallopa_core::retry_after::Format::Http,
                )
            })
            .map(|d| d.min(ciallopa_routing::quota::MAX_HEADER_RETRY_AFTER));
        Refusal::Server { retry_after }
    } else {
        Refusal::Client
    };
    Seen::Refused(refusal)
}

pub(super) struct Ctx {
    pub others: bool,
    pub transient: u32,
    pub refreshed: bool,
    pub refresh_failed: bool,
    pub overload: OverloadPlan,
    pub no_more_retry: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RetryWait {
    Overload(Duration),
    /// WS reselects after backoff, reacquiring credentials and its one refresh.
    Backoff,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Final {
    Best,
    This,
    Network(&'static str),
    Egress,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Verdict {
    Accept,
    RefreshRetry,
    RetrySame { wait: RetryWait },
    Switch { jitter: bool, transient: bool },
    Stop(Final),
}

impl Verdict {
    pub fn next(self, seen: &Seen) -> Next {
        match self {
            Self::Accept if matches!(seen, Seen::Connected) => Next::Ok,
            Self::Accept | Self::Stop(_) => Next::Stop,
            Self::RefreshRetry => Next::RefreshRetry,
            Self::RetrySame {
                wait: RetryWait::Overload(_),
            } => Next::WaitRetry,
            Self::RetrySame {
                wait: RetryWait::Backoff,
            } => Next::Retry,
            Self::Switch { .. } => Next::Switch,
        }
    }
}

pub(super) fn decide(policy: &Policy, seen: &Seen, ctx: &Ctx) -> Verdict {
    let response = if policy.final_error == FinalError::Ranked {
        Final::Best
    } else {
        Final::This
    };
    let transient = |same: bool, forbidden: bool, final_error| {
        if ctx.transient >= policy.transient_budget || forbidden || (!ctx.others && !same) {
            Verdict::Stop(final_error)
        } else if ctx.others {
            Verdict::Switch {
                jitter: true,
                transient: true,
            }
        } else {
            Verdict::RetrySame {
                wait: RetryWait::Backoff,
            }
        }
    };
    match seen {
        Seen::Connected => Verdict::Accept,
        Seen::BodyIncomplete | Seen::UnsupportedEncoding => Verdict::Stop(Final::This),
        Seen::Transport(code) => transient(
            policy.retry_same_transport,
            errors::is_proxy_code(code),
            Final::Network(code),
        ),
        Seen::Refused(Refusal::Unauthorized) if !ctx.refreshed && !ctx.refresh_failed => {
            Verdict::RefreshRetry
        }
        Seen::Refused(Refusal::Unauthorized | Refusal::Account(_)) => Verdict::Switch {
            jitter: false,
            transient: false,
        },
        Seen::Refused(Refusal::Overloaded { .. }) => match ctx.overload {
            OverloadPlan::Switch => Verdict::Switch {
                jitter: true,
                transient: false,
            },
            OverloadPlan::WaitRetry(wait) => Verdict::RetrySame {
                wait: RetryWait::Overload(wait),
            },
            OverloadPlan::PassThrough => Verdict::Stop(response),
        },
        Seen::Refused(Refusal::EgressBlocked(_)) => Verdict::Stop(Final::Egress),
        Seen::Refused(Refusal::Server { .. }) => transient(
            policy.retry_same_server,
            !ctx.others && ctx.no_more_retry,
            response,
        ),
        Seen::Refused(Refusal::Client) if policy.pass_through_client_errors => Verdict::Accept,
        Seen::Refused(Refusal::Client) => Verdict::Stop(Final::This),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ciallopa_core::Outcome;

    fn http() -> Policy {
        Policy::http(&UpstreamOpts {
            max_attempts: 2,
            ..Default::default()
        })
    }

    fn ctx() -> Ctx {
        Ctx {
            others: false,
            transient: 0,
            refreshed: false,
            refresh_failed: false,
            overload: OverloadPlan::PassThrough,
            no_more_retry: false,
        }
    }

    #[test]
    fn action_strings_and_json_are_unchanged() {
        for (cooled, next, expected) in [
            (None, Next::Pending, ""),
            (None, Next::Ok, "ok"),
            (None, Next::Retry, "retry"),
            (None, Next::Switch, "switch"),
            (None, Next::Stop, "stop"),
            (None, Next::WaitRetry, "wait+retry"),
            (None, Next::RefreshRetry, "refresh+retry"),
            (Some(Cooled::Plain), Next::Switch, "cooldown+switch"),
            (Some(Cooled::Plain), Next::Stop, "cooldown+stop"),
            (Some(Cooled::Plain), Next::WaitRetry, "cooldown+wait+retry"),
            (Some(Cooled::Secs(15)), Next::Switch, "cooldown(15s)+switch"),
            (Some(Cooled::Secs(15)), Next::Stop, "cooldown(15s)+stop"),
            (Some(Cooled::Secs(0)), Next::Stop, "cooldown(0s)+stop"),
        ] {
            let action = Action { cooled, next };
            assert_eq!(action.to_string(), expected);
            assert_eq!(serde_json::to_value(action).expect("action JSON"), expected);
            let mut attempt = errors::attempt("a", "http");
            attempt.action = action;
            assert_eq!(
                serde_json::to_value(attempt).expect("attempt JSON")["action"],
                expected
            );
        }
    }

    #[test]
    fn policies_pin_transport_differences() {
        let http = http();
        let ws = Policy::ws_handshake();
        assert_eq!((http.transient_budget, ws.transient_budget), (2, 4));
        assert_eq!(
            (http.hold_cap, ws.hold_cap, Policy::ws_redial().hold_cap),
            (Duration::MAX, Duration::from_secs(10), Duration::MAX)
        );
        assert_eq!(
            (http.final_error, ws.final_error),
            (FinalError::Ranked, FinalError::Latest)
        );
        assert_eq!((http.record_attempts, ws.record_attempts), (true, false));
        assert_eq!(
            (http.retry_same_transport, ws.retry_same_transport),
            (false, true)
        );
        assert_eq!(
            (http.retry_same_server, ws.retry_same_server),
            (false, true)
        );
        assert_eq!(
            (
                http.pass_through_client_errors,
                ws.pass_through_client_errors
            ),
            (true, false)
        );
        assert_eq!(
            (http.note_successful_attempt, ws.note_successful_attempt),
            (true, false)
        );
    }

    #[test]
    fn transient_decisions_keep_separate_budgets_and_same_account_rules() {
        let switch = Verdict::Switch {
            jitter: true,
            transient: true,
        };
        let backoff = Verdict::RetrySame {
            wait: RetryWait::Backoff,
        };
        for (others, transient, http_net, ws_net, http_server, ws_server) in [
            (
                false,
                0,
                Verdict::Stop(Final::Network("transport")),
                backoff,
                Verdict::Stop(Final::Best),
                backoff,
            ),
            (true, 0, switch, switch, switch, switch),
            (
                true,
                2,
                Verdict::Stop(Final::Network("transport")),
                switch,
                Verdict::Stop(Final::Best),
                switch,
            ),
            (
                true,
                4,
                Verdict::Stop(Final::Network("transport")),
                Verdict::Stop(Final::Network("transport")),
                Verdict::Stop(Final::Best),
                Verdict::Stop(Final::This),
            ),
            (
                false,
                4,
                Verdict::Stop(Final::Network("transport")),
                Verdict::Stop(Final::Network("transport")),
                Verdict::Stop(Final::Best),
                Verdict::Stop(Final::This),
            ),
        ] {
            let ctx = Ctx {
                others,
                transient,
                ..ctx()
            };
            let network = Seen::Transport("transport");
            let server = Seen::Refused(Refusal::Server { retry_after: None });
            assert_eq!(decide(&http(), &network, &ctx), http_net);
            assert_eq!(decide(&Policy::ws_handshake(), &network, &ctx), ws_net);
            assert_eq!(decide(&http(), &server, &ctx), http_server);
            assert_eq!(decide(&Policy::ws_handshake(), &server, &ctx), ws_server);
        }
        for others in [false, true] {
            for policy in [http(), Policy::ws_handshake()] {
                let ctx = Ctx {
                    others,
                    no_more_retry: true,
                    ..ctx()
                };
                assert_eq!(
                    decide(&policy, &Seen::Transport("proxy_connect"), &ctx),
                    Verdict::Stop(Final::Network("proxy_connect"))
                );
                assert_eq!(
                    decide(
                        &policy,
                        &Seen::Refused(Refusal::Server { retry_after: None }),
                        &ctx
                    ),
                    if others {
                        switch
                    } else if policy.final_error == FinalError::Ranked {
                        Verdict::Stop(Final::Best)
                    } else {
                        Verdict::Stop(Final::This)
                    }
                );
            }
        }
    }

    #[test]
    fn refusal_decision_table() {
        let switch = Verdict::Switch {
            jitter: false,
            transient: false,
        };
        for policy in [http(), Policy::ws_handshake()] {
            for (seen, expected) in [
                (Seen::Connected, Verdict::Accept),
                (Seen::BodyIncomplete, Verdict::Stop(Final::This)),
                (Seen::UnsupportedEncoding, Verdict::Stop(Final::This)),
                (Seen::Refused(Refusal::Unauthorized), Verdict::RefreshRetry),
                (Seen::Refused(Refusal::Account(Outcome::AuthFailed)), switch),
                (
                    Seen::Refused(Refusal::Account(Outcome::NotFound {
                        retry_after: None,
                        cooldown: Duration::ZERO,
                    })),
                    switch,
                ),
                (
                    Seen::Refused(Refusal::EgressBlocked(egress::Block {
                        code: "cf_blocked",
                        cf_code: Some(1010),
                    })),
                    Verdict::Stop(Final::Egress),
                ),
                (
                    Seen::Refused(Refusal::Client),
                    if policy.pass_through_client_errors {
                        Verdict::Accept
                    } else {
                        Verdict::Stop(Final::This)
                    },
                ),
            ] {
                assert_eq!(decide(&policy, &seen, &ctx()), expected, "{seen:?}");
            }
            for (refreshed, refresh_failed) in [(true, false), (false, true), (true, true)] {
                assert_eq!(
                    decide(
                        &policy,
                        &Seen::Refused(Refusal::Unauthorized),
                        &Ctx {
                            refreshed,
                            refresh_failed,
                            ..ctx()
                        }
                    ),
                    switch
                );
            }
            for (overload, expected) in [
                (
                    OverloadPlan::Switch,
                    Verdict::Switch {
                        jitter: true,
                        transient: false,
                    },
                ),
                (
                    OverloadPlan::WaitRetry(Duration::from_secs(1)),
                    Verdict::RetrySame {
                        wait: RetryWait::Overload(Duration::from_secs(1)),
                    },
                ),
                (
                    OverloadPlan::PassThrough,
                    Verdict::Stop(if policy.final_error == FinalError::Ranked {
                        Final::Best
                    } else {
                        Final::This
                    }),
                ),
            ] {
                // Overload does not consume the transient retry budget.
                let ctx = Ctx {
                    overload,
                    transient: u32::MAX,
                    ..ctx()
                };
                let seen = Seen::Refused(Refusal::Overloaded {
                    code: "server_is_overloaded".into(),
                });
                assert_eq!(decide(&policy, &seen, &ctx), expected);
                assert_eq!(expected.next(&seen), overload.next());
            }
        }
    }

    #[test]
    fn overload_plan_table_covers_headers_waits_and_captured_ws_limit() {
        for (others, waited, base, no_more, switches, limit, expected_http, expected_ws) in [
            (
                true,
                false,
                3,
                false,
                0,
                1,
                OverloadPlan::Switch,
                OverloadPlan::Switch,
            ),
            (
                true,
                true,
                0,
                true,
                1,
                2,
                OverloadPlan::Switch,
                OverloadPlan::PassThrough,
            ),
            (
                true,
                false,
                3,
                false,
                1,
                3,
                OverloadPlan::Switch,
                OverloadPlan::Switch,
            ),
            (
                false,
                false,
                3,
                false,
                0,
                1,
                OverloadPlan::WaitRetry(Duration::from_secs(1)),
                OverloadPlan::WaitRetry(Duration::from_secs(1)),
            ),
            (
                false,
                true,
                3,
                false,
                0,
                1,
                OverloadPlan::PassThrough,
                OverloadPlan::PassThrough,
            ),
            (
                false,
                false,
                0,
                false,
                0,
                1,
                OverloadPlan::PassThrough,
                OverloadPlan::PassThrough,
            ),
            (
                false,
                false,
                3,
                true,
                0,
                1,
                OverloadPlan::PassThrough,
                OverloadPlan::PassThrough,
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("retry-after", "1".parse().expect("header"));
            if no_more {
                headers.insert("x-retry-metadata", "NO_MORE_RETRY".parse().expect("header"));
            }
            assert_eq!(
                http().overload_plan(others, waited, &headers, base, switches, limit),
                expected_http
            );
            assert_eq!(
                Policy::ws_redial().overload_plan(others, waited, &headers, base, switches, limit),
                expected_ws
            );
        }
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "120".parse().expect("header"));
        assert_eq!(
            http().overload_plan(false, false, &headers, 3, 0, 1),
            OverloadPlan::WaitRetry(Duration::from_secs(10))
        );
        // A WS frame has no headers; its one wait uses the configured jitter range.
        let OverloadPlan::WaitRetry(wait) =
            Policy::ws_redial().overload_plan(false, false, &HeaderMap::new(), 3, 0, 1)
        else {
            panic!("frame retry")
        };
        assert!((Duration::from_secs(2)..=Duration::from_secs(5)).contains(&wait));
    }

    #[test]
    fn classify_dial_and_refusal_table() {
        let opts = UpstreamOpts::default();
        for (status, body, expected) in [
            (400, "bad request", Refusal::Client),
            (401, "bad token", Refusal::Unauthorized),
            (403, "forbidden", Refusal::Account(Outcome::AuthFailed)),
            (
                403,
                "error code: 1010",
                Refusal::EgressBlocked(egress::Block {
                    code: "cf_blocked",
                    cf_code: Some(1010),
                }),
            ),
            (
                402,
                "payment",
                Refusal::Account(Outcome::PaymentRequired {
                    deactivated: false,
                    cooldown: Duration::from_secs(opts.payment_required_cooldown_secs),
                }),
            ),
            (
                404,
                "missing",
                Refusal::Account(Outcome::NotFound {
                    retry_after: Some(Duration::from_secs(7)),
                    cooldown: Duration::from_secs(opts.not_found_cooldown_secs),
                }),
            ),
            (
                429,
                r#"{"error":{"type":"usage_limit_reached"}}"#,
                Refusal::Account(Outcome::RateLimited {
                    retry_after: Some(Duration::from_secs(7)),
                    usage_limit: true,
                    quota: None,
                }),
            ),
            (
                408,
                "timeout",
                Refusal::Server {
                    retry_after: Some(Duration::from_secs(7)),
                },
            ),
            (
                500,
                "server error",
                Refusal::Server {
                    retry_after: Some(Duration::from_secs(7)),
                },
            ),
            (
                503,
                r#"{"error":{"code":"server_is_overloaded"}}"#,
                Refusal::Overloaded {
                    code: "server_is_overloaded".into(),
                },
            ),
            (
                400,
                r#"{"error":{"code":"server_is_overloaded"}}"#,
                Refusal::Overloaded {
                    code: "server_is_overloaded".into(),
                },
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("retry-after", "7".parse().expect("header"));
            let dialed: Dialed<()> = Dialed::Refused {
                status,
                headers,
                body: bytes::Bytes::from_static(body.as_bytes()),
            };
            assert_eq!(
                classify(&dialed, &opts),
                Seen::Refused(expected),
                "{status}: {body}"
            );
        }
        assert_eq!(
            classify(
                &Dialed::Connected {
                    conn: (),
                    status: 200
                },
                &opts
            ),
            Seen::Connected
        );
        let network: Dialed<()> = Dialed::Transport {
            code: "transport",
            cause: String::new(),
        };
        assert_eq!(classify(&network, &opts), Seen::Transport("transport"));
        let encoding: Dialed<()> = Dialed::UnsupportedEncoding {
            status: 200,
            request_id: None,
            message: String::new(),
        };
        assert_eq!(classify(&encoding, &opts), Seen::UnsupportedEncoding);
    }

    #[test]
    fn quota_replay_counts_the_current_failure() {
        for (initial, limit, expected) in [
            (0, 1, Next::Stop),
            (0, 2, Next::Switch),
            (1, 2, Next::Stop),
            (1, 3, Next::Switch),
        ] {
            let mut switches = initial;
            assert_eq!(quota_plan(&mut switches, limit), expected);
            assert_eq!(switches, initial + 1);
        }
    }
}
