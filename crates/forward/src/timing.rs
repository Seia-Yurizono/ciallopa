//! Production seconds stay unchanged; loopback integration tests can use shorter units.
use crate::AppState;
use ciallopa_core::Config;
use std::time::Duration;

pub(crate) struct Timing {
    pub preparation: Duration,
    pub header: Duration,
    pub idle: Duration,
    pub cooldown_wait: Duration,
}

impl Timing {
    pub fn capture(cfg: &Config) -> Self {
        let o = &cfg.upstream_opts;
        Self {
            preparation: scaled(cfg, Duration::from_secs(o.preparation_timeout_secs)),
            header: scaled(cfg, Duration::from_secs(o.response_header_timeout_secs)),
            idle: scaled(cfg, Duration::from_secs(o.stream_idle_timeout_secs.max(1))),
            cooldown_wait: scaled(cfg, Duration::from_secs(cfg.routing.cooldown_wait_secs)),
        }
    }
}

fn scaled(cfg: &Config, duration: Duration) -> Duration {
    let unit = cfg.upstream_opts.time_unit;
    if unit == Duration::from_secs(1) {
        duration
    } else {
        duration.mul_f64(unit.as_secs_f64())
    }
}

pub(crate) fn scale(st: &AppState, duration: Duration) -> Duration {
    scaled(&st.config.load(), duration)
}

pub(crate) fn switch_jitter(st: &AppState) -> Duration {
    scale(st, ciallopa_upstream::retry::switch_jitter())
}

/// Count elapsed time even when the preparation deadline or client cancels the await.
pub(crate) async fn accumulate<T>(
    total: Option<&mut Option<Duration>>,
    future: impl std::future::Future<Output = T>,
) -> T {
    struct Elapsed<'a>(&'a mut Option<Duration>, std::time::Instant);
    impl Drop for Elapsed<'_> {
        fn drop(&mut self) {
            *self.0.get_or_insert(Duration::ZERO) += self.1.elapsed();
        }
    }
    let _elapsed = total.map(|total| Elapsed(total, std::time::Instant::now()));
    future.await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_durations_are_exact_and_test_units_scale_them() {
        let mut cfg = Config::parse("").expect("default config");
        for d in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
            assert_eq!(scaled(&cfg, d), d);
        }
        let timing = Timing::capture(&cfg);
        assert_eq!(timing.preparation, Duration::from_secs(600));
        assert_eq!(timing.header, Duration::from_secs(300));
        assert_eq!(timing.idle, Duration::from_secs(300));
        cfg.upstream_opts.time_unit = Duration::from_millis(20);
        assert_eq!(
            scaled(&cfg, Duration::from_millis(500)),
            Duration::from_millis(10)
        );
        assert_eq!(Timing::capture(&cfg).preparation, Duration::from_secs(12));
        cfg.upstream_opts.stream_idle_timeout_secs = 0;
        assert_eq!(Timing::capture(&cfg).idle, Duration::from_millis(20));
    }

    #[test]
    fn time_unit_has_no_config_surface() {
        assert!(Config::parse("[upstream_opts]\ntime_unit = 0").is_err());
        let cfg = Config::parse("").expect("default config");
        let mut opts = cfg.upstream_opts;
        opts.time_unit = Duration::from_millis(20);
        let encoded = serde_json::to_value(&opts).expect("serialize options");
        assert!(encoded.get("time_unit").is_none());
        let decoded: ciallopa_core::config::UpstreamOpts =
            serde_json::from_value(encoded).expect("deserialize options");
        assert_eq!(decoded.time_unit, Duration::from_secs(1));
    }
}
