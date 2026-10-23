//! Cumulative time spent preparing attempts, separate from successful stream reads.
use axum::response::Response;
use std::time::Duration;

pub(crate) struct PreparationBudget {
    remaining: Duration,
}

impl PreparationBudget {
    pub(crate) fn new(limit: Duration) -> Self {
        Self { remaining: limit }
    }

    pub(crate) async fn run<T>(
        &mut self,
        future: impl std::future::Future<Output = T>,
    ) -> Result<T, Response> {
        // Do not poll a new attempt after the shared budget has been spent.
        if self.remaining.is_zero() {
            return Err(crate::errors::network("preparation_timeout"));
        }
        let start = tokio::time::Instant::now();
        let result = tokio::time::timeout(self.remaining, future).await;
        self.remaining = self.remaining.saturating_sub(start.elapsed());
        result.map_err(|_| {
            self.remaining = Duration::ZERO;
            crate::errors::network("preparation_timeout")
        })
    }
}
