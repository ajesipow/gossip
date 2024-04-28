use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;

#[derive(Debug, Copy, Clone)]
pub(crate) enum RetryDecision {
    Retry { retry_after: DateTime<Utc> },
    DoNotRetry,
}

pub(crate) trait RetryPolicy {
    /// Decides if and when a retry should be made.
    fn should_retry(
        &self,
        first_dispatch_time: DateTime<Utc>,
        n_past_retries: u32,
    ) -> RetryDecision;
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct ExponentialBackOff {
    max_retry_attempts: u32,
    base_interval_ms: f32,
    exponential_rate: f32,
}

impl ExponentialBackOff {
    pub(crate) fn new(max_retry_attempts: u32) -> Self {
        Self {
            max_retry_attempts,
            base_interval_ms: 5.0,
            exponential_rate: 5.0,
        }
    }
}

impl Default for ExponentialBackOff {
    fn default() -> Self {
        Self::new(5)
    }
}

impl RetryPolicy for ExponentialBackOff {
    fn should_retry(
        &self,
        first_dispatch_time: DateTime<Utc>,
        n_past_retries: u32,
    ) -> RetryDecision {
        if n_past_retries >= self.max_retry_attempts {
            return RetryDecision::DoNotRetry;
        }
        let next_retry_ms =
            self.base_interval_ms * self.exponential_rate.powi(n_past_retries as i32);
        let retry_after =
            first_dispatch_time + Duration::from_micros((next_retry_ms * 100.0) as u64);
        RetryDecision::Retry { retry_after }
    }
}
