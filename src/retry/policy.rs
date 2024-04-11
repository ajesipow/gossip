use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;

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
    base_interval_ms: u32,
    exponential_rate: f32,
}

impl ExponentialBackOff {
    pub(crate) fn new(max_retry_attempts: u32) -> Self {
        Self {
            max_retry_attempts,
            base_interval_ms: 500,
            exponential_rate: 1.5,
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
            self.base_interval_ms as f32 * self.exponential_rate.powi(n_past_retries as i32);
        RetryDecision::Retry {
            retry_after: first_dispatch_time + Duration::from_millis(next_retry_ms as u64),
        }
    }
}
