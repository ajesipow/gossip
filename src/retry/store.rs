use std::collections::BTreeMap;
use std::collections::HashMap;

use chrono::DateTime;
use chrono::Utc;

use crate::pre_message::PreMessage;
use crate::retry::policy::RetryDecision;
use crate::retry::policy::RetryPolicy;
use crate::retry::RetryMessage;

/// A datastructure to organise messages for retry
#[derive(Debug)]
pub(crate) struct RetryStore<P> {
    retry_queue: BTreeMap<DateTime<Utc>, Vec<RetryMessage>>,
    broadcast_messages: HashMap<PreMessage, RetryDecision>,
    policy: P,
}

impl<P: RetryPolicy> RetryStore<P> {
    /// Creates a new instance of a [`RetryStore`] with the given policy.
    pub(crate) fn new(policy: P) -> Self {
        Self {
            retry_queue: Default::default(),
            broadcast_messages: HashMap::with_capacity(1024),
            policy,
        }
    }

    pub(crate) fn add(
        &mut self,
        msg: PreMessage,
    ) {
        if self.broadcast_messages.contains_key(&msg) {
            return;
        }
        let retry_attempt = 0;
        let first_retry = Utc::now();
        let decision = self.policy.should_retry(first_retry, retry_attempt);
        self.broadcast_messages.insert(msg.clone(), decision);
        match decision {
            RetryDecision::Retry { retry_after } => {
                self.retry_queue
                    .entry(retry_after)
                    .or_default()
                    .push(RetryMessage {
                        n_past_retries: retry_attempt,
                        first_retry,
                        // TODO avoid clone
                        msg: msg.clone(),
                    });
            }
            RetryDecision::DoNotRetry => {}
        }
    }

    pub(crate) fn contains(
        &self,
        msg: &PreMessage,
    ) -> bool {
        self.broadcast_messages.contains_key(msg)
    }

    pub(crate) fn remove(
        &mut self,
        pre_msg: &PreMessage,
    ) {
        if let Some(decision) = self.broadcast_messages.get_mut(pre_msg) {
            match decision {
                RetryDecision::Retry { retry_after } => {
                    self.retry_queue.remove(retry_after);
                }
                RetryDecision::DoNotRetry => {}
            }
            // TODO this would grow indefinitely over time
            // Remember the key to not retry in the future
            *decision = RetryDecision::DoNotRetry;
        }
    }
}

impl<P: RetryPolicy> Iterator for RetryStore<P> {
    type Item = Vec<RetryMessage>;

    /// Every consumed message will have its retry decision updated
    /// automatically and is assumed to have been retried by the caller.
    fn next(&mut self) -> Option<Self::Item> {
        let maybe_msg = self.retry_queue.pop_first();
        match maybe_msg {
            None => None,
            Some((retry_after, msgs)) => {
                if retry_after <= Utc::now() {
                    for msg in &msgs {
                        let last_retry_attempts = msg.n_past_retries;
                        let retry_decision = self
                            .policy
                            .should_retry(msg.first_retry, last_retry_attempts);
                        self.broadcast_messages.insert(
                            // TODO avoid clone
                            msg.msg.clone(),
                            retry_decision,
                        );
                        match retry_decision {
                            RetryDecision::Retry { retry_after } => {
                                self.retry_queue.entry(retry_after).or_default().push(
                                    RetryMessage {
                                        n_past_retries: last_retry_attempts + 1,
                                        first_retry: msg.first_retry,
                                        // TODO avoid clone
                                        msg: msg.msg.clone(),
                                    },
                                );
                            }
                            RetryDecision::DoNotRetry => {}
                        }
                    }
                    Some(msgs)
                } else {
                    // Too early to retry, put message back
                    self.retry_queue.insert(retry_after, msgs);
                    None
                }
            }
        }
    }
}
