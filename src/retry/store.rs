use std::collections::BTreeMap;
use std::collections::HashSet;

use chrono::DateTime;
use chrono::Utc;

use crate::pre_message::PreMessage;
use crate::pre_message::PreMessageBody;
use crate::primitives::BroadcastMessage;
use crate::retry::policy::RetryDecision;
use crate::retry::policy::RetryPolicy;
use crate::retry::RetryMessage;

/// A datastructure to organise messages for retry
#[derive(Debug)]
pub(crate) struct RetryStore<P> {
    retry_queue: BTreeMap<DateTime<Utc>, Vec<RetryMessage>>,
    broadcast_messages: HashSet<(BroadcastMessage, String)>,
    policy: P,
}

impl<P: RetryPolicy> RetryStore<P> {
    /// Creates a new instance of a [`RetryStore`] with the given policy.
    pub(crate) fn new(policy: P) -> Self {
        Self {
            retry_queue: Default::default(),
            broadcast_messages: HashSet::with_capacity(1024),
            policy,
        }
    }

    pub(crate) fn add(
        &mut self,
        msg: PreMessage,
    ) {
        // TODO what if the msg already exists?
        if let PreMessageBody::Broadcast(body) = &msg.body {
            self.broadcast_messages
                .insert((body.message, msg.dest.as_ref().to_string()));
        }
        let retry_attempt = 0;
        let first_retry = Utc::now();
        match self.policy.should_retry(first_retry, retry_attempt) {
            RetryDecision::Retry { retry_after } => {
                self.retry_queue
                    .entry(retry_after)
                    .or_default()
                    .push(RetryMessage {
                        n_past_retries: retry_attempt,
                        first_retry,
                        msg,
                    });
            }
            RetryDecision::DoNotRetry => {}
        }
    }

    pub(crate) fn contains(
        &self,
        // TODO no owned type
        broadcast_message: BroadcastMessage,
        neighbour: String,
    ) -> bool {
        self.broadcast_messages
            .contains(&(broadcast_message, neighbour))
    }
}

impl<P: RetryPolicy> Iterator for RetryStore<P> {
    type Item = Vec<RetryMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        let now = Utc::now();

        let msgs = self.retry_queue.range(..now);
        let all_messages: Vec<RetryMessage> = msgs.flat_map(|m| m.1).cloned().collect();
        for msg in &all_messages {
            let last_retry_attempts = msg.n_past_retries;
            match self
                .policy
                .should_retry(msg.first_retry, last_retry_attempts)
            {
                RetryDecision::Retry { retry_after } => {
                    self.retry_queue
                        .entry(retry_after)
                        .or_default()
                        .push(RetryMessage {
                            n_past_retries: last_retry_attempts + 1,
                            first_retry: msg.first_retry,
                            // TODO avoid clone
                            msg: msg.msg.clone(),
                        });
                }
                RetryDecision::DoNotRetry => {}
            }
        }
        Some(all_messages)
    }
}
