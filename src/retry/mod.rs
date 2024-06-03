mod policy;
mod store;

use std::iter;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use itertools::Itertools;
use tokio::time::interval;
use tracing::debug;
use tracing::error;

use crate::dispatch::MessageDispatchHandle;
use crate::message_store::BroadcastMessageStoreHandle;
use crate::primitives::MessageRecipient;
use crate::protocol::Message;
use crate::retry::policy::ExponentialBackOff;
use crate::retry::store::RetryStore;

/// A message that should be retried
#[derive(Debug, Clone)]
pub(crate) struct RetryMessage {
    // The number of times the message has been retried
    n_past_retries: u32,
    // The time the message was first retried
    first_retry: DateTime<Utc>,
    msg: Message,
}

/// Handles message retries
#[derive(Debug)]
pub(crate) struct RetryHandler {
    msg_dispatch: MessageDispatchHandle,
    broadcast_message_store: BroadcastMessageStoreHandle,
    retry_store: RetryStore<ExponentialBackOff>,
}

impl RetryHandler {
    pub(crate) fn new(
        msg_dispatch: MessageDispatchHandle,
        broadcast_message_store: BroadcastMessageStoreHandle,
    ) -> Self {
        Self {
            msg_dispatch,
            broadcast_message_store,
            retry_store: RetryStore::new(ExponentialBackOff::default()),
        }
    }

    pub(crate) async fn run(&mut self) {
        debug!("Running retry handler");
        let mut interval = interval(Duration::from_millis(1));
        loop {
            interval.tick().await;
            // Send the same broadcast message to other nodes that we think have not seen it
            // yet.
            let unacked_broadcast_messages_by_nodes = self
                .broadcast_message_store
                .unacked_nodes_all_msgs()
                .await
                .unwrap();
            let recently_acked_msgs_by_nodes = self
                .broadcast_message_store
                .recent_peer_inserts()
                .await
                .unwrap();

            let new_unacked_broadcast_messages: Vec<_> = unacked_broadcast_messages_by_nodes
                .into_iter()
                .flat_map(|(neighbour, unacked_msgs)| {
                    unacked_msgs
                        .into_iter()
                        .zip(iter::repeat(neighbour))
                        .map(move |(unacked_msg, peer)| {
                            Message::broadcast(MessageRecipient::new(peer), unacked_msg)
                        })
                        .filter(|msg| !self.retry_store.contains(msg))
                })
                .collect();
            // Remove all received msgs from the retry store.
            let msgs_to_remove = recently_acked_msgs_by_nodes
                .into_iter()
                .flat_map(|(neighbour, bdcast_msgs)| {
                    bdcast_msgs.into_iter().zip(iter::repeat(neighbour)).map(
                        move |(bdcast_msg, peer)| {
                            Message::broadcast(MessageRecipient::new(peer), bdcast_msg)
                        },
                    )
                })
                .collect_vec();
            for msg in new_unacked_broadcast_messages {
                self.retry_store.add(msg)
            }

            for msg in &msgs_to_remove {
                self.retry_store.remove(msg);
            }

            for msgs in self.retry_store.by_ref() {
                debug!("Retrying messages {:?}", msgs);
                let to_retry = msgs.into_iter().map(|msg| msg.msg).collect_vec();
                if !to_retry.is_empty() {
                    if let Err(e) = self.msg_dispatch.dispatch(to_retry).await {
                        error!("Could not retry broadcast message: {:?}", e);
                    }
                }
            }
        }
    }
}
