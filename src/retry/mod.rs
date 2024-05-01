mod policy;
mod store;

use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use itertools::Itertools;
use thingbuf::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::debug;
use tracing::error;

use crate::message_store::BroadcastMessageStore;
use crate::pre_message::PreMessage;
use crate::primitives::MessageRecipient;
use crate::retry::policy::ExponentialBackOff;
use crate::retry::store::RetryStore;

/// A message that should be retried
#[derive(Debug, Clone)]
pub(crate) struct RetryMessage {
    // The number of times the message has been retried
    n_past_retries: u32,
    // The time the message was first retried
    first_retry: DateTime<Utc>,
    msg: PreMessage,
}

/// Handles message retries
#[derive(Debug)]
pub(crate) struct RetryHandler {
    msg_dispatch_queue_tx: Sender<Vec<PreMessage>>,
    broadcast_message_store: Arc<RwLock<BroadcastMessageStore>>,
    retry_store: RetryStore<ExponentialBackOff>,
}

impl RetryHandler {
    pub(crate) fn new(
        msg_dispatch_queue_tx: Sender<Vec<PreMessage>>,
        broadcast_message_store: Arc<RwLock<BroadcastMessageStore>>,
    ) -> Self {
        Self {
            msg_dispatch_queue_tx,
            broadcast_message_store,
            retry_store: RetryStore::new(ExponentialBackOff::default()),
        }
    }

    pub(crate) async fn run(&mut self) {
        debug!("Running retry handler");
        let mut interval = interval(Duration::from_micros(100));
        loop {
            interval.tick().await;
            // Send the same broadcast message to other nodes that we think have not seen it
            // yet.

            let mut lock = self.broadcast_message_store.write().await;
            let unacked_broadcast_messages_by_nodes = lock.unacked_nodes_all_msgs();
            let recently_acked_msgs_by_nodes = lock.recent_peer_inserts();
            drop(lock);

            let new_unacked_broadcast_messages: Vec<_> = unacked_broadcast_messages_by_nodes
                .into_iter()
                .flat_map(|(neighbour, unacked_msgs)| {
                    unacked_msgs
                        .into_iter()
                        .map(move |unacked_msg| {
                            PreMessage::broadcast(
                                // TODO zip repeat
                                MessageRecipient::new(neighbour.to_string()),
                                unacked_msg,
                            )
                        })
                        .filter(|msg| !self.retry_store.contains(msg))
                })
                .collect();
            // Remove all received msgs from the retry store.
            let msgs_to_remove = recently_acked_msgs_by_nodes
                .into_iter()
                .flat_map(|(neighbour, bdcast_msgs)| {
                    // TODO zip repeat
                    bdcast_msgs.into_iter().map(move |bdcast_msg| {
                        PreMessage::broadcast(
                            MessageRecipient::new(neighbour.to_string()),
                            bdcast_msg,
                        )
                    })
                })
                .collect_vec();

            for msg in new_unacked_broadcast_messages {
                self.retry_store.add(msg)
            }

            for msg in &msgs_to_remove {
                self.retry_store.remove(msg);
            }

            for msgs in self.retry_store.by_ref() {
                let n_msgs = msgs.len();
                if let Err(e) = self
                    .msg_dispatch_queue_tx
                    .send(msgs.into_iter().map(|msg| msg.msg).collect())
                    .await
                {
                    error!("Could not retry broadcast message: {:?}", e);
                } else {
                    debug!("Retried {} messages", n_msgs);
                }
            }
        }
    }
}
