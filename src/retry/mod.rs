mod policy;
mod store;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use itertools::Itertools;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::interval;
use tokio::time::sleep;
use tracing::debug;
use tracing::error;

use crate::node::NODE_ID;
use crate::pre_message::BroadcastPreBody;
use crate::pre_message::PreMessage;
use crate::pre_message::PreMessageBody;
use crate::primitives::BroadcastMessage;
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
    neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<BroadcastMessage>>>>,
    broadcast_messages: Arc<RwLock<HashSet<BroadcastMessage>>>,
    retry_store: RetryStore<ExponentialBackOff>,
}

impl RetryHandler {
    pub(crate) fn new(
        msg_dispatch_queue_tx: Sender<Vec<PreMessage>>,
        neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<BroadcastMessage>>>>,
        broadcast_messages: Arc<RwLock<HashSet<BroadcastMessage>>>,
    ) -> Self {
        Self {
            msg_dispatch_queue_tx,
            neighbour_broadcast_messages,
            broadcast_messages,
            retry_store: RetryStore::new(ExponentialBackOff::default()),
        }
    }

    pub(crate) async fn run(&mut self) {
        debug!("Running retry handler");
        // let mut interval = interval(Duration::from_millis(1));
        loop {
            debug!("retry loop 1");
            // Why does this not work? blog post!
            // interval.tick().await;
            debug!("retry loop 2");
            // Send the same broadcast message to other nodes that we think have not seen it
            // yet.
            sleep(Duration::from_millis(1)).await;
            debug!("retry loop 3");
            let neighbour_broadcast_messages_lock = self.neighbour_broadcast_messages.read().await;
            let broadcast_messages_lock = self.broadcast_messages.read().await;
            let new_unacked_broadcast_messages: Vec<_> = neighbour_broadcast_messages_lock
                .iter()
                .flat_map(|(neighbour, acknowledged_messages)| {
                    broadcast_messages_lock
                        .difference(acknowledged_messages)
                        .map(|unacknowledged_message| {
                            PreMessage::broadcast(
                                MessageRecipient::new(neighbour.to_string()),
                                *unacknowledged_message,
                            )
                        })
                        .filter(|msg| !self.retry_store.contains(msg))
                })
                .collect();
            // Remove all received msgs from the retry store.
            let msgs_to_remove = neighbour_broadcast_messages_lock
                .iter()
                .flat_map(|(neighbour, bdcast_msgs)| {
                    bdcast_msgs.iter().map(|bdcast_msg| {
                        PreMessage::broadcast(
                            MessageRecipient::new(neighbour.to_string()),
                            *bdcast_msg,
                        )
                    })
                })
                .collect_vec();
            drop(neighbour_broadcast_messages_lock);
            drop(broadcast_messages_lock);
            debug!(
                "new_unacked_broadcast_messages: {:?} node {:?}",
                new_unacked_broadcast_messages.len(),
                NODE_ID.get().unwrap()
            );
            for msg in new_unacked_broadcast_messages {
                self.retry_store.add(msg)
            }

            for msg in &msgs_to_remove {
                self.retry_store.remove(msg);
            }

            for msgs in self.retry_store.by_ref() {
                let n_msgs = msgs.len();
                debug!(
                    "retry msgs {n_msgs:?} - {:?}: {msgs:?}",
                    NODE_ID.get().unwrap()
                );
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
