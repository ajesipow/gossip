mod policy;
mod store;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::debug;
use tracing::error;

use crate::pre_message::BroadcastPreBody;
use crate::pre_message::PreMessage;
use crate::pre_message::PreMessageBody;
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
    neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
    broadcast_messages: Arc<RwLock<HashSet<usize>>>,
    retry_store: RetryStore<ExponentialBackOff>,
}

impl RetryHandler {
    pub(crate) fn new(
        msg_dispatch_queue_tx: Sender<Vec<PreMessage>>,
        neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
        broadcast_messages: Arc<RwLock<HashSet<usize>>>,
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
        // Periodically check for message to dispatch for sending
        loop {
            sleep(Duration::from_millis(5)).await;

            // Send the same broadcast message to other nodes that we think have not seen it
            // yet.
            let neighbour_broadcast_messages_lock = self.neighbour_broadcast_messages.read().await;
            let broadcast_messages_lock = self.broadcast_messages.read().await;
            let new_unacknowledged_broadcast_messages: Vec<_> = neighbour_broadcast_messages_lock
                .iter()
                .flat_map(|(neighbour, acknowledged_messages)| {
                    broadcast_messages_lock
                        .difference(acknowledged_messages)
                        // TODO avoid clone
                        .filter(|msg| self.retry_store.contains(**msg, neighbour.to_string()))
                        .map(|unacknowledged_message| {
                            PreMessage::new(
                                // TODO avoid clone
                                MessageRecipient(neighbour.clone()),
                                PreMessageBody::Broadcast(BroadcastPreBody {
                                    message: *unacknowledged_message,
                                }),
                            )
                        })
                })
                .collect();
            drop(neighbour_broadcast_messages_lock);
            drop(broadcast_messages_lock);

            for msg in new_unacknowledged_broadcast_messages {
                self.retry_store.add(msg)
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
