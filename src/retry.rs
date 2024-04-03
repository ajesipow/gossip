use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::debug;
use tracing::error;

use crate::pre_message::BroadcastPreBody;
use crate::pre_message::PreMessage;
use crate::pre_message::PreMessageBody;
use crate::primitives::MessageRecipient;

/// Handles message retries
#[derive(Debug)]
pub(crate) struct RetryHandler {
    msg_dispatch_queue_tx: Sender<Vec<PreMessage>>,
    neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
    broadcast_messages: Arc<RwLock<HashSet<usize>>>,
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
        }
    }

    pub(crate) async fn run(&mut self) {
        debug!("Running retry handler");
        // Periodically check for message to dispatch for sending
        loop {
            sleep(Duration::from_millis(100)).await;

            // Send the same broadcast message to other nodes that we think have not seen it
            // yet.
            let neighbour_broadcast_messages_lock = self.neighbour_broadcast_messages.read().await;
            let broadcast_messages_lock = self.broadcast_messages.read().await;
            let unacknowledged_broadcast_messages: Vec<_> = neighbour_broadcast_messages_lock
                .iter()
                .flat_map(|(neighbour, acknowledged_messages)| {
                    broadcast_messages_lock
                        .difference(acknowledged_messages)
                        .sorted_unstable()
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

            if let Err(e) = self
                .msg_dispatch_queue_tx
                .send(unacknowledged_broadcast_messages)
                .await
            {
                error!("Could not retry broadcast message: {:?}", e);
            }
        }
    }
}
