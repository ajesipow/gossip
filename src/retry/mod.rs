mod policy;
mod store;

use std::iter;
use std::time::Duration;

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use itertools::Itertools;
pub use policy::ExponentialBackOff;
pub use policy::RetryPolicy;
use tokio::time::interval;
use tracing::debug;
use tracing::error;

use crate::broadcast::Broadcast;
use crate::dispatch::MessageDispatchHandle;
use crate::message_store::BroadcastMessageStoreHandle;
use crate::primitives::BroadcastMessage;
use crate::primitives::MessageId;
use crate::primitives::MessageRecipient;
use crate::primitives::NodeId;
use crate::protocol::Message;
use crate::retry::store::RetryStore;
use crate::topology::Topology;

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
#[derive(Debug, Clone)]
pub(crate) struct RetryHandle {
    message_store: BroadcastMessageStoreHandle,
}

impl RetryHandle {
    pub(crate) fn new<P: RetryPolicy + Send + 'static>(
        msg_dispatch: MessageDispatchHandle,
        message_store: BroadcastMessageStoreHandle,
        policy: P,
    ) -> Self {
        // TODO handle shutdown
        let message_store_clone = message_store.clone();
        tokio::spawn(async {
            let retry_store = RetryStore::new(policy);
            let mut broadcast = RetryBroadcast {
                msg_dispatch,
                message_store: message_store_clone,
                retry_store,
            };
            broadcast.run().await
        });

        Self { message_store }
    }
}

#[async_trait]
impl Broadcast for RetryHandle {
    async fn messages(&self) -> anyhow::Result<Vec<BroadcastMessage>> {
        self.message_store.msgs().await
    }

    async fn update_topology(
        &self,
        topology: Topology,
    ) {
        let neighbours = topology.overlay_neighbours().unwrap_or_default();

        if !neighbours.is_empty() {
            for neighbour in neighbours {
                self.message_store
                    .register_peer(neighbour.to_string())
                    .await;
            }
        }
    }

    async fn ack_by_msg_id(
        &self,
        node: NodeId,
        msg_id: MessageId,
    ) {
        // Remember that the recipient received the broadcast message so that we do not
        // send it again.
        self.message_store
            .insert_for_peer_by_msg_id_if_exists(node.to_string(), msg_id)
            .await;
    }

    async fn broadcast(
        &self,
        msg: BroadcastMessage,
    ) {
        self.message_store.insert(msg).await;
    }
}

struct RetryBroadcast<P> {
    msg_dispatch: MessageDispatchHandle,
    message_store: BroadcastMessageStoreHandle,
    retry_store: RetryStore<P>,
}

impl<P: RetryPolicy> RetryBroadcast<P> {
    pub(crate) async fn run(&mut self) {
        debug!("Running retry broadcast");
        let mut interval = interval(Duration::from_millis(1));
        loop {
            interval.tick().await;
            // Send the same broadcast message to other nodes that we think have not seen it
            // yet.
            let unacked_broadcast_messages_by_nodes =
                self.message_store.unacked_nodes_all_msgs().await.unwrap();
            let recently_acked_msgs_by_nodes =
                self.message_store.recent_peer_inserts().await.unwrap();

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
