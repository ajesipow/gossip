use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::time::interval;
use tracing::error;
use tracing::instrument;

use crate::broadcast::node_selector::NodeSelector;
use crate::broadcast::Broadcast;
use crate::dispatch::MessageDispatchHandle;
use crate::message_store::BroadcastMessageStoreHandle;
use crate::primitives::BroadcastMessage;
use crate::primitives::MessageId;
use crate::primitives::NodeId;
use crate::protocol::Message;
use crate::topology::Topology;
use crate::topology::TopologyStoreHandle;

/// A handle for interacting with a gossip broadcast
#[derive(Debug, Clone)]
pub(crate) struct GossipHandle {
    topology_store: TopologyStoreHandle,
    message_store: BroadcastMessageStoreHandle,
}

impl GossipHandle {
    pub(crate) fn new(
        message_store: BroadcastMessageStoreHandle,
        topology_store: TopologyStoreHandle,
        fanout: Fanout,
        max_broadcast_rounds: usize,
        msg_dispatch: MessageDispatchHandle,
    ) -> Self {
        let gossip = Gossip::new(
            message_store.clone(),
            topology_store.clone(),
            fanout,
            max_broadcast_rounds,
            msg_dispatch,
        );

        // TODO cancel task when all handles are gone
        tokio::spawn(run_gossip(gossip));

        Self {
            topology_store,
            message_store,
        }
    }
}

/// An implementation of a simple probabilistic gossip protocol.
#[derive(Debug)]
struct Gossip {
    message_store: BroadcastMessageStoreHandle,
    topology_store: TopologyStoreHandle,
    // The number of nodes to broadcast to per round
    fanout: Fanout,
    // Max number of rounds we broadcast each message
    max_broadcast_rounds: usize,
    // Messages actively broadcast mapped to the number of rounds they have already been
    // broadcast. One round usually broadcasts to more than one node.
    infectious_msgs: HashMap<BroadcastMessage, usize>,
    // Messages that are not broadcast any longer
    removed_msgs: HashSet<BroadcastMessage>,
    msg_dispatch: MessageDispatchHandle,
}

// TODO: impl Default, impl Builder, config?
// TODO policy?
impl Gossip {
    fn new(
        message_store: BroadcastMessageStoreHandle,
        topology_store: TopologyStoreHandle,
        fanout: Fanout,
        max_broadcast_rounds: usize,
        msg_dispatch: MessageDispatchHandle,
    ) -> Self {
        Self {
            message_store,
            topology_store,
            fanout,
            max_broadcast_rounds,
            infectious_msgs: HashMap::new(),
            removed_msgs: HashSet::new(),
            msg_dispatch,
        }
    }

    /// Triggers a broadcast round for all infectious broadcast messages.
    #[instrument(skip(self))]
    async fn broadcast(&mut self) {
        // TODO get notified about new topology and update node selector
        let topology = self.topology_store.topology().await;
        let Some(topology) = topology else {
            return;
        };
        let msgs = self.message_store.msgs().await.unwrap();
        for broadcast_message in msgs {
            if !self.infectious_msgs.contains_key(&broadcast_message)
                && !self.removed_msgs.contains(&broadcast_message)
            {
                self.infectious_msgs.insert(broadcast_message, 0);
            }
        }

        let node_selector = NodeSelector::new(topology);
        for (broadcast_msg, broadcast_round) in self.infectious_msgs.iter_mut() {
            *broadcast_round += 1;
            if *broadcast_round >= self.max_broadcast_rounds {
                self.removed_msgs.insert(*broadcast_msg);
                continue;
            }
            let nodes = node_selector.select(self.fanout.0);
            let mut msgs = Vec::with_capacity(nodes.len());
            for node in nodes {
                let message = Message::broadcast(node.into(), *broadcast_msg);
                self.message_store
                    .register_msg_id(message.id(), *broadcast_msg)
                    .await;
                msgs.push(message);
            }

            // FIXME AJES: proper error handling
            if self.msg_dispatch.dispatch(msgs).await.is_err() {
                error!("Cannot dispatch messages");
            }
        }
        self.infectious_msgs
            .retain(|_, broadcasts| *broadcasts < self.max_broadcast_rounds);
    }
}

async fn run_gossip(mut gossip: Gossip) {
    let mut interval = interval(Duration::from_millis(50));
    loop {
        interval.tick().await;
        gossip.broadcast().await;
    }
}

#[async_trait]
impl Broadcast for GossipHandle {
    #[instrument(skip(self))]
    async fn messages(&self) -> Result<Vec<BroadcastMessage>> {
        self.message_store.msgs().await
    }

    #[instrument(skip_all)]
    async fn update_topology(
        &self,
        topology: Topology,
    ) {
        let neighbours = topology.overlay_neighbours().unwrap_or_default();
        // TODO error handling
        self.topology_store.update(topology).await.unwrap();

        if !neighbours.is_empty() {
            for neighbour in neighbours {
                self.message_store
                    .register_peer(neighbour.to_string())
                    .await;
            }
        }
    }

    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
    async fn broadcast(
        &self,
        msg: BroadcastMessage,
    ) {
        self.message_store.insert(msg).await;
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Fanout(usize);

impl Fanout {
    pub(crate) fn new(fanout: usize) -> Self {
        Self(fanout)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::BytesMut;
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::node::NODE_ID;
    use crate::protocol::MessageBody;
    use crate::topology::Topology;

    #[tokio::test]
    async fn test_basic_broadcast_works() {
        let this_node_id = NodeId::from("n1");
        NODE_ID.set(this_node_id.clone()).unwrap();
        let topology: Topology = (
            BTreeMap::from_iter([
                ("n1".to_string(), vec!["n2".to_string(), "n3".to_string()]),
                ("n2".to_string(), vec!["n1".to_string()]),
                ("n3".to_string(), vec!["n1".to_string()]),
            ]),
            this_node_id,
        )
            .into();

        let msg = BroadcastMessage::new(1);

        let msg_store = BroadcastMessageStoreHandle::new();
        let (mut client, server) = tokio::io::duplex(1024);
        let dispatch = MessageDispatchHandle::new(server);
        let topology_store_handle = TopologyStoreHandle::new();
        let gossip = GossipHandle::new(
            msg_store,
            topology_store_handle,
            Fanout::new(2),
            2,
            dispatch,
        );

        gossip.update_topology(topology).await;
        gossip.broadcast(msg).await;

        let mut buf = BytesMut::with_capacity(1024);
        client.read_buf(&mut buf).await.unwrap();
        let raw_message_str = String::from_utf8_lossy(&buf);
        let raw_messages = raw_message_str.split('\n');
        let messages: Vec<Message> = raw_messages
            .into_iter()
            .filter_map(|raw_msg| {
                if raw_msg.is_empty() {
                    // A trailing `\n` leads to an empty string
                    None
                } else {
                    serde_json::from_str(raw_msg).ok()
                }
            })
            .collect();

        let mut results = HashMap::new();
        for pre_msg in messages {
            if let MessageBody::Broadcast(body) = pre_msg.body {
                results.insert(pre_msg.dest.as_ref().to_string(), body.message);
            } else {
                continue;
            }
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results.remove("n2"), Some(msg));
        assert_eq!(results.remove("n3"), Some(msg));
    }
}
