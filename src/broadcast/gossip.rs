use std::collections::HashMap;
use std::collections::HashSet;

use itertools::Itertools;
use thingbuf::mpsc::Sender;
use tracing::debug;

use crate::broadcast::node_selector::NodeSelector;
use crate::node::NODE_ID;
use crate::pre_message::PreMessage;
use crate::primitives::BroadcastMessage;
use crate::topology::Topology;

/// An implementation of a simple probabilistic gossip protocol.
#[derive(Debug, Clone)]
pub(crate) struct GossipBroadcast {
    // The number of nodes to broadcast to per round
    fanout: Fanout,
    // Max number of rounds we broadcast each message
    max_broadcast_rounds: usize,
    node_selector: Option<NodeSelector>,
    // Messages actively broadcast mapped to the number of rounds they have already been
    // broadcast. One round usually broadcasts to more than one node.
    infectious_msgs: HashMap<BroadcastMessage, usize>,
    // Messages that are not broadcast any longer
    removed_msgs: HashSet<BroadcastMessage>,
    msg_dispatch_queue_tx: Sender<Vec<PreMessage>>,
}

// TODO: impl Default, impl Builder, config?
// TODO policy
impl GossipBroadcast {
    pub(crate) fn new(
        topology: Option<Topology>,
        fanout: Fanout,
        max_broadcast_rounds: usize,
        msg_dispatch_queue_tx: Sender<Vec<PreMessage>>,
    ) -> Self {
        Self {
            fanout,
            max_broadcast_rounds,
            node_selector: topology.map(NodeSelector::new),
            infectious_msgs: HashMap::new(),
            removed_msgs: HashSet::new(),
            msg_dispatch_queue_tx,
        }
    }

    pub(crate) fn topology(
        &mut self,
        topology: Topology,
    ) {
        self.node_selector = Some(NodeSelector::new(topology))
    }

    /// Adds a message to the infectious broadcast messages if it has not
    /// already been broadcast.
    pub(crate) fn add(
        &mut self,
        broadcast_message: BroadcastMessage,
    ) {
        if !self.infectious_msgs.contains_key(&broadcast_message)
            && !self.removed_msgs.contains(&broadcast_message)
        {
            self.infectious_msgs.insert(broadcast_message, 0);
        }
    }

    /// Triggers a broadcast round for all infectious broadcast messages.
    pub(crate) async fn broadcast(&mut self) {
        let Some(node_selector) = &self.node_selector else {
            return;
        };
        for (msg, broadcast_round) in self.infectious_msgs.iter_mut() {
            *broadcast_round += 1;
            if *broadcast_round >= self.max_broadcast_rounds {
                self.removed_msgs.insert(*msg);
                continue;
            }
            let nodes = node_selector.select(self.fanout.0);
            debug!(
                "{:?} infecting nodes {:?} with {:?}",
                NODE_ID.get().unwrap(),
                nodes,
                msg
            );
            let msgs = nodes
                .into_iter()
                .map(|node| PreMessage::broadcast(node.into(), *msg, HashSet::new()))
                .collect_vec();
            // FIXME AJES: proper error handling
            self.msg_dispatch_queue_tx.send(msgs).await.unwrap();
        }
        self.infectious_msgs
            .retain(|_, broadcasts| *broadcasts < self.max_broadcast_rounds);
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
    use std::collections::HashMap;

    use super::*;
    use crate::pre_message::PreMessageBody;
    use crate::topology::Topology;

    #[tokio::test]
    async fn test_basic_broadcast_works() {
        let topology: Topology = (
            HashMap::from_iter([
                ("n1".to_string(), vec!["n2".to_string(), "n3".to_string()]),
                ("n2".to_string(), vec!["n1".to_string()]),
                ("n3".to_string(), vec!["n1".to_string()]),
            ]),
            "n1".to_string(),
        )
            .into();

        let msg = BroadcastMessage::new(1);

        let (tx, rx) = thingbuf::mpsc::channel(8);
        let mut gossip = GossipBroadcast::new(None, Fanout::new(2), 2, tx);

        gossip.topology(topology);
        gossip.add(msg);

        gossip.broadcast().await;

        // Just one because we send multiple messages as a vec at once
        assert_eq!(rx.len(), 1);

        let mut results = HashMap::new();
        let res = rx.recv().await.unwrap();
        for pre_msg in res {
            if let PreMessageBody::Broadcast(body) = pre_msg.body {
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
