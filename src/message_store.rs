use std::cmp::max;
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;

use anyhow::anyhow;
use anyhow::Result;
use itertools::Itertools;

use crate::primitives::BroadcastMessage;
use crate::primitives::MessageId;

/// Stores broadcast messages of this node and peer nodes to keep track of
/// other node's state. This information can be used to limit the number of
/// broadcast messages that need to be (re-)sent to peers.
#[derive(Debug)]
pub(crate) struct BroadcastMessageStore {
    // Store the known broadcast messages of peer nodes
    peer_broadcast_msgs: HashMap<String, HashSet<BroadcastMessage>>,
    recent_peer_broadcast_msgs: HashMap<String, HashSet<BroadcastMessage>>,
    // This node's broadcast messages
    node_broadcast_msgs: HashSet<BroadcastMessage>,
    // Keep track of which broadcast message we sent with which message ID because
    // peers acknowledge a broadcast message by message ID.
    broadcast_msg_by_msg_id: HashMap<MessageId, BroadcastMessage>,
}

impl BroadcastMessageStore {
    pub(crate) fn new() -> Self {
        Self {
            peer_broadcast_msgs: HashMap::with_capacity(128),
            recent_peer_broadcast_msgs: HashMap::with_capacity(128),
            node_broadcast_msgs: HashSet::with_capacity(1024),
            broadcast_msg_by_msg_id: HashMap::with_capacity(1024),
        }
    }

    /// Registers a peer node in the store.
    pub(crate) fn register_peer(
        &mut self,
        peer: String,
    ) {
        self.recent_peer_broadcast_msgs.entry(peer).or_default();
    }

    /// Gets all broadcast messages this node has stored.
    pub(crate) fn msgs(&self) -> Vec<BroadcastMessage> {
        self.node_broadcast_msgs.iter().copied().collect()
    }

    /// Gets all peer nodes that have not yet acknowledged the given
    /// `broadcast_message` according to the state of the store.
    pub(crate) fn unacked_nodes(
        &self,
        broadcast_message: &BroadcastMessage,
    ) -> Vec<String> {
        self.peer_broadcast_msgs
            .iter()
            .chain(self.recent_peer_broadcast_msgs.iter())
            .filter_map(|(peer, msgs)| {
                if !msgs.contains(broadcast_message) {
                    Some(peer)
                } else {
                    None
                }
            })
            .unique()
            .cloned()
            .collect()
    }

    /// Gets all [BroadcastMessage]s this node is aware of that have not yet
    /// been acknowledged by peer nodes.
    pub(crate) fn unacked_nodes_all_msgs(&self) -> HashMap<String, HashSet<BroadcastMessage>> {
        let n_peers = max(
            self.peer_broadcast_msgs.len(),
            self.recent_peer_broadcast_msgs.len(),
        );
        self.peer_broadcast_msgs
            .iter()
            .chain(self.recent_peer_broadcast_msgs.iter())
            .fold(
                HashMap::<String, HashSet<BroadcastMessage>>::with_capacity(n_peers),
                |mut acc, (peer, acked_msgs)| {
                    acc.entry(peer.clone())
                        .or_default()
                        .extend(self.node_broadcast_msgs.difference(acked_msgs));
                    acc
                },
            )
    }

    /// Gets the broadcast messages by peer that have been inserted since the
    /// last time this method was called.
    pub(crate) fn recent_peer_inserts(&mut self) -> HashMap<String, HashSet<BroadcastMessage>> {
        let recents = mem::take(&mut self.recent_peer_broadcast_msgs);
        for (peer, recent_msgs) in &recents {
            if let Some(msgs) = self.peer_broadcast_msgs.get_mut(peer) {
                msgs.extend(recent_msgs);
            } else {
                // We could just use this implementation, but to avoid cloning peer if it's
                // already a key (which should be the case most of the time) we
                // use `get_mut` above first.
                self.peer_broadcast_msgs
                    .entry(peer.clone())
                    .or_default()
                    .extend(recent_msgs);
            }
        }
        recents
    }

    /// Inserts a [BroadcastMessage] for this node into the store.
    pub(crate) fn insert(
        &mut self,
        broadcast_msg: BroadcastMessage,
    ) {
        self.node_broadcast_msgs.insert(broadcast_msg);
    }

    /// Inserts a [BroadcastMessage] for an existing peer node into the store.
    /// Peer nodes need to be registered first with [Self::register_peer].
    pub(crate) fn insert_for_peer_if_exists(
        &mut self,
        peer_node: &str,
        broadcast_msg: BroadcastMessage,
    ) {
        if let Some(msgs) = self.recent_peer_broadcast_msgs.get_mut(peer_node) {
            msgs.insert(broadcast_msg);
        }
    }

    /// Inserts a [BroadcastMessage] for an existing peer by a [MessageId].
    /// Peer nodes need to be registered first with [Self::register_peer].
    /// The `msg_id` must have previously been registered with a broadcast
    /// message using [Self::register_msg_id].
    ///
    /// # Errors
    /// Returns an error if the `msg_id` is unknown (i.e. has not previously
    /// been registered via [Self::register_msg_id]).
    pub(crate) fn insert_for_peer_by_msg_id_if_exists(
        &mut self,
        peer_node: String,
        msg_id: &MessageId,
    ) -> Result<()> {
        let bdcast_msg = self
            .broadcast_msg_by_msg_id
            .get(msg_id)
            .ok_or_else(|| anyhow!("unknown message id"))?;
        self.recent_peer_broadcast_msgs
            .entry(peer_node)
            .or_default()
            .insert(*bdcast_msg);
        Ok(())
    }

    /// Registers a [MessageId] used to send a specific [BroadcastMessage].
    ///
    /// The [MessageId] can then be used to retrieve the associated
    /// [BroadcastMessage] with [`Self::get_by_msg_id()`].
    pub(crate) fn register_msg_id(
        &mut self,
        msg_id: MessageId,
        broadcast_msg: BroadcastMessage,
    ) {
        self.broadcast_msg_by_msg_id.insert(msg_id, broadcast_msg);
    }
}
