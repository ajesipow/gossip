use std::cmp::max;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;

use anyhow::anyhow;
use anyhow::Result;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::instrument;

use crate::primitives::BroadcastMessage;
use crate::primitives::MessageId;

#[derive(Debug, Clone)]
pub(crate) struct BroadcastMessageStoreHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl BroadcastMessageStoreHandle {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel(1024);
        let store = BroadcastMessageStore::new(rx);
        tokio::spawn(async move { run_message_store(store).await });

        Self { sender: tx }
    }

    #[instrument(skip(self))]
    pub(crate) async fn unacked_nodes_all_msgs(
        &self
    ) -> Result<HashMap<String, HashSet<BroadcastMessage>>> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .sender
            .send(ActorMessage::UnackedNodesAllMsgs { respond_to: tx })
            .await;
        let msgs = rx.await?;
        Ok(msgs)
    }

    #[instrument(skip(self))]
    pub(crate) async fn msgs(&self) -> Result<Vec<BroadcastMessage>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ActorMessage::AllMsgs { respond_to: tx })
            .await
            .unwrap();
        let msgs = rx.await?;
        Ok(msgs)
    }

    #[instrument(skip(self))]
    pub(crate) async fn recent_peer_inserts(
        &self
    ) -> Result<HashMap<String, HashSet<BroadcastMessage>>> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .sender
            .send(ActorMessage::RecentPeerInserts { respond_to: tx })
            .await;
        let msgs = rx.await?;
        Ok(msgs)
    }

    #[instrument(skip(self))]
    pub(crate) async fn insert(
        &self,
        broadcast_msg: BroadcastMessage,
    ) {
        self.sender
            .send(ActorMessage::Insert { broadcast_msg })
            .await
            .unwrap()
    }

    #[instrument(skip(self))]
    pub(crate) async fn insert_for_peer_by_msg_id_if_exists(
        &self,
        // TODO adapt type to other methods (make NodeId)
        peer_node: String,
        msg_id: MessageId,
    ) {
        let _ = self
            .sender
            .send(ActorMessage::InsertForPeerByMsgId { peer_node, msg_id })
            .await;
    }

    #[instrument(skip(self))]
    pub(crate) async fn register_msg_id(
        &self,
        msg_id: MessageId,
        broadcast_msg: BroadcastMessage,
    ) {
        let _ = self
            .sender
            .send(ActorMessage::RegisterMsgId {
                msg_id,
                broadcast_msg,
            })
            .await;
    }

    #[instrument(skip(self))]
    pub(crate) async fn register_peer(
        &self,
        peer: String,
    ) {
        let _ = self.sender.send(ActorMessage::RegisterPeer { peer }).await;
    }
}

async fn run_message_store(mut store: BroadcastMessageStore) {
    while let Some(actor_msg) = store.receiver.recv().await {
        store.handle_actor_message(actor_msg);
    }
}

/// Stores broadcast messages of this node and peer nodes to keep track of
/// other node's state. This information can be used to limit the number of
/// broadcast messages that need to be (re-)sent to peers.
#[derive(Debug)]
struct BroadcastMessageStore {
    receiver: mpsc::Receiver<ActorMessage>,
    // Store the known broadcast messages of peer nodes
    peer_broadcast_msgs: HashMap<String, HashSet<BroadcastMessage>>,
    recent_peer_broadcast_msgs: HashMap<String, HashSet<BroadcastMessage>>,
    // This node's broadcast messages
    node_broadcast_msgs: HashSet<BroadcastMessage>,
    // Keep track of which broadcast message we sent with which message ID because
    // peers acknowledge a broadcast message by message ID.
    broadcast_msg_by_msg_id: HashMap<MessageId, BroadcastMessage>,
}

#[derive(Debug)]
enum ActorMessage {
    AllMsgs {
        respond_to: oneshot::Sender<Vec<BroadcastMessage>>,
    },
    UnackedNodesAllMsgs {
        respond_to: oneshot::Sender<HashMap<String, HashSet<BroadcastMessage>>>,
    },
    RecentPeerInserts {
        respond_to: oneshot::Sender<HashMap<String, HashSet<BroadcastMessage>>>,
    },
    Insert {
        broadcast_msg: BroadcastMessage,
    },
    InsertForPeerByMsgId {
        peer_node: String,
        msg_id: MessageId,
    },
    RegisterMsgId {
        msg_id: MessageId,
        broadcast_msg: BroadcastMessage,
    },
    RegisterPeer {
        peer: String,
    },
}

impl BroadcastMessageStore {
    pub(crate) fn new(receiver: mpsc::Receiver<ActorMessage>) -> Self {
        Self {
            receiver,
            peer_broadcast_msgs: HashMap::with_capacity(128),
            recent_peer_broadcast_msgs: HashMap::with_capacity(128),
            node_broadcast_msgs: HashSet::with_capacity(1024),
            broadcast_msg_by_msg_id: HashMap::with_capacity(1024),
        }
    }

    fn handle_actor_message(
        &mut self,
        actor_message: ActorMessage,
    ) {
        match actor_message {
            ActorMessage::AllMsgs { respond_to } => {
                let _ = respond_to.send(self.msgs());
            }
            ActorMessage::UnackedNodesAllMsgs { respond_to } => {
                let _ = respond_to.send(self.unacked_nodes_all_msgs());
            }
            ActorMessage::RecentPeerInserts { respond_to } => {
                let _ = respond_to.send(self.recent_peer_inserts());
            }
            ActorMessage::Insert { broadcast_msg } => {
                self.insert(broadcast_msg);
            }
            ActorMessage::InsertForPeerByMsgId { peer_node, msg_id } => {
                // TODO handle error
                let _ = self.insert_for_peer_by_msg_id_if_exists(peer_node, &msg_id);
            }
            ActorMessage::RegisterMsgId {
                msg_id,
                broadcast_msg,
            } => {
                self.register_msg_id(msg_id, broadcast_msg);
            }
            ActorMessage::RegisterPeer { peer } => {
                self.register_peer(peer);
            }
        };
    }

    /// Registers a peer node in the store.
    #[instrument(skip(self))]
    fn register_peer(
        &mut self,
        peer: String,
    ) {
        self.recent_peer_broadcast_msgs.entry(peer).or_default();
    }

    /// Gets all broadcast messages this node has stored.
    #[instrument(skip(self))]
    fn msgs(&self) -> Vec<BroadcastMessage> {
        self.node_broadcast_msgs.iter().copied().collect()
    }

    /// Gets all peer nodes that have not yet acknowledged the given
    /// `broadcast_message` according to the state of the store.
    // #[instrument(skip(self))]
    // fn unacked_nodes(
    //     &self,
    //     broadcast_message: &BroadcastMessage,
    // ) -> HashSet<String> {
    //     let records = self
    //         .peer_broadcast_msgs
    //         .iter()
    //         .chain(self.recent_peer_broadcast_msgs.iter())
    //         .fold(PeerRecords::new(), |mut acc, (peer, msgs)| {
    //             if !msgs.contains(broadcast_message) {
    //                 acc.keep.insert(peer);
    //             } else {
    //                 // Since we chain the two hashmaps we don't want to count the peer if the msg
    // is                 // contained in just one of the two maps.
    //                 acc.remove.insert(peer);
    //             }
    //             acc
    //         });
    //     records
    //         .keep
    //         .difference(&records.remove)
    //         .map(|s| s.to_string())
    //         .collect()
    // }

    /// Gets all [BroadcastMessage]s this node is aware of that have not yet
    /// been acknowledged by peer nodes.
    #[instrument(skip(self))]
    fn unacked_nodes_all_msgs(&self) -> HashMap<String, HashSet<BroadcastMessage>> {
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
                    let diff = self
                        .node_broadcast_msgs
                        .difference(acked_msgs)
                        .copied()
                        .collect();
                    match acc.entry(peer.clone()) {
                        Entry::Occupied(mut v) => {
                            let m = v.get_mut();
                            *m = m.intersection(&diff).copied().collect();
                        }
                        Entry::Vacant(v) => {
                            v.insert(diff);
                        }
                    }
                    acc
                },
            )
    }

    /// Gets the broadcast messages by peer that have been inserted since the
    /// last time this method was called.
    #[instrument(skip(self))]
    fn recent_peer_inserts(&mut self) -> HashMap<String, HashSet<BroadcastMessage>> {
        let recents = mem::take(&mut self.recent_peer_broadcast_msgs);
        for (peer, recent_msgs) in &recents {
            // Insert peers back into recent map otherwise inserting will not work
            self.recent_peer_broadcast_msgs
                .entry(peer.clone())
                .or_default();

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
    #[instrument(skip(self))]
    fn insert(
        &mut self,
        broadcast_msg: BroadcastMessage,
    ) {
        self.node_broadcast_msgs.insert(broadcast_msg);
    }

    /// Inserts a [BroadcastMessage] for an existing peer node into the store.
    /// Peer nodes need to be registered first with [Self::register_peer].
    #[instrument(skip(self))]
    fn insert_for_peer_if_exists(
        &mut self,
        peer_node: &str,
        broadcast_msg: BroadcastMessage,
    ) -> Result<()> {
        if let Some(msgs) = self.recent_peer_broadcast_msgs.get_mut(peer_node) {
            msgs.insert(broadcast_msg);
            Ok(())
        } else {
            Err(anyhow!("Did not find peer {peer_node}"))
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
    #[instrument(skip(self))]
    fn insert_for_peer_by_msg_id_if_exists(
        &mut self,
        peer_node: String,
        msg_id: &MessageId,
    ) -> Result<()> {
        let bdcast_msg = self
            .broadcast_msg_by_msg_id
            .get(msg_id)
            .ok_or_else(|| anyhow!("unknown message id"))?;
        self.insert_for_peer_if_exists(&peer_node, *bdcast_msg)
    }

    /// Registers a [MessageId] used to send a specific [BroadcastMessage].
    ///
    /// The [MessageId] can then be used to retrieve the associated
    /// [BroadcastMessage] with [`Self::get_by_msg_id()`].
    #[instrument(skip(self))]
    fn register_msg_id(
        &mut self,
        msg_id: MessageId,
        broadcast_msg: BroadcastMessage,
    ) {
        self.broadcast_msg_by_msg_id.insert(msg_id, broadcast_msg);
    }
}

// /// Helper struct for keeping track of peers to retain and remove below
// struct PeerRecords<'a> {
//     keep: HashSet<&'a String>,
//     remove: HashSet<&'a String>,
// }
//
// impl<'a> PeerRecords<'a> {
//     fn new() -> Self {
//         Self {
//             keep: HashSet::new(),
//             remove: HashSet::new(),
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inserting_and_getting_broadcast_messages_for_own_node_works() {
        let (_tx, rx) = mpsc::channel(1);
        let mut store = BroadcastMessageStore::new(rx);

        let msgs: HashSet<_> = (0..5).map(BroadcastMessage::new).collect();
        for msg in &msgs {
            store.insert(*msg);
        }

        let stored_msgs: HashSet<_> = store.msgs().into_iter().collect();
        assert_eq!(stored_msgs, msgs);
    }

    #[test]
    fn test_registering_and_retrieving_broadcast_msg_by_msg_id() {
        let (_tx, rx) = mpsc::channel(1);
        let mut store = BroadcastMessageStore::new(rx);
        let broadcast_msg_1 = BroadcastMessage::new(1);
        let broadcast_msg_2 = BroadcastMessage::new(2);
        let msg_id_1 = MessageId::new();
        let msg_id_2 = MessageId::new();
        let peer = "n1".to_string();

        store.register_peer(peer.clone());
        store.register_msg_id(msg_id_1, broadcast_msg_1);
        assert!(store
            .insert_for_peer_by_msg_id_if_exists(peer.clone(), &msg_id_1)
            .is_ok());
        assert_eq!(
            store.recent_peer_inserts(),
            HashMap::from_iter([(peer.clone(), HashSet::from_iter([broadcast_msg_1]))])
        );

        store.register_msg_id(msg_id_2, broadcast_msg_2);
        assert!(store
            .insert_for_peer_by_msg_id_if_exists(peer.clone(), &msg_id_2)
            .is_ok());
        assert_eq!(
            store.recent_peer_inserts(),
            HashMap::from_iter([(peer, HashSet::from_iter([broadcast_msg_2]))])
        );
    }

    #[test]
    fn test_registering_and_retrieving_broadcast_msg_by_msg_id_does_not_insert_for_unregistered_peer(
    ) {
        let (_tx, rx) = mpsc::channel(1);
        let mut store = BroadcastMessageStore::new(rx);
        let broadcast_msg = BroadcastMessage::new(1);
        let msg_id = MessageId::new();
        let peer = "n1".to_string();

        store.register_msg_id(msg_id, broadcast_msg);
        assert!(store
            .insert_for_peer_by_msg_id_if_exists(peer.clone(), &msg_id)
            .is_err());
        assert_eq!(store.recent_peer_inserts(), HashMap::new());
    }

    #[test]
    fn test_insert_for_peer_if_exists_works() {
        let (_tx, rx) = mpsc::channel(1);
        let mut store = BroadcastMessageStore::new(rx);
        let broadcast_msg_1 = BroadcastMessage::new(1);
        let broadcast_msg_2 = BroadcastMessage::new(2);
        let peer = "n1".to_string();

        store.register_peer(peer.clone());
        assert!(store
            .insert_for_peer_if_exists(&peer, broadcast_msg_1)
            .is_ok());
        assert_eq!(
            store.recent_peer_inserts(),
            HashMap::from_iter([(peer.clone(), HashSet::from_iter([broadcast_msg_1]))])
        );

        assert!(store
            .insert_for_peer_if_exists(&peer, broadcast_msg_2)
            .is_ok());
        assert_eq!(
            store.recent_peer_inserts(),
            HashMap::from_iter([(peer, HashSet::from_iter([broadcast_msg_2]))])
        );
    }

    #[test]
    fn test_insert_for_peer_if_exists_does_not_insert_if_peer_not_registered() {
        let (_tx, rx) = mpsc::channel(1);
        let mut store = BroadcastMessageStore::new(rx);
        let broadcast_msg = BroadcastMessage::new(1);
        let peer = "n1".to_string();

        assert!(store
            .insert_for_peer_if_exists(&peer, broadcast_msg)
            .is_err());
        assert_eq!(store.recent_peer_inserts(), HashMap::new());
    }

    #[test]
    fn test_insert_does_not_leek_to_peers() {
        let (_tx, rx) = mpsc::channel(1);
        let mut store = BroadcastMessageStore::new(rx);

        let msgs: HashSet<_> = (0..5).map(BroadcastMessage::new).collect();
        for msg in &msgs {
            store.insert(*msg);
        }
        assert_eq!(store.recent_peer_inserts(), HashMap::new());
    }

    #[test]
    fn test_recent_peer_inserts_works() {
        let (_tx, rx) = mpsc::channel(1);
        let mut store = BroadcastMessageStore::new(rx);
        let peer = "n1".to_string();
        store.register_peer(peer.clone());
        let msgs: HashSet<_> = (0..5).map(BroadcastMessage::new).collect();
        for msg in &msgs {
            assert!(store.insert_for_peer_if_exists(&peer, *msg).is_ok());
        }
        assert_eq!(
            store.recent_peer_inserts(),
            HashMap::from_iter([(peer.clone(), msgs)])
        );
        assert_eq!(
            store.recent_peer_inserts(),
            HashMap::from_iter([(peer, HashSet::new())])
        );
    }

    #[test]
    fn test_unacked_nodes_works() {
        let (_tx, rx) = mpsc::channel(1);
        let mut store = BroadcastMessageStore::new(rx);
        let peer_1 = "n1".to_string();
        let peer_2 = "n2".to_string();
        let peer_3 = "n3".to_string();
        store.register_peer(peer_1.clone());
        store.register_peer(peer_2.clone());
        store.register_peer(peer_3.clone());

        assert_eq!(
            store.recent_peer_inserts(),
            HashMap::from_iter([
                (peer_1.clone(), HashSet::new()),
                (peer_2.clone(), HashSet::new()),
                (peer_3.clone(), HashSet::new())
            ])
        );

        let msg_1 = BroadcastMessage::new(1);
        let msg_2 = BroadcastMessage::new(2);
        store.insert(msg_1);
        store.insert(msg_2);
        let _ = store.insert_for_peer_if_exists(&peer_1, msg_1);
        let _ = store.insert_for_peer_if_exists(&peer_3, msg_1);
        let _ = store.insert_for_peer_if_exists(&peer_3, msg_2);

        assert_eq!(
            store.unacked_nodes_all_msgs(),
            HashMap::from_iter([
                (peer_1.clone(), HashSet::from_iter([msg_2])),
                (peer_2.clone(), HashSet::from_iter([msg_1, msg_2])),
                (peer_3.clone(), HashSet::new())
            ])
        );

        assert_eq!(
            store.recent_peer_inserts(),
            HashMap::from_iter([
                (peer_1.clone(), HashSet::from_iter([msg_1])),
                (peer_2.clone(), HashSet::new()),
                (peer_3.clone(), HashSet::from_iter([msg_1, msg_2]))
            ])
        );

        assert_eq!(
            store.unacked_nodes_all_msgs(),
            HashMap::from_iter([
                (peer_1.clone(), HashSet::from_iter([msg_2])),
                (peer_2.clone(), HashSet::from_iter([msg_1, msg_2])),
                (peer_3.clone(), HashSet::new())
            ])
        );
    }
}
