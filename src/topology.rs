use std::collections::BTreeMap;
use std::collections::HashMap;

use itertools::Itertools;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tracing::error;
use tracing::instrument;
use tracing::warn;

use crate::primitives::NodeId;

/// The overlay network topology.
#[derive(Debug, Clone)]
pub(crate) struct Topology {
    current_node: NodeId,
    raw_topology: HashMap<NodeId, Vec<NodeId>>,
}

impl Topology {
    /// Get all nodes in the topology, excluding the current node.
    pub(crate) fn all_nodes(&self) -> Vec<NodeId> {
        self.raw_topology
            .keys()
            .filter(|node| **node != self.current_node)
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    /// Provides the current node's direct neighbours in the established overlay
    /// network.
    pub(crate) fn overlay_neighbours(&self) -> Vec<NodeId> {
        self.raw_topology
            .get(&self.current_node)
            .cloned()
            .unwrap_or_default()
    }
}

impl From<(BTreeMap<String, Vec<String>>, String)> for Topology {
    fn from((value, node_id): (BTreeMap<String, Vec<String>>, String)) -> Self {
        Self {
            current_node: node_id.into(),
            raw_topology: value
                .into_iter()
                .map(|(k, v)| (k.into(), v.into_iter().map_into().collect()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyStoreHandle {
    sender: mpsc::Sender<TopologyStoreMessage>,
}

impl TopologyStoreHandle {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        let store = TopologyStore::new(receiver);
        tokio::spawn(async move { run_topology_store(store).await });
        Self { sender }
    }

    #[instrument(skip(self))]
    pub(crate) async fn update(
        &self,
        topology: Topology,
    ) {
        if self
            .sender
            .send(TopologyStoreMessage::Update { topology })
            .await
            .is_err()
        {
            error!("updating topology failed!");
        }
    }

    #[instrument(skip(self))]
    pub(crate) async fn topology(&self) -> Option<Topology> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self
            .sender
            .send(TopologyStoreMessage::Get { reply_to: tx })
            .await
        {
            error!("sending to get topology failed: {:?}", e);
        }

        // TODO handle errors
        let res = rx.await;
        if let Err(e) = res {
            error!("receiving to get topology failed: {:?}", e);
            None
        } else {
            res.unwrap()
        }
    }
}

#[derive(Debug)]
struct TopologyStore {
    receiver: Receiver<TopologyStoreMessage>,
    topology: Option<Topology>,
}

#[derive(Debug)]
enum TopologyStoreMessage {
    Update {
        topology: Topology,
    },
    Get {
        reply_to: oneshot::Sender<Option<Topology>>,
    },
}

async fn run_topology_store(mut topology_store: TopologyStore) {
    while let Some(msg) = topology_store.receiver.recv().await {
        warn!("received topology msg {:?}", msg);
        topology_store.handle_msg(msg);
    }
}

impl TopologyStore {
    fn new(receiver: Receiver<TopologyStoreMessage>) -> Self {
        Self {
            receiver,
            topology: None,
        }
    }

    fn update(
        &mut self,
        topology: Topology,
    ) {
        self.topology = Some(topology)
    }

    fn handle_msg(
        &mut self,
        msg: TopologyStoreMessage,
    ) {
        match msg {
            TopologyStoreMessage::Update { topology } => {
                self.update(topology);
            }
            TopologyStoreMessage::Get { reply_to } => {
                if reply_to.send(self.topology.clone()).is_err() {
                    error!("cannot send get topology")
                }
            }
        }
    }
}
