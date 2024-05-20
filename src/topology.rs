use std::collections::BTreeMap;
use std::collections::HashMap;

use itertools::Itertools;

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
