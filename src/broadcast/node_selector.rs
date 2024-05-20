use rand::seq::index::sample;

use crate::primitives::NodeId;
use crate::topology::Topology;

/// Select nodes from a topology.
#[derive(Debug, Clone)]
pub(crate) struct NodeSelector {
    all_nodes: Vec<NodeId>,
}

impl NodeSelector {
    /// Creates a new [`NodeSelector`] instance.
    pub(crate) fn new(topology: Topology) -> Self {
        let all_nodes = topology.all_nodes();
        Self { all_nodes }
    }

    /// Selects `n` nodes from the topology excluding the current node.
    pub(crate) fn select(
        &self,
        n: usize,
    ) -> Vec<NodeId> {
        let mut rng = rand::thread_rng();
        let idcs = sample(&mut rng, self.all_nodes.len(), n);
        idcs.iter()
            .filter_map(|idx| self.all_nodes.get(idx))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashSet;

    use crate::broadcast::node_selector::NodeSelector;
    use crate::primitives::NodeId;
    use crate::topology::Topology;

    #[test]
    fn test_sampling_contains_only_unique_nodes_and_not_self() {
        let this_node = "n1".to_string();
        let topology: Topology = (
            BTreeMap::from_iter([
                ("n1".to_string(), vec!["n2".to_string(), "n3".to_string()]),
                ("n2".to_string(), vec!["n1".to_string()]),
                ("n3".to_string(), vec!["n1".to_string()]),
            ]),
            this_node.clone(),
        )
            .into();
        let selector = NodeSelector::new(topology);

        let this_node_id = NodeId::from(this_node);

        // Ensure no flakiness
        for _ in 0..10 {
            let nodes = selector.select(2).into_iter().collect::<HashSet<_>>();
            assert_eq!(nodes.len(), 2);
            assert!(!nodes.contains(&this_node_id));
        }
    }
}
