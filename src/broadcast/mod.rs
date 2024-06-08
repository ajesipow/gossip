use anyhow::Result;
use async_trait::async_trait;

use crate::primitives::BroadcastMessage;
use crate::primitives::MessageId;
use crate::primitives::NodeId;
use crate::topology::Topology;

pub(crate) mod gossip;
pub(crate) mod node_selector;

/// Broadcast messages to other nodes in the network.
#[async_trait]
pub(crate) trait Broadcast: Send + Clone {
    /// Get this node's broadcast messages
    async fn messages(&self) -> Result<Vec<BroadcastMessage>>;

    /// Update the topology
    async fn update_topology(
        &self,
        topology: Topology,
    );

    /// Register the acknowledgement of a broadcast message by a peer node via a
    /// message id
    async fn ack_by_msg_id(
        &self,
        node: NodeId,
        msg_id: MessageId,
    );

    /// Broadcast a message to peers
    async fn broadcast(
        &self,
        msg: BroadcastMessage,
    );
}
