use thiserror::Error;

use crate::primitives::NodeId;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Topology(#[from] TopologyError),
}

#[derive(Debug, Error)]
pub(crate) enum TopologyError {
    #[error("cannot update topology: {0}")]
    Update(String),
    #[error("could not find the current Node `{0}` in topology")]
    NodeNotFound(NodeId),
}
