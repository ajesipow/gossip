use std::collections::HashSet;

use crate::primitives::MessageId;

/// Stores message acknowledgments, i.e. when we received a reply for a message
/// id.
#[derive(Debug)]
pub(crate) struct AckStore {
    // If a message id is contained in the store, it means it has been acknowledged.
    store: HashSet<MessageId>,
}

impl AckStore {
    /// Create a new [`AckStore`]
    pub(crate) fn new() -> Self {
        Self {
            store: HashSet::with_capacity(1024),
        }
    }

    /// Mark a message as acknowledged by its ID.
    pub(crate) fn ack_message_id(
        &mut self,
        message_id: MessageId,
    ) {
        self.store.insert(message_id);
    }

    /// Query if a message has been acknowledged.
    pub(crate) fn is_acked(
        &self,
        id: &MessageId,
    ) -> bool {
        self.store.contains(id)
    }
}
