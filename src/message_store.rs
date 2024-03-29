use std::collections::HashMap;

use crate::primitives::MessageId;

/// Stores message ids to broadcast messages
#[derive(Debug)]
pub(crate) struct MsgStore {
    store: HashMap<MessageId, usize>,
}

impl MsgStore {
    /// Create a new [`MsgStore`]
    pub(crate) fn new() -> Self {
        Self {
            store: HashMap::with_capacity(1024),
        }
    }

    pub(crate) fn insert(
        &mut self,
        msg_id: MessageId,
        broadcast_msg: usize,
    ) {
        self.store.insert(msg_id, broadcast_msg);
    }

    pub(crate) fn get(
        &self,
        msg_id: &MessageId,
    ) -> Option<usize> {
        self.store.get(msg_id).copied()
    }
}
