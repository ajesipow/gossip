use std::collections::HashMap;

use crate::primitives::MessageId;

/// Stores broadcast messages by a message ID so that we can know which
/// broadcast message has been acknowledged when receiving a broadcast reply.
#[derive(Debug)]
pub(crate) struct BroadcastMessageStore {
    broadcast_msg_by_msg_id: HashMap<MessageId, usize>,
}

impl BroadcastMessageStore {
    pub(crate) fn new() -> Self {
        Self {
            broadcast_msg_by_msg_id: HashMap::with_capacity(1024),
        }
    }

    pub(crate) fn insert(
        &mut self,
        msg_id: MessageId,
        broadcast_msg: usize,
    ) {
        self.broadcast_msg_by_msg_id.insert(msg_id, broadcast_msg);
    }

    pub(crate) fn remove(
        &mut self,
        msg_id: &MessageId,
    ) -> Option<usize> {
        self.broadcast_msg_by_msg_id.remove(msg_id)
    }
}
