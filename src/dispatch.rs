use tokio::sync::mpsc::Receiver;
use tracing::debug;
use tracing::error;

use crate::node::NODE_ID;
use crate::pre_message::PreMessage;
use crate::primitives::MessageCount;
use crate::protocol::Message;

pub(crate) struct MessageDispatch {
    message_queue_receiver: Receiver<PreMessage>,
    message_counter: usize,
}

impl MessageDispatch {
    pub(crate) fn new(rx: Receiver<PreMessage>) -> Self {
        Self {
            message_queue_receiver: rx,
            message_counter: 0,
        }
    }

    pub(crate) async fn run(&mut self) {
        loop {
            if let Some(pre_message) = self.message_queue_receiver.recv().await {
                let src = NODE_ID.get().expect("Node ID not yet initialized");
                // FIXME: avoid clone
                let msg: Message = (pre_message, src.clone(), self.message_count()).into();
                if let Ok(serialized_response) = serde_json::to_string(&msg) {
                    debug!("Sending message from {:?} to {:?}", src, msg.dest);
                    println!("{serialized_response}");
                } else {
                    error!("Could not serialize message: {:?}", msg);
                }
            }
        }
    }

    fn message_count(&mut self) -> MessageCount {
        self.message_counter += 1;
        MessageCount(self.message_counter)
    }
}
