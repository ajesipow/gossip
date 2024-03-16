use std::sync::Arc;

use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;

use crate::ack_store::AckStore;
use crate::node::NODE_ID;
use crate::pre_message::PreMessage;
use crate::primitives::MessageId;
use crate::protocol::Message;

pub(crate) struct MessageDispatcher {
    premessage_queue_receiver: Receiver<PreMessage>,
    retry_queue_receiver: Receiver<Message>,
    message_counter: usize,
    ack_store: Arc<RwLock<AckStore>>,
}

impl MessageDispatcher {
    pub(crate) fn new(
        pre_message_queue: Receiver<PreMessage>,
        retry_queue: Receiver<Message>,
        ack_store: Arc<RwLock<AckStore>>,
    ) -> Self {
        Self {
            premessage_queue_receiver: pre_message_queue,
            retry_queue_receiver: retry_queue,
            message_counter: 0,
            ack_store,
        }
    }

    pub(crate) async fn run(&mut self) {
        loop {
            tokio::select! {
                v = self.premessage_queue_receiver.recv() => {
                    if let Some(pre_message) = v {
                        let src = NODE_ID.get().expect("Node ID not yet initialized");
                        // FIXME: avoid clone
                        let msg: Message = (pre_message, src.clone(), self.next_message_id()).into();
                        serialize_and_send(&msg);
                    }
                }
                v = self.retry_queue_receiver.recv() => {
                    if let Some(msg) = v {
                        // Check if the message has been acknowledged before retry sending.
                        // It could be that the reply was received when the message was in the
                        // retry queue or the retry handler did not check itself before scheduling
                        // the message for retry.
                        let lock = self.ack_store.read().await;
                        let is_acked = lock.is_acked(&msg.id());
                        drop(lock);
                        if !is_acked {
                            serialize_and_send(&msg);
                        }
                    }
                }
            }
        }
    }

    fn next_message_id(&mut self) -> MessageId {
        self.message_counter += 1;
        MessageId(self.message_counter)
    }
}

fn serialize_and_send(msg: &Message) {
    if let Ok(serialized_response) = serde_json::to_string(msg) {
        debug!("Sending message from {:?} to {:?}", msg.src, msg.dest);
        println!("{serialized_response}");
    } else {
        error!("Could not serialize message: {:?}", msg);
    }
}
