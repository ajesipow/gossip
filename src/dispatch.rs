use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tracing::debug;

use crate::message_store::BroadcastMessageStore;
use crate::node::NODE_ID;
use crate::pre_message::PreMessage;
use crate::primitives::MessageId;
use crate::protocol::Message;
use crate::protocol::MessageBody;

pub(crate) struct MessageDispatcher {
    msg_dispatch_queue_rx: Receiver<Vec<PreMessage>>,
    broadcast_message_store: Arc<RwLock<BroadcastMessageStore>>,
    message_counter: usize,
    node_id: String,
}

impl MessageDispatcher {
    pub(crate) fn new(
        message_queue: Receiver<Vec<PreMessage>>,
        broadcast_message_store: Arc<RwLock<BroadcastMessageStore>>,
    ) -> Self {
        let node_id = NODE_ID
            .get()
            .expect("Node ID not yet initialized")
            .to_string();
        Self {
            msg_dispatch_queue_rx: message_queue,
            message_counter: 0,
            broadcast_message_store,
            node_id,
        }
    }

    pub(crate) async fn run(&mut self) {
        debug!("Running dispatch");
        loop {
            if let Some(queued_msgs) = self.msg_dispatch_queue_rx.recv().await {
                for msg in queued_msgs {
                    // FIXME: avoid clone
                    let msg: Message = (msg, self.node_id.clone(), self.next_message_id()).into();

                    let _ = serialize_and_send(&msg);
                    // Store the broadcast message by the ID so that we later know which broadcast
                    // message was acknowledged (because the Broadcast reply only
                    // contains the original message ID).
                    if let MessageBody::Broadcast(ref body) = msg.body {
                        self.broadcast_message_store
                            .write()
                            .await
                            .insert(msg.id(), body.message);
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

fn serialize_and_send(msg: &Message) -> Result<()> {
    if let Ok(serialized_response) = serde_json::to_string(&msg) {
        debug!("Sending message from {:?} to {:?}", msg.src, msg.dest);
        // Send to stdout
        println!("{serialized_response}");
        Ok(())
    } else {
        Err(anyhow!("Could not serialize message: {:?}", msg))
    }
}
