use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tracing::debug;

use crate::message_store::MsgStore;
use crate::message_handling::QueuedMessage;
use crate::node::NODE_ID;
use crate::primitives::MessageId;
use crate::protocol::Message;
use crate::protocol::MessageBody;
use crate::retry::RetryMessage;

pub(crate) struct MessageDispatcher {
    msg_dispatch_queue_rx: Receiver<QueuedMessage>,
    retry_queue_tx: Sender<RetryMessage>,
    message_counter: usize,
    neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
    msg_store: Arc<RwLock<MsgStore>>,
}

impl MessageDispatcher {
    pub(crate) fn new(
        pre_message_queue: Receiver<QueuedMessage>,
        retry_queue_tx: Sender<RetryMessage>,
        neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
        msg_store: Arc<RwLock<MsgStore>>,
    ) -> Self {
        Self {
            msg_dispatch_queue_rx: pre_message_queue,
            retry_queue_tx,
            message_counter: 0,
            neighbour_broadcast_messages,
            msg_store,
        }
    }

    pub(crate) async fn run(&mut self) {
        debug!("Running dispatch");
        loop {
            if let Some(queued_msg) = self.msg_dispatch_queue_rx.recv().await {
                match queued_msg {
                    QueuedMessage::Initial(pre_message) => {
                        debug!("Received queued initial mesage");
                        let src = NODE_ID.get().expect("Node ID not yet initialized");
                        // FIXME: avoid clone
                        let msg: Message =
                            (pre_message, src.clone(), self.next_message_id()).into();
                        let _ =
                            serialize_and_send(msg, &self.retry_queue_tx, &self.msg_store).await;
                    }
                    QueuedMessage::ForRetry(msg) => {
                        debug!("Received queued retry message");
                        // Check if the message has been acknowledged before retry sending.
                        // It could be that the reply was received when the message was in the
                        // retry queue or the retry handler did not check itself before scheduling
                        // the message for retry.
                        let lock = self.neighbour_broadcast_messages.read().await;
                        if let MessageBody::Broadcast(body) = &msg.body {
                            let has_recipient_already_received_broadcast_message = lock
                                .get(&msg.dest.0)
                                .map(|msgs| msgs.contains(&body.message))
                                .unwrap_or(false);
                            drop(lock);
                            if !has_recipient_already_received_broadcast_message {
                                debug!(
                                    "Broadcast message {:?} not yet acked, retrying.",
                                    body.message
                                );
                                let _ =
                                    serialize_and_send(msg, &self.retry_queue_tx, &self.msg_store)
                                        .await;
                            } else {
                                debug!(
                                    "Broadcast message {:?} already acked, not retrying.",
                                    body.message
                                );
                            }
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

async fn serialize_and_send(
    msg: Message,
    retry_queue_tx: &Sender<RetryMessage>,
    msg_store: &Arc<RwLock<MsgStore>>,
) -> Result<()> {
    if let Ok(serialized_response) = serde_json::to_string(&msg) {
        debug!("Sending message from {:?} to {:?}", msg.src, msg.dest);
        // Send to stdout
        println!("{serialized_response}");

        // All outgoing messages are marked for retry here, the retry handler will take
        // care of everything else
        let msg_id = msg.id();
        if let MessageBody::Broadcast(body) = &msg.body {
            msg_store.write().await.insert(msg_id, body.message);
            retry_queue_tx.send(RetryMessage::new(msg)).await?;
            debug!("Message {:?} scheduled for retry", msg_id);
        }
        Ok(())
    } else {
        Err(anyhow!("Could not serialize message: {:?}", msg))
    }
}
