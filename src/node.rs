use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::vec;

use anyhow::Result;
use once_cell::sync::OnceCell;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tracing::error;

use crate::dispatch::MessageDispatch;
use crate::pre_message::BroadcastOkPreBody;
use crate::pre_message::BroadcastPreBody;
use crate::pre_message::EchoOkPreBody;
use crate::pre_message::InitOkPreBody;
use crate::pre_message::PreMessage;
use crate::pre_message::PreMessageBody;
use crate::pre_message::ReadOkPreBody;
use crate::pre_message::TopologyOkPreBody;
use crate::primitives::MessageRecipient;
use crate::protocol::Message;
use crate::protocol::MessageBody;
use crate::transport::StdInTransport;

pub(crate) static NODE_ID: OnceCell<String> = OnceCell::new();

/// A node representing a server
#[derive(Debug)]
pub(crate) struct Node {
    transport: StdInTransport,
    dispatch_queue: Sender<PreMessage>,
    broadcast_messages: Arc<RwLock<HashSet<usize>>>,
    // The broadcast messages we sent to or received from our neighbours
    neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
}

impl Node {
    /// Creates a new node.
    /// The node can only be initialised with an init message received via .
    ///
    /// # Panics
    /// An init message is expected for creating the node. This method will
    /// panic if the message could not be read or is of a different type.
    pub async fn new() -> Self {
        let mut transport = StdInTransport::new();

        let init_msg = transport
            .read_message()
            .await
            .expect("be able to read init message");

        let MessageBody::Init(ref init_body) = init_msg.body else {
            panic!("expected init message, got: {:?}", init_msg.body)
        };
        NODE_ID
            .set(init_body.node_id.clone())
            .expect("Node ID already set");

        let (tx, rx) = mpsc::channel::<PreMessage>(10);
        let mut message_sender = MessageDispatch::new(rx);
        tokio::spawn(async move {
            message_sender.run().await;
        });

        let broadcast_messages = Arc::new(RwLock::new(HashSet::new()));
        let neighbour_broadcast_messages = Arc::new(RwLock::new(HashMap::new()));

        let msgs = handle_message(
            init_msg,
            broadcast_messages.clone(),
            neighbour_broadcast_messages.clone(),
        )
        .await;
        for msg in msgs {
            tx.send(msg).await.expect("be able to send init message");
        }

        Self {
            dispatch_queue: tx,
            transport,
            broadcast_messages,
            neighbour_broadcast_messages,
        }
    }

    /// Run the node.
    /// Lets the node read and respond to incoming messages.
    ///
    /// # Errors
    /// Throws an error if the message cannot be read, sent or is of an unknown
    /// type.
    pub async fn run(&mut self) -> Result<()> {
        loop {
            // TODO: Handle the error differently here
            let msg = self.transport.read_message().await?;
            let tx = self.dispatch_queue.clone();
            let broadcast_messages = self.broadcast_messages.clone();
            let neighbour_broadcast_messages = self.neighbour_broadcast_messages.clone();

            tokio::spawn(async move {
                let responses =
                    handle_message(msg, broadcast_messages, neighbour_broadcast_messages).await;
                for msg in responses {
                    let _ = tx.send(msg).await;
                }
            });
        }
    }
}

/// Handle incoming messages and return an appropriate response.
async fn handle_message(
    message: Message,
    broadcast_messages: Arc<RwLock<HashSet<usize>>>,
    neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
) -> Vec<PreMessage> {
    let src = message.src;
    match message.body {
        MessageBody::Echo(body) => vec![PreMessage::new(
            MessageRecipient(src),
            PreMessageBody::EchoOk(EchoOkPreBody {
                echo: body.echo,
                in_reply_to: body.msg_id,
            }),
        )],
        MessageBody::EchoOk(_) => Default::default(),
        MessageBody::Init(body) => vec![PreMessage::new(
            MessageRecipient(src),
            PreMessageBody::InitOk(InitOkPreBody {
                in_reply_to: body.msg_id,
            }),
        )],
        MessageBody::InitOk(_) => Default::default(),
        MessageBody::Broadcast(body) => {
            let mut messages = vec![];

            let broadcast_message = body.message;
            let mut broadcast_messages_lock = broadcast_messages.write().await;
            broadcast_messages_lock.insert(broadcast_message);
            drop(broadcast_messages_lock);

            // Record which message we already received from another node so we don't
            // unnecessarily send it back again.
            let mut neighbour_broadcast_messages_lock = neighbour_broadcast_messages.write().await;
            neighbour_broadcast_messages_lock
                .entry(src.clone())
                .or_default()
                .insert(broadcast_message);
            drop(neighbour_broadcast_messages_lock);

            let neighbour_broadcast_messages_lock = neighbour_broadcast_messages.read().await;
            let recipients: Vec<MessageRecipient> = neighbour_broadcast_messages_lock
                .iter()
                .filter_map(|(neighbour, messages)| {
                    if &src != neighbour && !messages.contains(&broadcast_message) {
                        // FIXME: avoid clone
                        Some(MessageRecipient(neighbour.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            drop(neighbour_broadcast_messages_lock);

            messages.push(PreMessage::new(
                MessageRecipient(src),
                PreMessageBody::BroadcastOk(BroadcastOkPreBody {
                    in_reply_to: body.msg_id,
                }),
            ));

            messages.extend(recipients.into_iter().map(|recipient| {
                PreMessage::new(
                    recipient,
                    PreMessageBody::Broadcast(BroadcastPreBody {
                        message: broadcast_message,
                    }),
                )
            }));

            messages
        }
        MessageBody::BroadcastOk(_) => Default::default(),
        MessageBody::Read(body) => {
            let messages = broadcast_messages.read().await;
            vec![PreMessage::new(
                MessageRecipient(src),
                PreMessageBody::ReadOk(ReadOkPreBody {
                    messages: messages.iter().copied().collect(),
                    in_reply_to: body.msg_id,
                }),
            )]
        }
        MessageBody::ReadOk(_) => Default::default(),
        MessageBody::Topology(mut body) => {
            if let Some(node_id) = NODE_ID.get() {
                if let Some(neighbours) = body.topology.remove(node_id) {
                    let mut neighbour_broadcast_messages_lock =
                        neighbour_broadcast_messages.write().await;
                    for neighbour in neighbours {
                        neighbour_broadcast_messages_lock
                            .entry(neighbour)
                            .or_default();
                    }
                    drop(neighbour_broadcast_messages_lock);
                }
            } else {
                error!("NODE_ID uninitialised!")
            }

            vec![PreMessage::new(
                MessageRecipient(src),
                PreMessageBody::TopologyOk(TopologyOkPreBody {
                    in_reply_to: body.msg_id,
                }),
            )]
        }
        MessageBody::TopologyOk(_) => Default::default(),
    }
}
