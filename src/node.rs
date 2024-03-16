use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use once_cell::sync::OnceCell;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

use crate::ack_store::AckStore;
use crate::dispatch::MessageDispatcher;
use crate::message_handling::handle_message;
use crate::pre_message::PreMessage;
use crate::protocol::Message;
use crate::protocol::MessageBody;
use crate::transport::StdInTransport;

pub(crate) static NODE_ID: OnceCell<String> = OnceCell::new();

/// A node representing a server
#[derive(Debug)]
pub(crate) struct Node {
    transport: StdInTransport,
    pre_message_queue: Sender<PreMessage>,
    ack_store: Arc<RwLock<AckStore>>,
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

        let (pre_message_queue_tx, pre_message_queue_rx) = mpsc::channel::<PreMessage>(32);
        let (_retry_queue_tx, retry_queue_rx) = mpsc::channel::<Message>(32);
        let ack_store = Arc::new(RwLock::new(AckStore::new()));
        let mut message_dispatcher =
            MessageDispatcher::new(pre_message_queue_rx, retry_queue_rx, ack_store.clone());
        tokio::spawn(async move {
            message_dispatcher.run().await;
        });

        let broadcast_messages = Arc::new(RwLock::new(HashSet::new()));
        let neighbour_broadcast_messages = Arc::new(RwLock::new(HashMap::new()));
        let msgs = handle_message(
            init_msg,
            ack_store.clone(),
            broadcast_messages.clone(),
            neighbour_broadcast_messages.clone(),
        )
        .await;
        for msg in msgs {
            pre_message_queue_tx
                .send(msg)
                .await
                .expect("be able to send init message");
        }

        Self {
            pre_message_queue: pre_message_queue_tx,
            transport,
            ack_store,
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
            let tx = self.pre_message_queue.clone();
            let broadcast_messages = self.broadcast_messages.clone();
            let neighbour_broadcast_messages = self.neighbour_broadcast_messages.clone();
            let ack_store = self.ack_store.clone();
            tokio::spawn(async move {
                let responses = handle_message(
                    msg,
                    ack_store,
                    broadcast_messages,
                    neighbour_broadcast_messages,
                )
                .await;
                // TODO join all?
                for msg in responses {
                    let _ = tx.send(msg).await;
                }
            });
        }
    }
}
