use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use once_cell::sync::OnceCell;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::info;
use tracing::span;
use tracing::Level;

use crate::dispatch::MessageDispatcher;
use crate::message_handling::handle_message;
use crate::message_store::BroadcastMessageStore;
use crate::pre_message::PreMessage;
use crate::protocol::MessageBody;
use crate::retry::RetryHandler;
use crate::transport::StdInTransport;

pub(crate) static NODE_ID: OnceCell<String> = OnceCell::new();

/// A node representing a server
#[derive(Debug)]
pub(crate) struct Node {
    transport: StdInTransport,
    msg_dispatch_queue_tx: Sender<PreMessage>,
    broadcast_messages: Arc<RwLock<HashSet<usize>>>,
    // The broadcast messages we sent to or received from our neighbours
    neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
    broadcast_message_store: Arc<RwLock<BroadcastMessageStore>>,
}

impl Node {
    /// Creates a new node.
    /// The node can only be initialised with an init message received via .
    ///
    /// # Panics
    /// An init message is expected for creating the node. This method will
    /// panic if the message could not be read or is of a different type.
    pub async fn new() -> Self {
        info!("Starting Node");
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

        // FIXME AJES: shutdown gracefully

        let (msg_dispatch_queue_tx, msg_dispatch_queue_rx) = mpsc::channel::<PreMessage>(32);
        let neighbour_broadcast_messages = Arc::new(RwLock::new(HashMap::new()));
        let broadcast_messages = Arc::new(RwLock::new(HashSet::new()));
        let broadcast_message_store = Arc::new(RwLock::new(BroadcastMessageStore::new()));
        let mut message_dispatcher =
            MessageDispatcher::new(msg_dispatch_queue_rx, broadcast_message_store.clone());
        tokio::spawn(async move {
            message_dispatcher.run().await;
        });
        let msg_dispatch_queue_tx_for_retry_handler = msg_dispatch_queue_tx.clone();
        let mut retry_handler = RetryHandler::new(
            msg_dispatch_queue_tx_for_retry_handler,
            neighbour_broadcast_messages.clone(),
            broadcast_messages.clone(),
        );
        tokio::spawn(async move {
            retry_handler.run().await;
        });

        let msgs = handle_message(
            init_msg,
            broadcast_messages.clone(),
            neighbour_broadcast_messages.clone(),
            broadcast_message_store.clone(),
        )
        .await;
        for msg in msgs {
            msg_dispatch_queue_tx
                .send(msg)
                .await
                .expect("be able to send init message");
        }

        Self {
            msg_dispatch_queue_tx,
            transport,
            broadcast_messages,
            neighbour_broadcast_messages,
            broadcast_message_store,
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
            let span = span!(
                Level::INFO,
                "node_operation",
                node_id = NODE_ID.get().expect("Node ID set")
            );
            let _enter = span.enter();
            let msg = self.transport.read_message().await?;
            let tx = self.msg_dispatch_queue_tx.clone();
            let broadcast_messages = self.broadcast_messages.clone();
            let neighbour_broadcast_messages = self.neighbour_broadcast_messages.clone();
            let broadcast_message_store = self.broadcast_message_store.clone();
            tokio::spawn(async move {
                let responses = handle_message(
                    msg,
                    broadcast_messages,
                    neighbour_broadcast_messages,
                    broadcast_message_store,
                )
                .await;
                debug!("sending initial messages: {:?}", responses.len());
                // TODO join all?
                for msg in responses {
                    let _ = tx.send(msg).await;
                }
            });
        }
    }
}
