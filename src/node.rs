use std::sync::Arc;

use anyhow::Result;
use once_cell::sync::OnceCell;
use thingbuf::mpsc;
use thingbuf::mpsc::Sender;
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
    msg_dispatch_queue_tx: Sender<Vec<PreMessage>>,
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
        // Using a vec to batch-send messages
        let (msg_dispatch_queue_tx, msg_dispatch_queue_rx) = mpsc::channel::<Vec<PreMessage>>(8);

        let broadcast_message_store = Arc::new(RwLock::new(BroadcastMessageStore::new()));

        let mut message_dispatcher =
            MessageDispatcher::new(msg_dispatch_queue_rx, broadcast_message_store.clone());
        tokio::spawn(async move {
            message_dispatcher.run().await;
        });
        let msg_dispatch_queue_tx_for_retry_handler = msg_dispatch_queue_tx.clone();
        let msgs = handle_message(init_msg, broadcast_message_store.clone()).await;
        msg_dispatch_queue_tx
            .send(msgs)
            .await
            .expect("be able to send init message");

        let mut retry_handler = RetryHandler::new(
            msg_dispatch_queue_tx_for_retry_handler,
            broadcast_message_store.clone(),
        );
        tokio::spawn(async move {
            retry_handler.run().await;
        });

        Self {
            msg_dispatch_queue_tx,
            transport,
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
            let broadcast_message_store = self.broadcast_message_store.clone();
            tokio::spawn(async move {
                let responses = handle_message(msg, broadcast_message_store).await;
                if !responses.is_empty() {
                    debug!("sending initial messages: {:?}", responses.len());
                    let _ = tx.send(responses).await;
                }
            });
        }
    }
}
