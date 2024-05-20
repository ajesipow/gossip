use anyhow::Result;
use once_cell::sync::OnceCell;
use tracing::debug;
use tracing::info;
use tracing::span;
use tracing::Level;

use crate::broadcast::gossip::Fanout;
use crate::broadcast::gossip::GossipHandle;
use crate::dispatch::MessageDispatchHandle;
use crate::message_handling::handle_message;
use crate::message_store::BroadcastMessageStoreHandle;
use crate::protocol::MessageBody;
use crate::retry::RetryHandler;
use crate::transport::StdInTransport;

// TODO make NodeId
pub(crate) static NODE_ID: OnceCell<String> = OnceCell::new();

/// A node representing a server
#[derive(Debug)]
pub(crate) struct Node {
    transport: StdInTransport,
    msg_dispatch: MessageDispatchHandle,
    broadcast: GossipHandle,
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

        let message_dispatch_handle = MessageDispatchHandle::new();
        let broadcast_message_store = BroadcastMessageStoreHandle::new();

        let gossip_broadcast = GossipHandle::new(
            broadcast_message_store.clone(),
            Fanout::new(10),
            3,
            message_dispatch_handle.clone(),
        );

        let msgs = handle_message(init_msg, gossip_broadcast.clone()).await;
        message_dispatch_handle
            .dispatch(msgs)
            .await
            .expect("be able to send init message");

        let mut retry_handler = RetryHandler::new(
            message_dispatch_handle.clone(),
            broadcast_message_store.clone(),
        );
        tokio::spawn(async move {
            retry_handler.run().await;
        });

        Self {
            msg_dispatch: message_dispatch_handle,
            transport,
            broadcast: gossip_broadcast,
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
            let message_dispatch = self.msg_dispatch.clone();
            let gossip_broadcast = self.broadcast.clone();
            tokio::spawn(async move {
                let responses = handle_message(msg, gossip_broadcast).await;
                if !responses.is_empty() {
                    debug!("sending initial messages: {:?}", responses.len());
                    let _ = message_dispatch.dispatch(responses).await;
                }
            });
        }
    }
}
