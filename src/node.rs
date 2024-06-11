use anyhow::Result;
use once_cell::sync::OnceCell;
use tokio::io;
use tracing::info;

use crate::broadcast::gossip::Fanout;
use crate::broadcast::gossip::GossipHandle;
use crate::broadcast::Broadcast;
use crate::dispatch::MessageDispatchHandle;
use crate::message_handling::handle_message;
use crate::message_store::BroadcastMessageStoreHandle;
use crate::primitives::NodeId;
use crate::protocol::Message;
use crate::protocol::MessageBody;
use crate::retry::ExponentialBackOff;
use crate::retry::RetryHandle;
use crate::retry::RetryPolicy;
use crate::topology::TopologyStoreHandle;
use crate::transport::StdInTransport;

pub(crate) static NODE_ID: OnceCell<NodeId> = OnceCell::new();

#[derive(Debug)]
pub struct NodeBuilder {
    transport: StdInTransport,
}

impl Default for NodeBuilder {
    fn default() -> Self {
        Self {
            transport: StdInTransport::new(),
        }
    }
    // TODO add build method or impl await
}

impl NodeBuilder {
    pub fn gossip(self) -> GossipBuilder {
        GossipBuilder {
            node_builder: self,
            fanout: Fanout::new(4),
            max_broadcast_rounds: 3,
        }
    }

    pub fn retry(self) -> RetryBuilder<ExponentialBackOff> {
        RetryBuilder {
            policy: ExponentialBackOff::default(),
            node_builder: self,
        }
    }
}

#[derive(Debug)]
pub struct GossipBuilder {
    node_builder: NodeBuilder,
    fanout: Fanout,
    max_broadcast_rounds: usize,
}

impl GossipBuilder {
    pub fn fanout(
        self,
        fanout: usize,
    ) -> Self {
        Self {
            fanout: Fanout::new(fanout),
            ..self
        }
    }

    pub fn max_broadcast_rounds(
        self,
        max_broadcast_rounds: usize,
    ) -> Self {
        Self {
            max_broadcast_rounds,
            ..self
        }
    }

    pub async fn build(mut self) -> Node<GossipHandle> {
        // FIXME AJES: shutdown gracefully
        let init_msg = initialize_node(&mut self.node_builder.transport).await;
        let message_dispatch_handle = MessageDispatchHandle::new(io::stdout());

        let broadcast_message_store = BroadcastMessageStoreHandle::new();
        let topology_store_handle = TopologyStoreHandle::new();
        let broadcast = GossipHandle::new(
            broadcast_message_store,
            topology_store_handle,
            self.fanout,
            self.max_broadcast_rounds,
            message_dispatch_handle.clone(),
        );
        build_node(
            self.node_builder.transport,
            init_msg,
            message_dispatch_handle,
            broadcast,
        )
        .await
    }
}

#[derive(Debug)]
pub struct RetryBuilder<P: RetryPolicy + Send> {
    node_builder: NodeBuilder,
    policy: P,
}

impl<P: RetryPolicy + Send + 'static> RetryBuilder<P> {
    pub fn policy(
        self,
        retry_policy: P,
    ) -> Self {
        Self {
            policy: retry_policy,
            ..self
        }
    }

    pub async fn build(mut self) -> Node<RetryHandle> {
        // FIXME AJES: shutdown gracefully
        let init_msg = initialize_node(&mut self.node_builder.transport).await;
        let message_dispatch_handle = MessageDispatchHandle::new(io::stdout());
        let message_store = BroadcastMessageStoreHandle::new();
        let broadcast =
            RetryHandle::new(message_dispatch_handle.clone(), message_store, self.policy);
        build_node(
            self.node_builder.transport,
            init_msg,
            message_dispatch_handle,
            broadcast,
        )
        .await
    }
}

/// A node representing a server
#[derive(Debug)]
pub struct Node<B> {
    transport: StdInTransport,
    msg_dispatch: MessageDispatchHandle,
    broadcast: B,
}

async fn initialize_node(transport: &mut StdInTransport) -> Message {
    info!("Starting Node");

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
    init_msg
}

async fn build_node<B: Broadcast>(
    transport: StdInTransport,
    init_msg: Message,
    message_dispatch_handle: MessageDispatchHandle,
    broadcast: B,
) -> Node<B> {
    let msgs = handle_message(init_msg, broadcast.clone()).await;
    message_dispatch_handle
        .dispatch(msgs)
        .await
        .expect("be able to send init message");

    Node {
        msg_dispatch: message_dispatch_handle,
        transport,
        broadcast,
    }
}

impl<B: Broadcast + 'static> Node<B> {
    /// Run the node.
    /// Lets the node read and respond to incoming messages.
    ///
    /// # Errors
    /// Throws an error if the message cannot be read, sent or is of an unknown
    /// type.
    pub async fn run(&mut self) -> Result<()> {
        loop {
            let msg = self.transport.read_message().await?;
            let message_dispatch = self.msg_dispatch.clone();
            let broadcast = self.broadcast.clone();
            tokio::spawn(async move {
                let responses = handle_message(msg, broadcast).await;
                if !responses.is_empty() {
                    let _ = message_dispatch.dispatch(responses).await;
                }
            });
        }
    }
}
