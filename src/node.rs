use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::anyhow;
use anyhow::Result;

use crate::protocol::Body;
use crate::protocol::BroadcastBody;
use crate::protocol::BroadcastOkBody;
use crate::protocol::EchoOkBody;
use crate::protocol::InitOkBody;
use crate::protocol::Message;
use crate::protocol::ReadOkBody;
use crate::protocol::TopologyOkBody;
use crate::transport::StdInTransport;
use crate::transport::Transport;

/// A node representing a server
#[derive(Debug)]
pub(crate) struct Node<T = StdInTransport> {
    id: String,
    // Counter for message ids, monotonically increasing
    msg_counter: usize,
    transport: T,
    broadcast_messages: HashSet<usize>,
    // The broadcast messages we sent to or receveived from our neighbours
    neighbour_broadcast_messages: HashMap<String, HashSet<usize>>,
}

impl<T: Transport> Node<T> {
    /// Creates a new node.
    /// The node can only be initialised with an init message received via .
    ///
    /// # Panics
    /// An init message is expected for creating the node. This method will
    /// panic if the message could not be read or is of a different type.
    pub async fn new(mut transport: T) -> Self {
        let init_msg = transport
            .read_message()
            .await
            .expect("be able to read init message");
        let Body::Init(init_body) = init_msg.body else {
            panic!("expected init message, got: {:?}", init_msg.body)
        };
        let reply = Message {
            src: init_msg.dest,
            dest: init_msg.src,
            body: Body::InitOk(InitOkBody {
                in_reply_to: init_body.msg_id,
            }),
        };
        transport
            .send_message(&reply)
            .await
            .expect("be able to send init ok response");

        Self {
            id: init_body.node_id,
            msg_counter: 0,
            transport,
            broadcast_messages: HashSet::new(),
            neighbour_broadcast_messages: HashMap::new(),
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
            let msg = self.transport.read_message().await?;
            let response = self.handle_message(&msg.src, msg.body).await?;
            if let Some(response_body) = response {
                self.send(msg.src, response_body).await?;
            }
        }
    }

    /// Send a message message from the node.
    async fn send(
        &mut self,
        dest: String,
        body: Body,
    ) -> Result<()> {
        let msg = Message {
            src: self.id.clone(),
            dest,
            body,
        };
        self.transport.send_message(&msg).await
    }

    /// Handle incoming messages and return an appropriate response.
    async fn handle_message(
        &mut self,
        src: &str,
        msg_body: Body,
    ) -> Result<Option<Body>> {
        match msg_body {
            Body::Echo(echo_body) => Ok(Some(Body::EchoOk(EchoOkBody {
                msg_id: self.message_counter(),
                in_reply_to: Some(echo_body.msg_id),
                echo: echo_body.echo,
            }))),
            Body::BroadcastOk(_) => Ok(None),
            Body::Broadcast(broadcast) => {
                let message = broadcast.message;
                self.broadcast_messages.insert(message);
                let receivers: Vec<String> = self
                    .neighbour_broadcast_messages
                    .iter()
                    .filter_map(|(neighbour, messages)| {
                        if src != neighbour && !messages.contains(&message) {
                            Some(neighbour.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                // Send message to neighbours if they have not already seen the same ID
                for dest in receivers {
                    // No need to send the same message back
                    let msg_id = self.message_counter();
                    self.send(dest, Body::Broadcast(BroadcastBody { message, msg_id }))
                        .await?;
                }
                // The neighbour now has seen the message either because we
                // received it from them or we sent it to them.
                for (_, messages) in self.neighbour_broadcast_messages.iter_mut() {
                    messages.insert(message);
                }

                Ok(Some(Body::BroadcastOk(BroadcastOkBody {
                    msg_id: self.message_counter(),
                    in_reply_to: broadcast.msg_id,
                })))
            }
            Body::Read(read) => Ok(Some(Body::ReadOk(ReadOkBody {
                messages: self.broadcast_messages.iter().copied().collect(),
                in_reply_to: read.msg_id,
            }))),
            Body::Topology(mut body) => {
                if let Some(neighbours) = body.topology.remove(&self.id) {
                    for neighbour in neighbours {
                        self.neighbour_broadcast_messages
                            .entry(neighbour)
                            .or_default();
                    }
                }
                Ok(Some(Body::TopologyOk(TopologyOkBody {
                    in_reply_to: body.msg_id,
                })))
            }
            t => Err(anyhow!("cannot handle message of type {t:?}")),
        }
    }

    fn message_counter(&mut self) -> usize {
        self.msg_counter += 1;
        self.msg_counter
    }
}
