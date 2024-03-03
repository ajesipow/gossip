use std::collections::HashSet;

use anyhow::anyhow;
use anyhow::Result;

use crate::protocol::Body;
use crate::protocol::BroadcastOkBody;
use crate::protocol::EchoOkBody;
use crate::protocol::InitOkBody;
use crate::protocol::Message;
use crate::protocol::ReadOkBody;
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
}

impl<T: Transport> Node<T> {
    /// Creates a new node.
    /// The node can only be initialised with an init message received via .
    ///
    /// # Panics
    /// An init message is expected for creating the node. This method will
    /// panic if the message could not be read or is of a different type.
    pub fn new(mut transport: T) -> Self {
        let init_msg = transport
            .read_message()
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
            .expect("be able to send init ok response");

        Self {
            id: init_body.node_id,
            msg_counter: 0,
            transport,
            broadcast_messages: HashSet::new(),
        }
    }

    /// Run the node.
    /// Lets the node read and respond to incoming messages.
    ///
    /// # Errors
    /// Throws an error if the message cannot be read, sent or is of an unknown
    /// type.
    pub fn run(&mut self) -> Result<()> {
        loop {
            let msg = self.transport.read_message()?;
            let response = self.handle_message(msg.body)?;
            self.send(msg.src, response)?;
        }
    }

    /// Send a message message from the node.
    fn send(
        &mut self,
        dest: String,
        body: Body,
    ) -> Result<()> {
        let msg = Message {
            src: self.id.clone(),
            dest,
            body,
        };
        self.transport.send_message(&msg)
    }

    /// Handle incoming messages and return an appropriate response.
    fn handle_message(
        &mut self,
        msg_body: Body,
    ) -> Result<Body> {
        match msg_body {
            Body::Echo(echo_body) => Ok(Body::EchoOk(EchoOkBody {
                msg_id: self.msg_counter,
                in_reply_to: Some(echo_body.msg_id),
                echo: echo_body.echo,
            })),
            Body::Broadcast(broadcast) => {
                self.broadcast_messages.insert(broadcast.message);
                Ok(Body::BroadcastOk(BroadcastOkBody {
                    msg_id: self.msg_counter,
                    in_reply_to: broadcast.msg_id,
                }))
            }
            Body::Read(read) => Ok(Body::ReadOk(ReadOkBody {
                messages: self.broadcast_messages.iter().copied().collect(),
                in_reply_to: read.msg_id,
            })),
            t => Err(anyhow!("cannot handle message of type {t:?}")),
        }
    }
}
