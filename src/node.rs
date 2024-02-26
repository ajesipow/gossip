use anyhow::anyhow;
use anyhow::Result;
use tracing::debug;

use crate::protocol::Body;
use crate::protocol::EchoOkBody;
use crate::protocol::Message;
use crate::transport::Transport;

/// A node representing a server
#[derive(Debug)]
pub(crate) struct Node<T> {
    id: String,
    // Counter for message ids, monotonically increasing
    msg_counter: usize,
    transport: T,
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
        Self {
            id: init_body.node_id,
            msg_counter: 0,
            transport,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            let msg = self.transport.read_message()?;
            let response = handle_message(msg, self.id.clone(), self.msg_counter)?;
            let serialized_response = serde_json::to_string(&response)?;
            debug!("Sending message: {serialized_response:?}");
            println!("{serialized_response}");
            debug!("Message sent");
        }
    }
}

fn handle_message(
    msg: Message,
    // TODO make &str
    node_id: String,
    cnt: usize,
) -> Result<Message> {
    match msg.body {
        Body::Echo(echo_body) => Ok(Message {
            src: node_id,
            dest: msg.src,
            body: Body::EchoOk(EchoOkBody {
                msg_id: cnt,
                in_reply_to: Some(echo_body.msg_id),
                echo: echo_body.echo,
            }),
        }),
        t => Err(anyhow!("cannot handle message of type {t:?}")),
    }
}
