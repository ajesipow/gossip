use std::io::stdin;

use anyhow::Result;
use tracing::debug;
use tracing::info;

use crate::protocol::Message;

/// The main trait for retrieving messages.
pub(crate) trait Transport {
    /// Reads a message from transport.
    fn read_message(&mut self) -> Result<Message>;

    /// Sends a message via transport.
    fn send_message(
        &mut self,
        msg: Message,
    ) -> Result<()>;
}

/// An implementation for `Transport` to read from stdin.
#[derive(Debug, Clone)]
pub(crate) struct StdInTransport {
    buf: String,
}

impl StdInTransport {
    pub(crate) fn new() -> Self {
        Self { buf: String::new() }
    }
}

impl Transport for StdInTransport {
    fn read_message(&mut self) -> Result<Message> {
        stdin().read_line(&mut self.buf)?;
        debug!("Received message. Raw input: {:?}", &self.buf);
        let msg: Message = serde_json::from_str(&self.buf)?;
        info!("Message received from {:?}", msg.src);
        self.buf.clear();
        Ok(msg)
    }

    fn send_message(
        &mut self,
        msg: Message,
    ) -> Result<()> {
        let serialized_response = serde_json::to_string(&msg)?;
        debug!("Sending message");
        // We just send to stdout
        println!("{serialized_response}");
        Ok(())
    }
}
