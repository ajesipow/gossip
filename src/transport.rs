use std::io::stdin;

use anyhow::Result;
use tracing::debug;
use tracing::info;

use crate::protocol::Message;

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

impl StdInTransport {
    pub async fn read_message(&mut self) -> Result<Message> {
        self.buf.clear();
        stdin().read_line(&mut self.buf)?;
        debug!("Received message. Raw input: {:?}", &self.buf);
        let msg: Message = serde_json::from_str(&self.buf)?;
        info!("Message received from {:?}", msg.src);
        Ok(msg)
    }
}
