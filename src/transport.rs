use std::io::stdin;

use anyhow::Result;

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
        let msg: Message = serde_json::from_str(&self.buf)?;
        Ok(msg)
    }
}
