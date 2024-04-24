use std::borrow::Borrow;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
#[serde(transparent)]
pub(crate) struct MessageId(usize);

impl MessageId {
    pub(crate) fn new(id: usize) -> Self {
        Self(id)
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
#[serde(transparent)]
pub(crate) struct BroadcastMessage(usize);

#[cfg(test)]
impl BroadcastMessage {
    pub(crate) fn new(msg: usize) -> Self {
        Self(msg)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub(crate) struct MessageRecipient(String);

impl MessageRecipient {
    pub(crate) fn new(recipient: String) -> Self {
        Self(recipient)
    }
}

impl Borrow<str> for MessageRecipient {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for MessageRecipient {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
