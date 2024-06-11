use std::borrow::Borrow;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use serde::Deserialize;
use serde::Serialize;

pub(crate) static MESSAGE_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub(crate) struct NodeId(String);

impl Display for NodeId {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<NodeId> for MessageRecipient {
    fn from(value: NodeId) -> Self {
        Self(value.0)
    }
}

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
#[serde(transparent)]
pub(crate) struct MessageId(usize);

impl MessageId {
    pub(crate) fn new() -> Self {
        Self(MESSAGE_COUNTER.fetch_add(1, Ordering::Relaxed))
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

impl From<String> for MessageRecipient {
    fn from(value: String) -> Self {
        Self(value)
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
