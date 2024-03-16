use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Copy, Clone)]
pub(crate) struct MessageCount(pub(crate) usize);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MessageRecipient(pub(crate) String);
