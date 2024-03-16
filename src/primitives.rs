use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct MessageId(pub(crate) usize);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MessageRecipient(pub(crate) String);
