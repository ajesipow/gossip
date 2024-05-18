use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use crate::primitives::BroadcastMessage;
use crate::primitives::MessageId;
use crate::primitives::MessageRecipient;
use crate::primitives::NodeId;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Message {
    pub src: String,
    pub dest: MessageRecipient,
    pub body: MessageBody,
}

impl Message {
    pub(crate) fn id(&self) -> MessageId {
        match &self.body {
            MessageBody::Echo(b) => b.msg_id,
            MessageBody::EchoOk(b) => b.msg_id,
            MessageBody::Init(b) => b.msg_id,
            MessageBody::InitOk(b) => b.msg_id,
            MessageBody::Broadcast(b) => b.msg_id,
            MessageBody::BroadcastOk(b) => b.msg_id,
            MessageBody::Read(b) => b.msg_id,
            MessageBody::ReadOk(b) => b.msg_id,
            MessageBody::Topology(b) => b.msg_id,
            MessageBody::TopologyOk(b) => b.msg_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum MessageBody {
    Echo(EchoBody),
    EchoOk(EchoOkBody),
    Init(InitBody),
    InitOk(InitOkBody),
    Broadcast(BroadcastBody),
    BroadcastOk(BroadcastOkBody),
    Read(ReadBody),
    ReadOk(ReadOkBody),
    Topology(TopologyBody),
    TopologyOk(TopologyOkBody),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TopologyBody {
    pub msg_id: MessageId,
    pub topology: HashMap<String, Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TopologyOkBody {
    pub in_reply_to: MessageId,
    pub msg_id: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ReadBody {
    pub msg_id: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ReadOkBody {
    pub messages: Vec<BroadcastMessage>,
    pub in_reply_to: MessageId,
    pub msg_id: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct BroadcastBody {
    pub message: BroadcastMessage,
    pub msg_id: MessageId,
    // The nodes we are aware of that have already received the same broadcast message
    // pub acked_nodes: Vec<NodeId>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct BroadcastOkBody {
    pub msg_id: MessageId,
    pub in_reply_to: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct EchoBody {
    pub echo: String,
    pub msg_id: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct EchoOkBody {
    pub echo: String,
    pub msg_id: MessageId,
    pub in_reply_to: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct InitBody {
    pub msg_id: MessageId,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct InitOkBody {
    pub in_reply_to: MessageId,
    pub msg_id: MessageId,
}
