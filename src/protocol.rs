use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use crate::node::NODE_ID;
use crate::primitives::BroadcastMessage;
use crate::primitives::MessageId;
use crate::primitives::MessageRecipient;
use crate::primitives::NodeId;

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct Message {
    pub src: String,
    pub dest: MessageRecipient,
    pub body: MessageBody,
}

impl Message {
    pub(crate) fn broadcast(
        dest: MessageRecipient,
        message: BroadcastMessage,
    ) -> Self {
        Self {
            src: current_node_id(),
            dest,
            body: MessageBody::Broadcast(BroadcastBody {
                message,
                msg_id: MessageId::new(),
            }),
        }
    }

    pub(crate) fn broadcast_ok(
        dest: MessageRecipient,
        in_reply_to: MessageId,
    ) -> Self {
        Self {
            src: current_node_id(),
            dest,
            body: MessageBody::BroadcastOk(BroadcastOkBody {
                msg_id: MessageId::new(),
                in_reply_to,
            }),
        }
    }

    pub(crate) fn echo_ok(
        dest: MessageRecipient,
        echo: String,
        in_reply_to: MessageId,
    ) -> Self {
        Self {
            src: current_node_id(),
            dest,
            body: MessageBody::EchoOk(EchoOkBody {
                echo,
                msg_id: MessageId::new(),
                in_reply_to,
            }),
        }
    }

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

    pub(crate) fn init_ok(
        dest: MessageRecipient,
        in_reply_to: MessageId,
    ) -> Self {
        Self {
            src: current_node_id(),
            dest,
            body: MessageBody::InitOk(InitOkBody {
                msg_id: MessageId::new(),
                in_reply_to,
            }),
        }
    }

    pub(crate) fn read_ok(
        dest: MessageRecipient,
        messages: Vec<BroadcastMessage>,
        in_reply_to: MessageId,
    ) -> Self {
        Self {
            src: current_node_id(),
            dest,
            body: MessageBody::ReadOk(ReadOkBody {
                messages,
                msg_id: MessageId::new(),
                in_reply_to,
            }),
        }
    }

    pub(crate) fn topology_ok(
        dest: MessageRecipient,
        in_reply_to: MessageId,
    ) -> Self {
        Self {
            src: current_node_id(),
            dest,
            body: MessageBody::TopologyOk(TopologyOkBody {
                msg_id: MessageId::new(),
                in_reply_to,
            }),
        }
    }
}

fn current_node_id() -> String {
    NODE_ID
        .get()
        .expect("cannot build message with uninitialised NODE_ID")
        .to_string()
}

// TODO refactor subtypes directly into enum?
#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
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

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct TopologyBody {
    pub msg_id: MessageId,
    pub topology: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct TopologyOkBody {
    pub in_reply_to: MessageId,
    pub msg_id: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct ReadBody {
    pub msg_id: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct ReadOkBody {
    pub messages: Vec<BroadcastMessage>,
    pub in_reply_to: MessageId,
    pub msg_id: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct BroadcastBody {
    pub message: BroadcastMessage,
    pub msg_id: MessageId,
    // The nodes we are aware of that have already received the same broadcast message
    // pub acked_nodes: Vec<NodeId>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct BroadcastOkBody {
    pub msg_id: MessageId,
    pub in_reply_to: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct EchoBody {
    pub echo: String,
    pub msg_id: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct EchoOkBody {
    pub echo: String,
    pub msg_id: MessageId,
    pub in_reply_to: MessageId,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct InitBody {
    pub msg_id: MessageId,
    pub node_id: NodeId,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub(crate) struct InitOkBody {
    pub in_reply_to: MessageId,
    pub msg_id: MessageId,
}
