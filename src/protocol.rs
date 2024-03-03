use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum Body {
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TopologyBody {
    pub msg_id: usize,
    pub topology: HashMap<String, Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TopologyOkBody {
    pub in_reply_to: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReadBody {
    pub msg_id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReadOkBody {
    pub messages: Vec<usize>,
    pub in_reply_to: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BroadcastBody {
    pub message: usize,
    pub msg_id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BroadcastOkBody {
    pub msg_id: usize,
    pub in_reply_to: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EchoBody {
    pub echo: String,
    pub msg_id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EchoOkBody {
    pub echo: String,
    pub msg_id: usize,
    pub in_reply_to: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct InitBody {
    pub msg_id: usize,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct InitOkBody {
    pub in_reply_to: usize,
}
