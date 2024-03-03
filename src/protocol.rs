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