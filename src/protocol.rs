use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Body {
    #[serde(rename = "type")]
    pub body_type: MessageType,
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub echo_body: EchoBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EchoBody {
    pub echo: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MessageType {
    Echo,
    EchoOk,
}
