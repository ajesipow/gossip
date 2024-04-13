use crate::primitives::BroadcastMessage;
use crate::primitives::MessageId;
use crate::primitives::MessageRecipient;
use crate::protocol::BroadcastBody;
use crate::protocol::BroadcastOkBody;
use crate::protocol::EchoOkBody;
use crate::protocol::InitOkBody;
use crate::protocol::Message;
use crate::protocol::MessageBody;
use crate::protocol::ReadOkBody;
use crate::protocol::TopologyOkBody;

/// [`PreMessage`] is an internal message format that does not require generic
/// properties of a full [`Message`] that internal actors should not have to
/// care about. Only messages that can be sent from a node can be constructed as
/// a [`PreMessage`].
#[derive(Debug, Clone)]
pub(crate) struct PreMessage {
    pub dest: MessageRecipient,
    pub body: PreMessageBody,
}

impl PreMessage {
    pub fn new(
        dest: MessageRecipient,
        body: PreMessageBody,
    ) -> Self {
        Self { dest, body }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PreMessageBody {
    EchoOk(EchoOkPreBody),
    InitOk(InitOkPreBody),
    Broadcast(BroadcastPreBody),
    BroadcastOk(BroadcastOkPreBody),
    ReadOk(ReadOkPreBody),
    TopologyOk(TopologyOkPreBody),
}

#[derive(Debug, Clone)]
pub(crate) struct EchoOkPreBody {
    pub echo: String,
    pub in_reply_to: MessageId,
}

#[derive(Debug, Clone)]
pub(crate) struct InitOkPreBody {
    pub in_reply_to: MessageId,
}

#[derive(Debug, Clone)]
pub(crate) struct BroadcastPreBody {
    pub message: BroadcastMessage,
}

#[derive(Debug, Clone)]
pub(crate) struct BroadcastOkPreBody {
    pub in_reply_to: MessageId,
}

#[derive(Debug, Clone)]
pub(crate) struct ReadOkPreBody {
    pub messages: Vec<BroadcastMessage>,
    pub in_reply_to: MessageId,
}

#[derive(Debug, Clone)]
pub(crate) struct TopologyOkPreBody {
    pub in_reply_to: MessageId,
}

impl From<(PreMessage, String, MessageId)> for Message {
    fn from((pre_message, src, message_count): (PreMessage, String, MessageId)) -> Self {
        Message {
            src,
            dest: pre_message.dest,
            body: (pre_message.body, message_count).into(),
        }
    }
}

impl From<(PreMessageBody, MessageId)> for MessageBody {
    fn from((body, msg_count): (PreMessageBody, MessageId)) -> Self {
        match body {
            PreMessageBody::EchoOk(body) => Self::EchoOk(EchoOkBody {
                echo: body.echo,
                msg_id: msg_count,
                in_reply_to: body.in_reply_to,
            }),
            PreMessageBody::InitOk(body) => Self::InitOk(InitOkBody {
                msg_id: msg_count,
                in_reply_to: body.in_reply_to,
            }),
            PreMessageBody::Broadcast(body) => Self::Broadcast(BroadcastBody {
                message: body.message,
                msg_id: msg_count,
            }),
            PreMessageBody::BroadcastOk(body) => Self::BroadcastOk(BroadcastOkBody {
                msg_id: msg_count,
                in_reply_to: body.in_reply_to,
            }),
            PreMessageBody::ReadOk(body) => Self::ReadOk(ReadOkBody {
                messages: body.messages,
                msg_id: msg_count,
                in_reply_to: body.in_reply_to,
            }),
            PreMessageBody::TopologyOk(body) => Self::TopologyOk(TopologyOkBody {
                msg_id: msg_count,
                in_reply_to: body.in_reply_to,
            }),
        }
    }
}
