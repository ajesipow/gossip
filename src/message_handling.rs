use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::error;

use crate::node::NODE_ID;
use crate::pre_message::BroadcastOkPreBody;
use crate::pre_message::BroadcastPreBody;
use crate::pre_message::EchoOkPreBody;
use crate::pre_message::InitOkPreBody;
use crate::pre_message::PreMessage;
use crate::pre_message::PreMessageBody;
use crate::pre_message::ReadOkPreBody;
use crate::pre_message::TopologyOkPreBody;
use crate::primitives::MessageRecipient;
use crate::protocol::Message;
use crate::protocol::MessageBody;

/// Handle incoming messages and return an appropriate response.
pub(crate) async fn handle_message(
    message: Message,
    broadcast_messages: Arc<RwLock<HashSet<usize>>>,
    neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
) -> Vec<PreMessage> {
    let src = message.src;
    match message.body {
        MessageBody::Echo(body) => vec![PreMessage::new(
            MessageRecipient(src),
            PreMessageBody::EchoOk(EchoOkPreBody {
                echo: body.echo,
                in_reply_to: body.msg_id,
            }),
        )],
        MessageBody::EchoOk(_) => Default::default(),
        MessageBody::Init(body) => vec![PreMessage::new(
            MessageRecipient(src),
            PreMessageBody::InitOk(InitOkPreBody {
                in_reply_to: body.msg_id,
            }),
        )],
        MessageBody::InitOk(_) => Default::default(),
        MessageBody::Broadcast(body) => {
            let mut messages = vec![];

            let broadcast_message = body.message;
            let mut broadcast_messages_lock = broadcast_messages.write().await;
            broadcast_messages_lock.insert(broadcast_message);
            drop(broadcast_messages_lock);

            // Record which message we already received from another node so we don't
            // unnecessarily send it back again.
            let mut neighbour_broadcast_messages_lock = neighbour_broadcast_messages.write().await;
            neighbour_broadcast_messages_lock
                .entry(src.clone())
                .or_default()
                .insert(broadcast_message);
            drop(neighbour_broadcast_messages_lock);

            let neighbour_broadcast_messages_lock = neighbour_broadcast_messages.read().await;
            let recipients: Vec<MessageRecipient> = neighbour_broadcast_messages_lock
                .iter()
                .filter_map(|(neighbour, messages)| {
                    if &src != neighbour && !messages.contains(&broadcast_message) {
                        // FIXME: avoid clone
                        Some(MessageRecipient(neighbour.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            drop(neighbour_broadcast_messages_lock);

            messages.push(PreMessage::new(
                MessageRecipient(src),
                PreMessageBody::BroadcastOk(BroadcastOkPreBody {
                    in_reply_to: body.msg_id,
                }),
            ));

            messages.extend(recipients.into_iter().map(|recipient| {
                PreMessage::new(
                    recipient,
                    PreMessageBody::Broadcast(BroadcastPreBody {
                        message: broadcast_message,
                    }),
                )
            }));

            messages
        }
        MessageBody::BroadcastOk(_) => Default::default(),
        MessageBody::Read(body) => {
            let messages = broadcast_messages.read().await;
            vec![PreMessage::new(
                MessageRecipient(src),
                PreMessageBody::ReadOk(ReadOkPreBody {
                    messages: messages.iter().copied().collect(),
                    in_reply_to: body.msg_id,
                }),
            )]
        }
        MessageBody::ReadOk(_) => Default::default(),
        MessageBody::Topology(mut body) => {
            if let Some(node_id) = NODE_ID.get() {
                if let Some(neighbours) = body.topology.remove(node_id) {
                    let mut neighbour_broadcast_messages_lock =
                        neighbour_broadcast_messages.write().await;
                    for neighbour in neighbours {
                        neighbour_broadcast_messages_lock
                            .entry(neighbour)
                            .or_default();
                    }
                    drop(neighbour_broadcast_messages_lock);
                }
            } else {
                error!("NODE_ID uninitialised!")
            }

            vec![PreMessage::new(
                MessageRecipient(src),
                PreMessageBody::TopologyOk(TopologyOkPreBody {
                    in_reply_to: body.msg_id,
                }),
            )]
        }
        MessageBody::TopologyOk(_) => Default::default(),
    }
}
