use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;
use tracing::instrument;
use tracing::warn;

use crate::message_store::BroadcastMessageStore;
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
#[instrument(skip(broadcast_messages, neighbour_broadcast_messages))]
pub(crate) async fn handle_message(
    message: Message,
    broadcast_messages: Arc<RwLock<HashSet<usize>>>,
    neighbour_broadcast_messages: Arc<RwLock<HashMap<String, HashSet<usize>>>>,
    broadcast_message_store: Arc<RwLock<BroadcastMessageStore>>,
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
        MessageBody::EchoOk(_) => {
            debug!("Received Echo Ok msg from {:?}", src);
            Default::default()
        }
        MessageBody::Init(body) => {
            debug!("Received Init msg from {:?}", src);
            vec![PreMessage::new(
                MessageRecipient(src),
                PreMessageBody::InitOk(InitOkPreBody {
                    in_reply_to: body.msg_id,
                }),
            )]
        }
        MessageBody::InitOk(_) => {
            debug!("Received init Ok msg from {:?}", src);
            Default::default()
        }
        MessageBody::Broadcast(body) => {
            debug!("Received Broadcast msg from {:?}", src);
            let mut messages = vec![];

            let broadcast_message = body.message;
            let mut broadcast_messages_lock = broadcast_messages.write().await;
            broadcast_messages_lock.insert(broadcast_message);
            drop(broadcast_messages_lock);

            // Record which message we already received from another node so we don't
            // unnecessarily send it back again.
            // We're not just adding any `src` as key to this map because we're also
            // receiving these messages from clients which we don't want to
            let mut neighbour_broadcast_messages_lock = neighbour_broadcast_messages.write().await;
            neighbour_broadcast_messages_lock
                .get_mut(&src)
                .map(|msgs| msgs.insert(broadcast_message));
            drop(neighbour_broadcast_messages_lock);

            // Send the same broadcast message to other nodes that we think have not seen it
            // yet.
            let neighbour_broadcast_messages_lock = neighbour_broadcast_messages.read().await;
            let recipients: Vec<MessageRecipient> = neighbour_broadcast_messages_lock
                .iter()
                .filter_map(|(neighbour, messages)| {
                    if !messages.contains(&broadcast_message) {
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
        MessageBody::BroadcastOk(b) => {
            debug!("Received broadcast Ok msg from {:?}", src);
            // Remember that the recipient received the broadcast message so that we do not
            // send it again.
            let maybe_broadcast_msg = broadcast_message_store
                .read()
                .await
                .get(&b.in_reply_to)
                .copied();
            if let Some(broadcast_msg) = maybe_broadcast_msg {
                let mut neighbour_broadcast_messages_lock =
                    neighbour_broadcast_messages.write().await;
                neighbour_broadcast_messages_lock
                    .get_mut(&src)
                    .map(|msgs| msgs.insert(broadcast_msg));
                drop(neighbour_broadcast_messages_lock);
            } else {
                warn!("Could not find message ID in broadcast store");
            }
            Default::default()
        }
        MessageBody::Read(body) => {
            debug!("Received Read msg from {:?}", src);
            let messages = broadcast_messages.read().await;
            vec![PreMessage::new(
                MessageRecipient(src),
                PreMessageBody::ReadOk(ReadOkPreBody {
                    messages: messages.iter().copied().collect(),
                    in_reply_to: body.msg_id,
                }),
            )]
        }
        MessageBody::ReadOk(_) => {
            debug!("Received read Ok msg from {:?}", src);
            Default::default()
        }
        MessageBody::Topology(mut body) => {
            debug!("Received Topology msg {:?} from {:?}", body, src);
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
        MessageBody::TopologyOk(_) => {
            debug!("Received topology Ok msg from {:?}", src);
            Default::default()
        }
    }
}
