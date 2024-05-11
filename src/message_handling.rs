use std::sync::Arc;

use itertools::Itertools;
use log::warn;
use tokio::sync::RwLock;
use tracing::error;

use crate::message_store::BroadcastMessageStore;
use crate::node::NODE_ID;
use crate::pre_message::BroadcastOkPreBody;
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
    broadcast_message_store: Arc<RwLock<BroadcastMessageStore>>,
) -> Vec<PreMessage> {
    let src = message.src;
    match message.body {
        MessageBody::Echo(body) => vec![PreMessage::new(
            MessageRecipient::new(src),
            PreMessageBody::EchoOk(EchoOkPreBody {
                echo: body.echo,
                in_reply_to: body.msg_id,
            }),
        )],
        MessageBody::EchoOk(_) => Default::default(),
        MessageBody::Init(body) => {
            vec![PreMessage::new(
                MessageRecipient::new(src),
                PreMessageBody::InitOk(InitOkPreBody {
                    in_reply_to: body.msg_id,
                }),
            )]
        }
        MessageBody::InitOk(_) => Default::default(),
        MessageBody::Broadcast(body) => {
            let mut messages = vec![];

            let broadcast_message = body.message;
            let broadcast_message_store_clone = broadcast_message_store.clone();
            let peer = src.clone();
            let mut message_store_lock = broadcast_message_store_clone.write().await;
            message_store_lock.insert(broadcast_message);
            // Record which message we already received from another node so we don't
            // unnecessarily send it back again.
            // We're not just adding any `src` as key to this map because we're also
            // receiving these messages from clients which we don't want to send broadcast
            // messages back to.
            if message_store_lock
                .insert_for_peer_if_exists(&peer, broadcast_message)
                .is_err()
            {
                warn!("peer {peer:?} does not exist in message store");
            };

            // Send the same broadcast message to other nodes that we think have not seen
            // it yet.
            let unacked_nodes = message_store_lock
                .unacked_nodes(&broadcast_message)
                .into_iter()
                .cloned()
                .collect_vec();
            drop(message_store_lock);

            messages.push(PreMessage::new(
                MessageRecipient::new(src),
                PreMessageBody::BroadcastOk(BroadcastOkPreBody {
                    in_reply_to: body.msg_id,
                }),
            ));

            messages.extend(
                unacked_nodes.into_iter().map(|node| {
                    PreMessage::broadcast(MessageRecipient::new(node), broadcast_message)
                }),
            );

            messages
        }
        MessageBody::BroadcastOk(b) => {
            // Remember that the recipient received the broadcast message so that we do not
            // send it again.
            let mut lock = broadcast_message_store.write().await;
            // TODO avoid clone
            let res = lock.insert_for_peer_by_msg_id_if_exists(src.clone(), &b.in_reply_to);
            drop(lock);
            if let Err(e) = res {
                error!("could not insert broadcast message for peer: {e:?}");
            }
            Default::default()
        }
        MessageBody::Read(body) => {
            let store_lock = broadcast_message_store.read().await;
            let messages = store_lock.msgs();
            drop(store_lock);
            vec![PreMessage::new(
                MessageRecipient::new(src),
                PreMessageBody::ReadOk(ReadOkPreBody {
                    messages,
                    in_reply_to: body.msg_id,
                }),
            )]
        }
        MessageBody::ReadOk(_) => Default::default(),
        MessageBody::Topology(mut body) => {
            if let Some(node_id) = NODE_ID.get() {
                if let Some(neighbours) = body.topology.remove(node_id) {
                    let mut message_store_lock = broadcast_message_store.write().await;
                    for neighbour in neighbours {
                        message_store_lock.register_peer(neighbour);
                    }
                    drop(message_store_lock)
                }
            } else {
                error!("NODE_ID uninitialised!")
            }

            vec![PreMessage::new(
                MessageRecipient::new(src),
                PreMessageBody::TopologyOk(TopologyOkPreBody {
                    in_reply_to: body.msg_id,
                }),
            )]
        }
        MessageBody::TopologyOk(_) => Default::default(),
    }
}
