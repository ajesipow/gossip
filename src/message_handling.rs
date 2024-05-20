use tracing::error;

use crate::broadcast::Broadcast;
use crate::node::NODE_ID;
use crate::primitives::MessageRecipient;
use crate::protocol::Message;
use crate::protocol::MessageBody;
use crate::topology::Topology;

/// Handle incoming messages and return an appropriate response.
pub(crate) async fn handle_message<B>(
    message: Message,
    broadcaster: B,
) -> Vec<Message>
where
    B: Broadcast,
{
    let src = message.src;
    match message.body {
        MessageBody::Echo(body) => vec![Message::echo_ok(
            MessageRecipient::new(src),
            body.echo,
            body.msg_id,
        )],
        MessageBody::EchoOk(_) => Default::default(),
        MessageBody::Init(body) => {
            vec![Message::init_ok(MessageRecipient::new(src), body.msg_id)]
        }
        MessageBody::InitOk(_) => Default::default(),
        MessageBody::Broadcast(body) => {
            // use spawn?
            broadcaster.broadcast(body.message).await;

            // --> gossip
            // let broadcast_message = body.message;
            // let mut lock = broadcaster.write().await;
            // lock.add(broadcast_message);
            // drop(lock);
            //
            // let broadcast_message_store_clone = broadcast_message_store.clone();
            // // let peer = src.clone();
            // let mut message_store_lock = broadcast_message_store_clone.write().await;
            // message_store_lock.insert(broadcast_message);
            // <-- gossip

            // // Record which message we already received from another node so we don't
            // // unnecessarily send it back again.
            // // We're not just adding any `src` as key to this map because we're also
            // // receiving these messages from clients which we don't want to send
            // broadcast // messages back to.
            // if message_store_lock
            //     .insert_for_peer_if_exists(&peer, broadcast_message)
            //     .is_err()
            // {
            //     warn!("peer {peer:?} does not exist in message store");
            // };
            //
            // // Send the same broadcast message to other nodes that we think have not seen
            // // it yet.
            // let unacked_nodes = message_store_lock
            //     .unacked_nodes(&broadcast_message)
            //     .into_iter()
            //     .cloned()
            //     .collect_vec();
            // drop(message_store_lock);

            // messages.extend(
            //     unacked_nodes.into_iter().map(|node| {
            //         Message::broadcast(MessageRecipient::new(node), broadcast_message)
            //     }),
            // );

            vec![Message::broadcast_ok(
                MessageRecipient::new(src),
                body.msg_id,
            )]
        }
        MessageBody::BroadcastOk(b) => {
            broadcaster
                .ack_by_msg_id(src.clone().into(), &b.in_reply_to)
                .await;
            Default::default()
        }
        MessageBody::Read(body) => {
            let msgs = broadcaster.messages().await;
            vec![Message::read_ok(
                MessageRecipient::new(src),
                msgs,
                body.msg_id,
            )]
        }
        MessageBody::ReadOk(_) => Default::default(),
        MessageBody::Topology(body) => {
            if let Some(node_id) = NODE_ID.get() {
                let topology = Topology::from((body.topology, node_id.clone()));
                broadcaster.update_topology(topology).await;
            } else {
                error!("NODE_ID uninitialised!")
            }

            vec![Message::topology_ok(
                MessageRecipient::new(src),
                body.msg_id,
            )]
        }
        MessageBody::TopologyOk(_) => Default::default(),
    }
}
