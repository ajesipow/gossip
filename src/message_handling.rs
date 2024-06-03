use tracing::debug;
use tracing::error;
use tracing::instrument;

use crate::broadcast::Broadcast;
use crate::node::NODE_ID;
use crate::primitives::MessageRecipient;
use crate::protocol::Message;
use crate::protocol::MessageBody;
use crate::topology::Topology;

/// Handle incoming messages and return an appropriate response.
#[instrument(skip_all, fields(
    node = % NODE_ID.get().map(| s | s.as_str()).unwrap_or_else(|| "uninitialised")
))]
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
            debug!("Received broadcast message: {:?} from {:?}", body, src);
            // use spawn?
            broadcaster.broadcast(body.message).await;

            vec![Message::broadcast_ok(
                MessageRecipient::new(src),
                body.msg_id,
            )]
        }
        MessageBody::BroadcastOk(b) => {
            debug!("Received broadcast OK message");
            broadcaster
                .ack_by_msg_id(src.clone().into(), b.in_reply_to)
                .await;
            Default::default()
        }
        MessageBody::Read(body) => {
            debug!("Received read message");
            let msgs = broadcaster.messages().await.unwrap();
            vec![Message::read_ok(
                MessageRecipient::new(src),
                msgs,
                body.msg_id,
            )]
        }
        MessageBody::ReadOk(_) => Default::default(),
        MessageBody::Topology(body) => {
            debug!("received topology msg");
            if let Some(node_id) = NODE_ID.get() {
                debug!("creating topology");
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
