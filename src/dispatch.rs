use std::fmt::Debug;

use anyhow::anyhow;
use anyhow::Result;
use serde::Serialize;
use thingbuf::mpsc;
use thingbuf::mpsc::Receiver;
use thingbuf::mpsc::Sender;
use tracing::debug;
use tracing::instrument;

use crate::node::NODE_ID;
use crate::protocol::Message;

#[derive(Debug, Clone)]
pub(crate) struct MessageDispatchHandle {
    sender: Sender<Vec<Message>>,
}

impl MessageDispatchHandle {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel::<Vec<Message>>(1024);
        let dispatch = MessageDispatch::new(rx);
        tokio::spawn(run_dispatch(dispatch));
        Self { sender: tx }
    }

    pub(crate) async fn dispatch(
        &self,
        msgs: Vec<Message>,
    ) -> Result<()> {
        self.sender.send(msgs).await.map_err(|e| anyhow!(e))
    }
}

struct MessageDispatch {
    msg_dispatch_queue_rx: Receiver<Vec<Message>>,
}

impl MessageDispatch {
    fn new(message_queue: Receiver<Vec<Message>>) -> Self {
        Self {
            msg_dispatch_queue_rx: message_queue,
        }
    }
}

async fn run_dispatch(dispatch: MessageDispatch) {
    loop {
        if let Some(msgs) = dispatch.msg_dispatch_queue_rx.recv().await {
            for msg in &msgs {
                let _ = serialize_and_send(msg);
            }
        }
    }
}

#[instrument(skip_all, fields(
    node = % NODE_ID.get().map(| s | s.as_str()).unwrap_or_else(|| "uninitialised")
))]
fn serialize_and_send<S>(msg: S) -> Result<()>
where
    S: Serialize,
    S: Debug,
{
    if let Ok(serialized_response) = serde_json::to_string(&msg) {
        debug!("Sending message {}", serialized_response);
        // Send to stdout
        println!("{serialized_response}");
        Ok(())
    } else {
        Err(anyhow!("Could not serialize message: {:?}", msg))
    }
}
