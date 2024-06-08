use std::fmt::Debug;

use anyhow::anyhow;
use anyhow::Result;
use serde::Serialize;
use thingbuf::mpsc;
use thingbuf::mpsc::Receiver;
use thingbuf::mpsc::Sender;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tracing::debug;
use tracing::instrument;

use crate::node::NODE_ID;
use crate::protocol::Message;

#[derive(Debug, Clone)]
pub(crate) struct MessageDispatchHandle {
    sender: Sender<Vec<Message>>,
}

impl MessageDispatchHandle {
    pub(crate) fn new<W>(handle: W) -> Self
    where
        W: AsyncWrite,
        W: Unpin,
        W: Send,
        W: 'static,
    {
        let (tx, rx) = mpsc::channel::<Vec<Message>>(1024);
        tokio::spawn(async move {
            let dispatch = MessageDispatch::new(rx, handle);
            run_dispatch(dispatch).await
        });
        Self { sender: tx }
    }

    pub(crate) async fn dispatch(
        &self,
        msgs: Vec<Message>,
    ) -> Result<()> {
        self.sender.send(msgs).await.map_err(|e| anyhow!(e))
    }
}

struct MessageDispatch<W> {
    msg_dispatch_queue_rx: Receiver<Vec<Message>>,
    writer: W,
}

impl<W: AsyncWrite + Unpin> MessageDispatch<W> {
    fn new(
        message_queue: Receiver<Vec<Message>>,
        writer: W,
    ) -> Self {
        Self {
            msg_dispatch_queue_rx: message_queue,
            writer,
        }
    }
}

async fn run_dispatch<W: AsyncWrite + Unpin>(mut dispatch: MessageDispatch<W>) {
    loop {
        if let Some(msgs) = dispatch.msg_dispatch_queue_rx.recv().await {
            for msg in &msgs {
                let _ = serialize_and_send(msg, &mut dispatch.writer).await;
            }
        }
    }
}

#[instrument(skip_all, fields(
    node = % NODE_ID.get().map(| s | s.as_str()).unwrap_or_else(|| "uninitialised")
))]
async fn serialize_and_send<S, W>(
    msg: S,
    writer: &mut W,
) -> Result<()>
where
    S: Serialize,
    S: Debug,
    W: AsyncWrite,
    W: Unpin,
{
    if let Ok(serialized_response) = serde_json::to_string(&msg) {
        debug!("Sending message {}", serialized_response);
        writer.write_all(serialized_response.as_bytes()).await?;
        writer.write_all(&[b'\n']).await?;
        writer.flush().await?;
        Ok(())
    } else {
        Err(anyhow!("Could not serialize message: {:?}", msg))
    }
}
