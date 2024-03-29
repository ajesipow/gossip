use std::collections::VecDeque;

use chrono::DateTime;
use chrono::Utc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::debug;
use tracing::error;

use crate::message_handling::QueuedMessage;
use crate::protocol::Message;

#[derive(Debug)]
pub struct RetryMessage {
    last_dispatch: DateTime<Utc>,
    msg: Message,
}

impl RetryMessage {
    pub fn new(msg: Message) -> Self {
        Self {
            last_dispatch: Utc::now(),
            msg,
        }
    }
}

/// Handles message retries
#[derive(Debug)]
pub(crate) struct RetryHandler {
    fifo: VecDeque<RetryMessage>,
    retry_queue: Receiver<RetryMessage>,
    msg_dispatch_queue_tx: Sender<QueuedMessage>,
}

impl RetryHandler {
    pub(crate) fn new(
        retry_queue: Receiver<RetryMessage>,
        msg_dispatch_queue_tx: Sender<QueuedMessage>,
    ) -> Self {
        Self {
            fifo: VecDeque::new(),
            retry_queue,
            msg_dispatch_queue_tx,
        }
    }

    pub(crate) async fn run(&mut self) {
        debug!("Running retry handler");
        // Periodically check for message to dispatch for sending
        loop {
            if let Some(first) = self.fifo.pop_front() {
                debug!(
                    "Message {:?} in retry queue, last dispatched {:?}",
                    first.msg.id(),
                    first.last_dispatch
                );
                if (Utc::now() - first.last_dispatch).num_milliseconds() >= 100 {
                    let msg_id = first.msg.id();
                    debug!("Dispatching message {:?} again", msg_id);
                    // Dispatch again
                    if let Err(e) = self
                        .msg_dispatch_queue_tx
                        .send(QueuedMessage::ForRetry(first.msg.clone()))
                        .await
                    {
                        error!("Failed retrying message: {:?}", e);
                        self.fifo.push_front(first);
                    }
                } else {
                    debug!("Putting message {:?} into queue again", first.msg.id());
                    self.fifo.push_front(first);
                }
            }
            if let Some(to_retry) = self.retry_queue.recv().await {
                debug!("Received message {:?} for retry", to_retry.msg.id());
                self.schedule_for_retry(to_retry);
            }
        }
    }

    pub(crate) fn schedule_for_retry(
        &mut self,
        message: RetryMessage,
    ) {
        debug!(
            "Scheduling message for retry, receiver: {:?}",
            message.msg.dest
        );
        self.fifo.push_back(message);
        debug!("fifo contents {:?}", self.fifo);
    }
}
