use std::io::Write;
use std::io::{self};
use std::mem;

use anyhow::anyhow;
use anyhow::Result;
use protocol::Body;
use protocol::Message;
use protocol::MessageType;

mod protocol;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    if !buffer.is_empty() {
        let thread_buf = mem::take(&mut buffer);
        tokio::task::spawn(async move {
            let _ = process(&thread_buf).await;
        });
    }
    Ok(())
}

async fn process(input: &str) -> Result<()> {
    let msg: Message = serde_json::from_str(input)?;
    let response = handle_message(msg)?;
    let serialized_response = serde_json::to_string(&response)?;
    io::stdout().write_all(serialized_response.as_bytes())?;
    Ok(())
}

fn handle_message(msg: Message) -> Result<Message> {
    match msg.body.body_type {
        MessageType::Echo => Ok(handle_echo(msg)),
        t => Err(anyhow!("cannot handle message of type {t:?}")),
    }
}

fn handle_echo(msg: Message) -> Message {
    Message {
        src: "n1".to_string(),
        dest: msg.src,
        body: Body {
            body_type: MessageType::EchoOk,
            msg_id: Some(1),
            in_reply_to: msg.body.msg_id,
            echo_body: msg.body.echo_body,
        },
    }
}
