use anyhow::Result;
use node::Node;

mod dispatch;
mod node;
mod pre_message;
mod primitives;
mod protocol;
mod transport;

#[tokio::main]
async fn main() -> Result<()> {
    let mut node = Node::new().await;
    node.run().await?;
    Ok(())
}
