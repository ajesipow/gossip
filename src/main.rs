use anyhow::Result;
use node::Node;
use transport::StdInTransport;

mod node;
mod protocol;
mod transport;

#[tokio::main]
async fn main() -> Result<()> {
    let transport = StdInTransport::new();
    let mut node = Node::new(transport).await;
    node.run().await?;
    Ok(())
}
