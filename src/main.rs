use anyhow::Result;
use node::Node;
use transport::StdInTransport;

mod node;
mod protocol;
mod transport;

fn main() -> Result<()> {
    let transport = StdInTransport::new();
    let mut node = Node::new(transport);
    node.run()
}
