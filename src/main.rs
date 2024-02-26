use anyhow::Result;
use node::Node;
use tracing::Level;
use transport::StdInTransport;

mod node;
mod protocol;
mod transport;

fn main() -> Result<()> {
    // TODO change DIR here
    let file_appender =
        tracing_appender::rolling::hourly("/Users/alexjesipow/coding/gossip", "echo.log");
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_writer(file_appender)
        .init();
    let transport = StdInTransport::new();
    let mut node = Node::new(transport);
    node.run()
}
