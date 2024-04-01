use anyhow::Result;
use node::Node;
use tracing::metadata::LevelFilter;

mod dispatch;
mod message_handling;
mod message_store;
mod node;
mod pre_message;
mod primitives;
mod protocol;
mod retry;
mod transport;

#[tokio::main]
async fn main() -> Result<()> {
    let file_appender =
        tracing_appender::rolling::hourly("/Users/alexjesipow/coding/gossip/logs", "test.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .with_writer(non_blocking)
        .init();

    let mut node = Node::new().await;
    node.run().await?;
    Ok(())
}
