use anyhow::Result;
use node::Node;
use tracing::metadata::LevelFilter;

mod broadcast;
mod dispatch;
mod errors;
mod message_handling;
mod message_store;
mod node;
mod primitives;
mod protocol;
mod retry;
mod topology;
mod transport;

// struct ConstLayer;
//
// impl<S> Layer<S> for ConstLayer
//     where
//         S: Subscriber + for<'span> LookupSpan<'span>,
// {
//     fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx:
// Context<'_, S>) {         if let Some(span) = ctx.span(id) {
//             span.extensions_mut().insert(NODE_ID.get().map(|s|
// s.as_str()).unwrap_or_else(|| "uninitialised"))         }
//     }
// }

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
    node.run().await
}
