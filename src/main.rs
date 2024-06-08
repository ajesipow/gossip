use tracing::metadata::LevelFilter;

use crate::node::NodeBuilder;
use crate::retry::ExponentialBackOff;

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

use clap::Parser;
use clap::Subcommand;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Retry {
        #[arg(default_value_t = 5)]
        retries: u32,
    },
    Gossip {
        #[arg(default_value_t = 4)]
        fanout: usize,
        #[arg(default_value_t = 5)]
        max_rounds: usize,
    },
}

#[tokio::main]
async fn main() {
    let file_appender =
        tracing_appender::rolling::hourly("/Users/alexjesipow/coding/gossip/logs", "test.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .with_writer(non_blocking)
        .init();
    let cli = Cli::parse();
    match cli.command {
        Commands::Retry { retries } => {
            let mut node = NodeBuilder::default()
                .retry()
                .policy(ExponentialBackOff::new(retries))
                .build()
                .await;
            node.run().await.expect("node not fail");
        }
        Commands::Gossip { fanout, max_rounds } => {
            let mut node = NodeBuilder::default()
                .gossip()
                .fanout(fanout)
                .max_broadcast_rounds(max_rounds)
                .build()
                .await;
            node.run().await.expect("node not fail");
        }
    };
}
