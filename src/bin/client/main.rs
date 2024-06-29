use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::time;
use tokio_util::task::TaskTracker;
use tracing::{info, Level};

use tcp_over_redis::cache::connection::Connection;
use tcp_over_redis::cache::listener::Listener;
use tcp_over_redis::cache::redis::RedisClient;
use tcp_over_redis::error::ProxyError;
use tcp_over_redis::network;

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let client = RedisClient::new("redis://127.0.0.1").await?;

    run_client(client).await?;

    Ok(())
}

async fn run_client(client: RedisClient) -> Result<(), ProxyError> {
    let redis_timeout = Duration::from_secs(1);
    let connection_timeout = Duration::from_secs(30);

    let mut listener = Listener::listen(Some(Instant::now() + redis_timeout), redis_timeout, connection_timeout, Arc::new(client), 0).await?;

    loop {
        let Ok(conn) = listener.accept(None).await else {
            // TODO: error
            continue;
        };
        
        info!("accepting connection");

        tokio::spawn(handle_connection(conn));
    }
}

async fn handle_connection(conn: Connection) -> Result<(), ProxyError> {
    let timeout = Duration::from_secs(60);

    // TODO: timeout
    let stream = TcpStream::connect("44.206.219.79:80").await?;
    let (read_net, write_net) = stream.into_split();
    let (read_conn, write_conn) = conn.split();

    let copy1 = network::copy_from_net(read_net, write_conn);
    let copy2 = network::copy_from_conn(read_conn, write_net);

    let tracker = TaskTracker::new();
    tracker.spawn(copy1);
    tracker.spawn(copy2);
    tracker.close();

    time::timeout(timeout, tracker.wait()).await?;

    Ok(())
}