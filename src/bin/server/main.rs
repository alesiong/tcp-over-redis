use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::time;
use tokio_util::task::TaskTracker;
use tracing::Level;

use tcp_over_redis::cache::connection::Connection;
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

    run_server(client).await?;

    Ok(())
}

async fn run_server(client: RedisClient) -> Result<(), ProxyError> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let client = Arc::new(client);

    loop {
        // TODO: timeout
        let Ok((stream, _)) = listener.accept().await else {
            // TODO: error
            continue;
        };

        tokio::spawn(handle_connection(Arc::clone(&client), stream));
    }
}

async fn handle_connection(client: Arc<RedisClient>, stream: TcpStream) -> Result<(), ProxyError> {
    // TODO:
    let timeout = Duration::from_secs(60);
    let redis_timeout = Duration::from_secs(1);
    let connection_timeout = Duration::from_secs(30);
    let connection = Connection::dial(Some(Instant::now() + redis_timeout * 8), redis_timeout, connection_timeout, client, 1).await?;

    let (read_net, write_net) = stream.into_split();
    let (read_conn, write_conn) = connection.split();

    let copy1 = network::copy_from_net(read_net, write_conn);
    let copy2 = network::copy_from_conn(read_conn, write_net);

    let tracker = TaskTracker::new();
    tracker.spawn(copy1);
    tracker.spawn(copy2);
    tracker.close();

    time::timeout(timeout, tracker.wait()).await?;

    Ok(())
}



