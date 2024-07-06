use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::{select, time};
use tokio_util::task::TaskTracker;
use tracing::{error, info, instrument, Level};
use tracing_subscriber::util::SubscriberInitExt;

use tcp_over_redis::cache::connection::Connection;
use tcp_over_redis::cache::redis::RedisClient;
use tcp_over_redis::error::ProxyError;
use tcp_over_redis::network;

#[cfg(feature = "profiling")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), ProxyError> {
    #[cfg(feature = "profiling")]
    console_subscriber::ConsoleLayer::builder().with_default_env()
        .server_addr(SocketAddr::from_str("127.0.0.1:6670").unwrap())
        .init();

    #[cfg(not(feature = "profiling"))]
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    tokio_main()
}

#[tokio::main]
async fn tokio_main() -> Result<(), ProxyError> {
    #[cfg(feature = "profiling")]
    let _profiler = dhat::Profiler::builder()
        .file_name(PathBuf::from("dhat-heap-server.json"))
        .build();
    let client = RedisClient::new("redis://127.0.0.1").await?;

    run_server(client).await?;

    Ok(())
}

#[instrument(level = "info", skip_all, err(Debug))]
async fn run_server(client: RedisClient) -> Result<(), ProxyError> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let client = Arc::new(client);

    loop {
        // TODO: timeout
        select! {
           accept = listener.accept() => {
                let (stream, sock) = match accept {
                    Ok(ok) => ok,
                    Err(err) => {
                        error!(?err, "error accepting connection");
                        continue;
                    }
                };

                info!(remote= sock.to_string(), "accepting connection");

                tokio::spawn(handle_connection(Arc::clone(&client), stream));
            }
            // TODO: error
            _ = tokio::signal::ctrl_c() => return Ok(())
        }
    }
}

#[instrument(level = "info", skip_all, err(Debug))]
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



