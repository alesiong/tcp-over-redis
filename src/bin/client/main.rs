use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::{select, time};
use tokio_util::task::TaskTracker;
use tracing::{error, info, instrument, Level};

use tcp_over_redis::cache::connection::Connection;
use tcp_over_redis::cache::listener::Listener;
use tcp_over_redis::cache::redis::RedisClient;
use tcp_over_redis::error::ProxyError;
use tcp_over_redis::network;

#[cfg(feature = "tracing")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), ProxyError> {
    #[cfg(feature = "tracing")]
    console_subscriber::init();

    #[cfg(not(feature = "tracing"))]
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    tokio_main()
}


#[tokio::main]
async fn tokio_main() -> Result<(), ProxyError> {
    #[cfg(feature = "tracing")]
    let _profiler = dhat::Profiler::builder()
        .file_name(PathBuf::from("dhat-heap-client.json"))
        .build();
    
    let client = RedisClient::new("redis://127.0.0.1").await?;

    run_client(client).await?;

    Ok(())
}

#[instrument(level = "trace", skip_all, err(Debug))]
async fn run_client(client: RedisClient) -> Result<(), ProxyError> {
    let redis_timeout = Duration::from_secs(1);
    let connection_timeout = Duration::from_secs(30);

    let mut listener = Listener::listen(Some(Instant::now() + redis_timeout), redis_timeout, connection_timeout, Arc::new(client), 0).await?;

    loop {
        select! {
            accept = listener.accept(None) => {
                let conn = match accept {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!(?err, "error accepting connection");
                        continue;
                    }
                };
                info!("accepting connection");

                tokio::spawn(handle_connection(conn));
            }
            // TODO: error
            _ = tokio::signal::ctrl_c() => return Ok(())
        }
    }
}

#[instrument(level = "trace", fields(
    write_pipe = conn.write_pipe_name(), read_pipe = conn.read_pipe_name()
), skip_all, err(Debug))]
async fn handle_connection(conn: Connection) -> Result<(), ProxyError> {
    let timeout = Duration::from_secs(60);

    // TODO: timeout
    // let stream = TcpStream::connect("44.206.219.79:80").await?;
    let stream = TcpStream::connect("127.0.0.1:8000").await?;
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