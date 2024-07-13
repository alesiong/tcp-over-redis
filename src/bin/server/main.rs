use std::ffi::OsString;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::{select, time};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::task::TaskTracker;
use tracing::{error, info, instrument};

use tcp_over_redis::{config, log, network};
use tcp_over_redis::cache::connection::Connection;
use tcp_over_redis::cache::redis::RedisClient;
use tcp_over_redis::config::ServerConfig;
use tcp_over_redis::error::{EncodingError, ProxyError};

#[cfg(feature = "profiling")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), ProxyError> {
    let config_file = std::env::var_os("CONFIG")
        .map(OsString::into_string).transpose().map_err(EncodingError::OsString)?
        .unwrap_or_else(|| "server_config.toml".to_string());
    let server_config: ServerConfig = config::load_config_from_file(&config_file)?;


    #[cfg(feature = "profiling")]
    console_subscriber::ConsoleLayer::builder().with_default_env()
        .server_addr(SocketAddr::from_str("127.0.0.1:6670").unwrap())
        .init();
    #[cfg(not(feature = "profiling"))]
    log::init_log(&server_config.common)?;

    tokio_main(server_config)
}

#[tokio::main]
async fn tokio_main(server_config: ServerConfig) -> Result<(), ProxyError> {
    #[cfg(feature = "profiling")]
    let _profiler = dhat::Profiler::builder()
        .file_name(PathBuf::from("dhat-heap-server.json"))
        .build();
    let redis_config = if server_config.common.redis_password.is_empty() {
        format!("redis://{}", server_config.common.redis_addr)
    } else {
        format!("redis://:{}@{}", server_config.common.redis_password, server_config.common.redis_addr)
    };

    let client = RedisClient::new(redis_config).await?;

    run_server(client, server_config).await?;

    Ok(())
}

#[instrument(level = "info", skip_all, err(Debug))]
async fn run_server(client: RedisClient, server_config: ServerConfig) -> Result<(), ProxyError> {
    let listener = TcpListener::bind(("0.0.0.0", server_config.listen_port)).await?;
    let client = Arc::new(client);
    let server_config = Arc::new(server_config);

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

                info!(remote=sock.to_string(), "accepting connection");

                tokio::spawn(handle_connection(Arc::clone(&client), stream, Arc::clone(&server_config)));
            }
            // TODO: error
            _ = tokio::signal::ctrl_c() => return Ok(())
        }
    }
}

#[instrument(level = "info", skip_all, err(Debug))]
async fn handle_connection(client: Arc<RedisClient>, stream: TcpStream, server_config: Arc<ServerConfig>) -> Result<(), ProxyError> {
    let timeout = Duration::from_secs(server_config.timeout_second);
    let redis_timeout = Duration::from_millis(server_config.common.redis_timeout_milli);
    let connection_timeout = Duration::from_secs(server_config.common.connection_timeout_second);
    let connection = Connection::dial(Some(Instant::now() + redis_timeout * 8), redis_timeout, connection_timeout, client, server_config.common.listen_shard).await?;

    let (read_net, write_net) = stream.into_split();
    let (read_conn, write_conn) = connection.split();

    let copy1 = network::copy(read_net, write_conn);
    let copy2 = network::copy(read_conn, write_net);

    let tracker = TaskTracker::new();
    tracker.spawn(copy1);
    tracker.spawn(copy2);
    tracker.close();

    time::timeout(timeout, tracker.wait()).await?;

    Ok(())
}



