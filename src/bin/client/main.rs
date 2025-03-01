use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::{select, time};
use tokio::net::TcpStream;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, instrument};

use tcp_over_redis::{config, log, network};
use tcp_over_redis::cache::connection::Connection;
use tcp_over_redis::cache::listener::Listener;
use tcp_over_redis::cache::redis::RedisClient;
use tcp_over_redis::config::ClientConfig;
use tcp_over_redis::error::{EncodingError, ProxyError};

#[cfg(feature = "profiling")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), ProxyError> {
    let config_file = std::env::var_os("CONFIG")
        .map(OsString::into_string).transpose().map_err(EncodingError::OsString)?
        .unwrap_or_else(|| "client_config.toml".to_string());
    let client_config: ClientConfig = config::load_config_from_file(&config_file)?;

    #[cfg(feature = "profiling")]
    console_subscriber::init();

    #[cfg(not(feature = "profiling"))]
    log::init_log(&client_config.common)?;

    tokio_main(client_config)
}


#[tokio::main]
async fn tokio_main(client_config: ClientConfig) -> Result<(), ProxyError> {
    #[cfg(feature = "profiling")]
    let _profiler = dhat::Profiler::builder()
        .file_name(PathBuf::from("dhat-heap-client.json"))
        .build();

    let redis_config = if client_config.common.redis_password.is_empty() {
        format!("redis://{}", client_config.common.redis_addr)
    } else {
        format!("redis://:{}@{}", client_config.common.redis_password, client_config.common.redis_addr)
    };

    let client = RedisClient::new(redis_config).await?;

    run_client(client, client_config).await?;

    Ok(())
}

#[instrument(level = "info", skip_all, err(Debug))]
async fn run_client(client: RedisClient, client_config: ClientConfig) -> Result<(), ProxyError> {
    let redis_timeout = Duration::from_millis(client_config.common.redis_timeout_milli);
    let connection_timeout = Duration::from_secs(client_config.common.connection_timeout_second);
    let timeout = Duration::from_secs(client_config.timeout_second);

    let client = Arc::new(client);
    let tracker = TaskTracker::new();
    let proxy_to_addr = Arc::new(client_config.proxy_to_addr);

    for i in 0..client_config.common.listen_shard {
        let mut listener = Listener::listen(Some(Instant::now() + redis_timeout), redis_timeout, connection_timeout, Arc::clone(&client), i).await?;
        let proxy_to_addr = Arc::clone(&proxy_to_addr);
        tracker.spawn(async move {
            loop {
                let proxy_to_addr = Arc::clone(&proxy_to_addr);
                select! {
                    accept = listener.accept(None) => {
                        let conn = match accept {
                            Ok(conn) => conn,
                            Err(err) => {
                                error!(?err, "error accepting connection");
                                continue;
                            }
                        };
                       
                        debug!(write_pipe=conn.write_pipe_name(), "accepting connection");

                        tokio::spawn(handle_connection(conn, timeout, connection_timeout, proxy_to_addr));
                    }
                    _ = tokio::signal::ctrl_c() => return
                }
            }
        });
    }
    tracker.close();
    tracker.wait().await;
    Ok(())
}

#[instrument(level = "info", fields(
    write_pipe = conn.write_pipe_name(), read_pipe = conn.read_pipe_name()
), skip_all, err(Debug))]
async fn handle_connection(conn: Connection, timeout: Duration, connection_timeout: Duration, proxy_to_addr: Arc<String>) -> Result<(), ProxyError> {
    let stream = time::timeout(timeout, TcpStream::connect(proxy_to_addr.as_str())).await??;
    let (read_net, write_net) = stream.into_split();
    let (read_conn, write_conn) = conn.split();

    let copy1 = network::copy(read_net, write_conn);
    let copy2 = network::copy(read_conn, write_net);

    let tracker = TaskTracker::new();
    tracker.spawn(copy1);
    tracker.spawn(copy2);
    tracker.close();

    time::timeout(connection_timeout, tracker.wait()).await?;

    Ok(())
}
