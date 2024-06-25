use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, instrument, trace};

use crate::cache::connection::Connection;
use crate::cache::consts::{REDIS_KEY_LISTENING, REDIS_KEY_PREFIX_CLIENT_DATA, REDIS_KEY_PREFIX_SERVER_DATA, REDIS_KEY_PREFIX_SERVER_HAND_SHAKE};
use crate::cache::pipe;
use crate::cache::pipe::ReceiveHandle;
use crate::cache::redis::RedisClient;
use crate::error::ProxyError;
use crate::util::timeout_at;

pub struct Listener {
    connection_rx: mpsc::Receiver<Connection>,
    cancel: CancellationToken,
}


impl Listener {
    #[instrument(level = "trace", skip(cache), err(Debug))]
    pub async fn listen(deadline: Option<Instant>, redis_timeout: Duration, connection_timeout: Duration,
                        cache: Arc<RedisClient>, shard_id: u32) -> Result<Listener, ProxyError> {
        let listening = format!("{}{}", REDIS_KEY_LISTENING, shard_id);
        let mut listener_handle = ReceiveHandle::start_receive(deadline, redis_timeout, &cache, &listening).await?;
        let (connection_tx, connection_rx) = mpsc::channel(16);

        let cancel = CancellationToken::new();

        let inner_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                let seq_bytes = tokio::select! {
                    recv = listener_handle.receive(None) => {
                        match recv {
                            Ok(v) => v,
                            Err(err) => {
                                error!("error receiving {:?}", err);
                                continue;
                            }
                        }
                    }
                    _ = inner_cancel.cancelled() => {
                        trace!("stop listening");
                        break;
                    }
                };

                let seq = u64::from_be_bytes(seq_bytes.try_into().unwrap()); // TODO: error

                tokio::spawn(handshake(seq, redis_timeout, connection_timeout, Arc::clone(&cache), connection_tx.clone()));
            }
        });

        #[cfg(test)]
        crate::util::tracker::LISTENER_HANDLER.lock().await.push((format!("listener:{}", shard_id), handle));


        Ok(Listener {
            connection_rx,
            cancel,
        })
    }
    #[instrument(level = "trace", skip(self), err(Debug))]
    pub async fn accept(&mut self, deadline: Option<Instant>) -> Result<Connection, ProxyError> {
        timeout_at(deadline, self.connection_rx.recv()).await?
            .ok_or(ProxyError::Channel("channel closed".to_string()))
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}


#[instrument(level = "trace", skip(cache, connection_tx), err(Debug))]
async fn handshake(seq: u64, redis_timeout: Duration, connection_timeout: Duration,
                   cache: Arc<RedisClient>, connection_tx: mpsc::Sender<Connection>) -> Result<(), ProxyError> {
    trace!("accepting connection");

    let deadline = Instant::now() + redis_timeout * 4;
    let server_data_key = format!("{}{}", REDIS_KEY_PREFIX_SERVER_DATA, seq);
    let server_data_handle = ReceiveHandle::start_receive(Some(deadline), redis_timeout, &cache, &server_data_key).await?;
    trace!("start receiving server data");

    let server_hand_shake_key = format!("{}{}", REDIS_KEY_PREFIX_SERVER_HAND_SHAKE, seq);
    pipe::send(Some(deadline), connection_timeout, &cache, &server_hand_shake_key, b"0").await?;
    trace!("sent server hand shake");

    connection_tx.send(Connection {
        cache,
        redis_timeout,
        connection_timeout,
        read_pipe: server_data_handle,
        write_pipe_name: format!("{}{}", REDIS_KEY_PREFIX_CLIENT_DATA, seq),
    }).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    use crate::cache::connection::Connection;
    use crate::cache::listener::Listener;
    use crate::cache::redis::RedisClient;

    #[tokio::test]
    async fn test_connection() -> Result<(), anyhow::Error> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_ansi(false))
            .init();


        {
            let client = redis::Client::open("redis://127.0.0.1")?;
            let cache = RedisClient::new_redis_client_for_test(client).await;
            let cache = Arc::new(cache);
            let mut listener = Listener::listen(None, Duration::from_secs(1), Duration::from_secs(10), Arc::clone(&cache), 0).await?;

            let mut data: Vec<u32> = Vec::new();
            for _ in 0..30 {
                data.push(rand::random());
            }
            let data = Arc::new(data);
            
            let data_inner = Arc::clone(&data);
            let handle_send = tokio::spawn(async move {
                let conn = Connection::dial(None, Duration::from_secs(1), Duration::from_secs(10), cache, 1).await.unwrap();
                let (write, _read) = conn.split();

                for i in data_inner.iter() {
                    write.write(format!("{}", i).as_bytes()).await.unwrap();
                }
                write.close().await.unwrap();
            });

            let conn = listener.accept(None).await.unwrap();
            let (_write, mut read) = conn.split();
            let mut i = 0;
            while let Ok(r) = read.read().await {
                if r.is_empty() {
                    // eof
                    break;
                }
                // debug!("listener receiving {}", String::from_utf8_lossy(&r));
                assert_eq!(data[i].to_string().as_bytes(), &r);
                i += 1;
            }

            tokio::join!(handle_send);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        let leak_tasks = crate::util::tracker::get_leak_tasks().await;

        assert!(leak_tasks.is_empty(), "leak tasks: {:?}", leak_tasks);

        Ok(())
    }
}