use std::time::{Duration, Instant};

use redis::AsyncCommands;
use tokio::sync::{
    mpsc,
    mpsc::Receiver,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, instrument, Instrument, trace, trace_span, warn};

use crate::cache::redis::RedisClient;
use crate::error::ProxyError;
use crate::util::{timeout, timeout_at};

pub(crate) struct ReceiveHandle {
    pipe_name: String,
    result_rx: Receiver<Vec<u8>>,
    error_rx: Receiver<ProxyError>,
    cancellation_token: CancellationToken,
}

#[instrument(level = "trace", skip(cache, data), fields(len = data.len()), err(Debug))]
pub(crate) async fn send(deadline: Option<Instant>, connection_timeout: Duration, cache: &RedisClient,
                         pipe_name: &str, data: &[u8]) -> Result<(), ProxyError> {
    trace!("sending data");

    let mut cache = cache.new_connection().await?;
    timeout_at(deadline, async {
        cache.lpush(format!("{pipe_name}:data"), data).await?;

        if let Err(err) = cache.pexpire::<_, ()>(format!("{pipe_name}:data"), connection_timeout.as_millis() as i64).await {
            error!(?err, "error setting expire time, deleting queue");
            _ = cache.del::<_, ()>(format!("{pipe_name}:data")).await; // ignore error
        }
        Result::<_, ProxyError>::Ok(())
    }).await??;

    let pipe_name = pipe_name.to_string();
    tokio::spawn(async move {
        if let Err(err) = timeout_at(deadline, cache.publish::<_, _, ()>(format!("{pipe_name}:notify"), 0)).await {
            error!(?err, "error publishing notification");
        }
    });

    trace!("sent data");
    Ok(())
}

impl ReceiveHandle {
    #[instrument(level = "trace", skip(cache), err(Debug))]
    pub(crate) async fn start_receive(deadline: Option<Instant>, redis_timeout: Duration, cache: &RedisClient, pipe_name: &str) -> Result<ReceiveHandle, ProxyError> {
        trace!("start receiving");

        let mut data_sub = cache.new_pubsub().await?;
        timeout_at(deadline, data_sub.subscribe(format!("{pipe_name}:notify"))).await??;

        let (result_tx, result_rx) = mpsc::channel(32);
        let (error_tx, error_rx) = mpsc::channel(32);
        let pipe_name_string = pipe_name.to_string();

        let mut cache = cache.new_connection().await?;
        let token = CancellationToken::new();
        let token_inside = token.clone();

        let handle = tokio::spawn(async move {
            let mut data_chan = data_sub.on_message();
            let mut result = Result::<_, ProxyError>::Ok(());
            loop {
                tokio::select! {
                    _ = token_inside.cancelled() => {
                        trace!("stop receiving");
                        break;
                    }
                    Some(_) = data_chan.next() => {
                        trace!("received message");
                        let data: Vec<u8> = match timeout(redis_timeout, cache.rpop(format!("{pipe_name_string}:data"), None)).await
                            .map_err(ProxyError::from)
                            .and_then(|r| r.map_err(ProxyError::from)) {
                            Ok(data) => data,
                            Err(err) => {
                                warn!(?err, "error receiving data");
                                if let Err(err) = error_tx.send(err).await {
                                    result = Err(err.into());
                                    break;
                                }
                                continue;
                            }
                        };
                        trace!(len=data.len(), "received data");
                        if let Err(err) = result_tx.send(data).await {
                            result = Err(err.into());
                            break;
                        }
                    }
                }
            }
            // shutting down
            drop(data_chan);
            _ = data_sub.unsubscribe(format!("{pipe_name_string}:notify")).await; // ignore error
            result
        }
            .instrument(trace_span!(parent: None, "main_handle", pipe = pipe_name))
        );
        #[cfg(test)]
        crate::util::tracker::PIPE_RECEIVER.lock().await.push((pipe_name.to_string(), handle));

        Ok(ReceiveHandle {
            pipe_name: pipe_name.to_string(),
            result_rx,
            error_rx,
            cancellation_token: token,
        })
    }


    #[instrument(level = "trace", skip(self), err(Debug))]
    pub(crate) async fn receive(&mut self, deadline: Option<Instant>) -> Result<Vec<u8>, ProxyError> {
        let ddl = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now).into());
        tokio::select! {
            Some(data) = self.result_rx.recv() => {
                trace!(len=data.len(), pipe=self.pipe_name, "received data");
                Ok(data)
            }
            Some(err) = self.error_rx.recv() => {
                warn!(?err, "received error");
                Err(err)
            }
            _ = ddl, if deadline.is_some() => {
                warn!("timeout receiving");
                Err(ProxyError::Pipe("timeout receiving".to_string()))
            }
            else => {
                Err(ProxyError::Pipe("peer closed".to_string()))
            }
        }
    }
}

impl Drop for ReceiveHandle {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}


#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::Arc;
    use std::time::Duration;

    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    use crate::cache::pipe::{ReceiveHandle, send};
    use crate::cache::redis::RedisClient;

    #[tokio::test]
    async fn test_pipe() -> Result<(), anyhow::Error> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .init();

        {
            let client = redis::Client::open("redis://127.0.0.1")?;
            let cache = RedisClient::new_redis_client_for_test(client).await;

            let mut data: Vec<u32> = Vec::new();
            for _ in 0..30 {
                data.push(rand::random());
            }
            let data = Arc::new(data);

            let mut receive_handle = ReceiveHandle::start_receive(None, Duration::from_secs(1), &cache, "test").await?;

            let data_inner = Arc::clone(&data);

            let handle = tokio::spawn(async move {
                let mut i = 0;
                loop {
                    let result = receive_handle.receive(None).await.unwrap();
                    if result.is_empty() {
                        break;
                    }
                    // println!("{}", String::from_utf8_lossy(&result));
                    assert_eq!(data_inner[i].to_string().as_bytes(), &result);
                    i += 1;
                }
                // drop(receive_handle);
            });

            for i in data.deref() {
                send(None, Duration::from_secs(10), &cache, "test", i.to_string().as_bytes()).await.unwrap()
            }
            send(None, Duration::from_secs(10), &cache, "test", &[]).await.unwrap();

            tokio::join!(handle);

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let pipe_receiver = crate::util::tracker::PIPE_RECEIVER.lock().await;
        for (name, handle) in pipe_receiver.deref() {
            assert!(handle.is_finished(), "{} not finished", name);
        }

        Ok(())
    }
}