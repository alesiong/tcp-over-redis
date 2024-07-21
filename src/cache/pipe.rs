use std::sync::Arc;
use std::time::{Duration, Instant};

use redis::AsyncCommands;
use tokio::sync::{
    mpsc,
    mpsc::Receiver,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, instrument, Instrument, trace, trace_span, warn};

use crate::cache::consts::REDIS_KEY_GLOBAL_REGISTRY;
use crate::cache::redis::RedisClient;
use crate::error::ProxyError;
use crate::util::{timeout, timeout_at};

pub(crate) struct ReceiveHandle {
    pipe_name: String,
    result_rx: Receiver<Vec<u8>>,
    error_rx: Receiver<ProxyError>,
    cancellation_token: CancellationToken,
}

#[instrument(level = "info", skip(cache, data), fields(len = data.len()), err(Debug))]
pub(crate) async fn send(deadline: Option<Instant>, connection_timeout: Duration, cache: &RedisClient,
                         pipe_name: &str, data: &[u8]) -> Result<(), ProxyError> {
    trace!("sending data");

    let mut cache = cache.new_connection().await?;
    timeout_at(deadline, async {
        redis::pipe().atomic()
            .lpush(format!("{pipe_name}:data"), data)
            .pexpire(format!("{pipe_name}:data"), connection_timeout.as_millis() as i64)
            .query_async(&mut cache).await?;
        Result::<_, ProxyError>::Ok(())
    }).await??;


    #[cfg(not(feature = "pubsub-registry"))]
    {
        let notify_key = format!("{pipe_name}:notify");
        tokio::spawn(async move {
            if let Err(err) = timeout_at(deadline, cache.publish::<_, _, ()>(notify_key, 0)).await {
                error!(?err, "error publishing notification");
            }
        });
    }

    #[cfg(feature = "pubsub-registry")]
    {
        let pipe_name = pipe_name.to_string();
        tokio::spawn(async move {
            if let Err(err) = timeout_at(deadline, cache.publish::<_, _, ()>(REDIS_KEY_GLOBAL_REGISTRY, pipe_name)).await {
                error!(?err, "error publishing notification");
            }
        });
    }

    trace!("sent data");
    Ok(())
}

impl ReceiveHandle {
    #[instrument(level = "info", skip(cache), err(Debug))]
    pub(crate) async fn start_receive(deadline: Option<Instant>, redis_timeout: Duration, cache: Arc<RedisClient>, pipe_name: &str) -> Result<ReceiveHandle, ProxyError> {
        trace!("start receiving");
        #[cfg(feature = "pubsub-registry")]
        let reg = {
            let reg = registry::get_registry();
            reg.init_subscribe(deadline, Arc::clone(&cache)).await?;
            reg
        };

        #[cfg(not(feature = "pubsub-registry"))]
        let mut data_sub = {
            let mut data_sub = cache.new_pubsub().await?;
            timeout_at(deadline, data_sub.subscribe(format!("{pipe_name}:notify"))).await??;
            data_sub
        };


        let (result_tx, result_rx) = mpsc::channel(32);
        let (error_tx, error_rx) = mpsc::channel(32);
        let data_key = format!("{pipe_name}:data");
        let notify_key = format!("{pipe_name}:notify");

        #[cfg(feature = "pubsub-registry")]
        let mut receiver = reg.register(pipe_name.to_string());
        let pipe_name_str = pipe_name.to_string();

        let token = CancellationToken::new();
        let token_inside = token.clone();

        let _handle = tokio::spawn(async move {
            #[cfg(not(feature = "pubsub-registry"))]
            let mut data_chan = data_sub.on_message();
            let mut result = Result::<_, ProxyError>::Ok(());
            loop {
                #[cfg(not(feature = "pubsub-registry"))]
                let fut = data_chan.next();
                #[cfg(feature = "pubsub-registry")]
                let fut = receiver.recv();
                tokio::select! {
                    _ = token_inside.cancelled() => {
                        trace!("stop receiving");
                        break;
                    }
                    recv = fut => {
                        if recv.is_some() {
                            trace!("received message");
                            let mut conn = cache.new_connection().await?;
                            let data: Vec<u8> = match timeout(redis_timeout, conn.rpop(&data_key, None)).await
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
                        } else {
                            trace!("stop receiving, receiver is closed");
                            break;
                        }
                    }
                }
            }
            // shutting down
            #[cfg(feature = "pubsub-registry")]
            reg.deregister(&pipe_name_str);
            #[cfg(not(feature = "pubsub-registry"))]
            {
                drop(data_chan);
                _ = data_sub.unsubscribe(notify_key).await; // ignore error
            }
            result
        }
            .instrument(trace_span!(parent: None, "main_handle", pipe = pipe_name))
        );
        #[cfg(test)]
        crate::util::tracker::PIPE_RECEIVER.lock().unwrap().push((pipe_name.to_string(), _handle));

        Ok(ReceiveHandle {
            pipe_name: pipe_name.to_string(),
            result_rx,
            error_rx,
            cancellation_token: token,
        })
    }


    #[instrument(level = "info", skip(self), fields(pipe = self.pipe_name), err(Debug))]
    pub(crate) async fn receive(&mut self, deadline: Option<Instant>) -> Result<Vec<u8>, ProxyError> {
        let ddl = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now).into());
        tokio::select! {
            Some(data) = self.result_rx.recv() => {
                trace!(len=data.len(), "received data");
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

    pub(crate) fn pipe_name(&self) -> &str {
        &self.pipe_name
    }
}

impl Drop for ReceiveHandle {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

#[cfg(feature = "pubsub-registry")]
mod registry {
    use std::sync::{Arc, OnceLock};
    use std::time::Instant;

    use dashmap::DashMap;
    use tokio::sync::{mpsc, OnceCell};
    use tokio_stream::StreamExt;
    use tracing::{debug, error};

    use crate::cache::consts::{REDIS_KEY_GLOBAL_REGISTRY, REDIS_KEY_PREFIX};
    use crate::cache::redis::RedisClient;
    use crate::error::ProxyError;
    use crate::util::timeout_at;

    #[derive(Default)]
    pub(super) struct Registry {
        map: DashMap<String, mpsc::Sender<()>>,
        subscriber: OnceCell<()>,
    }

    impl Registry {
        pub(super) async fn init_subscribe(&'static self, deadline: Option<Instant>, cache: Arc<RedisClient>) -> Result<(), ProxyError> {
            self.subscriber.get_or_try_init(|| async {
                let mut pub_sub = cache.new_pubsub().await?;

                timeout_at(deadline, pub_sub.subscribe(REDIS_KEY_GLOBAL_REGISTRY)).await??;

                tokio::spawn(self.subscribe(pub_sub));
                Ok::<_, ProxyError>(())
            }).await?;

            Ok(())
        }

        pub(super) fn register(&self, pipe_name: String) -> mpsc::Receiver<()> {
            let (tx, rx) = mpsc::channel(32);
            self.map.insert(pipe_name, tx);
            rx
        }
        pub(super) fn deregister(&self, pipe_name: &str) {
            self.map.remove(pipe_name);
        }

        async fn subscribe(&self, mut pub_sub: redis::aio::PubSub) {
            let mut chan = pub_sub.on_message();
            while let Some(msg) = chan.next().await {
                let pipe_name: String = match msg.get_payload() {
                    Ok(str) => str,
                    Err(err) => {
                        error!(?err, "error receiving notification");
                        continue;
                    }
                };
                if let Some(tx) = self.map.get(&pipe_name) {
                    if let Err(err) = tx.send(()).await {
                        error!(?err, "error sending notification");
                    }
                } else {
                    debug!(pipe_name, "not found such register");
                }
            }
            drop(chan);
            _ = pub_sub.unsubscribe(REDIS_KEY_GLOBAL_REGISTRY).await; // ignore error
        }
    }

    pub(super) fn get_registry() -> &'static Registry {
        static REGISTRY: OnceLock<Registry> = OnceLock::new();
        REGISTRY.get_or_init(Default::default)
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tracing::Level;

    use crate::cache::pipe::{ReceiveHandle, send};
    use crate::cache::redis::RedisClient;

    #[tokio::test]
    async fn test_pipe() -> Result<(), anyhow::Error> {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .finish();
        tracing::subscriber::set_global_default(subscriber)?;

        {
            let client = redis::Client::open("redis://127.0.0.1")?;
            let cache = Arc::new(RedisClient::new_redis_client_for_test(client).await);

            let mut data: Vec<u32> = Vec::new();
            for _ in 0..30 {
                data.push(rand::random());
            }
            let data = Arc::new(data);
            let data_inner = Arc::clone(&data);

            let mut receive_handle = ReceiveHandle::start_receive(None, Duration::from_secs(1), Arc::clone(&cache), "test").await?;


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

            for i in data.iter() {
                send(None, Duration::from_secs(10), &cache, "test", i.to_string().as_bytes()).await.unwrap()
            }
            send(None, Duration::from_secs(10), &cache, "test", &[]).await.unwrap();

            tokio::join!(handle);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        let leak_tasks = crate::util::tracker::get_leak_tasks().await;

        assert!(leak_tasks.is_empty(), "leak tasks: {:?}", leak_tasks);

        Ok(())
    }
}