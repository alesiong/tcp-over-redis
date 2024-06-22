use redis::aio::PubSub;
use redis::Client;

use crate::error::ProxyError;

pub struct RedisClient {
    cache: Client,
    pool: deadpool_redis::Pool,
}

impl RedisClient {
    pub(crate) async fn new_connection(&self) -> Result<deadpool_redis::Connection, ProxyError> {
        Ok(self.pool.get().await?)
    }

    pub(crate) async fn new_pubsub(&self) -> Result<PubSub, ProxyError> {
        Ok(self.cache.get_async_pubsub().await?)
    }

    #[cfg(test)]
    pub(crate) async fn new_redis_client_for_test(cache: Client) -> Self {
        let config = deadpool_redis::Config::from_connection_info(cache.get_connection_info().clone());

        RedisClient {
            pool: config.create_pool(Some(deadpool_redis::Runtime::Tokio1)).unwrap(),
            cache,
        }
    }
}

