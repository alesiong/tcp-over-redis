use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("redis error")]
    Redis(#[from] redis::RedisError),
    #[error("timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("channel error")]
    Channel(String),
    #[error("pipe error")]
    Pipe(String),
    #[error("pool error")]
    Pool(#[from]deadpool_redis::PoolError),
}

impl<T> From<SendError<T>> for ProxyError {
    fn from(err: SendError<T>) -> Self {
        ProxyError::Channel(err.to_string())
    }
}

impl From<ProxyError> for std::io::Error {
    fn from(error: ProxyError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, error)
    }
}