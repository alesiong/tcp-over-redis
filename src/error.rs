use std::ffi::OsString;
use std::string::FromUtf8Error;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tracing::dispatcher::SetGlobalDefaultError;
use tracing::metadata::ParseLevelError;

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
    #[error("create pool error")]
    CreatePool(#[from]deadpool_redis::CreatePoolError),
    #[error("network error")]
    IoError(#[from]std::io::Error),
    #[error("system error")]
    System(String),
    #[error("encoding error")]
    Encoding(#[from]EncodingError),
    #[error("config error")]
    Config(#[from]toml::de::Error),
    #[error("config")]
    ParseLevelError(#[from]ParseLevelError),
}

impl<T> From<SendError<T>> for ProxyError {
    fn from(err: SendError<T>) -> Self {
        ProxyError::Channel(err.to_string())
    }
}

impl From<SetGlobalDefaultError> for ProxyError
{
    fn from(value: SetGlobalDefaultError) -> Self {
        ProxyError::System(value.to_string())
    }
}

#[derive(Error, Debug)]
pub enum EncodingError {
    #[error("vec")]
    FromUtf8Error(#[from]FromUtf8Error),
    #[error("os_string")]
    OsString(OsString),
}