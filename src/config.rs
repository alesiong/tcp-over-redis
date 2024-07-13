use serde::Deserialize;

use crate::error::{EncodingError, ProxyError};

#[derive(Deserialize)]
pub struct ServerConfig {
    pub listen_port: u16,
    #[serde(default = "default_timeout")]
    pub timeout_second: u64,
    pub common: CommonConfig,
}

#[derive(Deserialize)]
pub struct ClientConfig {
    pub proxy_to_addr: String,
    #[serde(default = "default_timeout")]
    pub timeout_second: u64,
    pub common: CommonConfig,
}

#[derive(Deserialize)]
pub struct CommonConfig {
    pub redis_addr: String,
    pub redis_password: String,
    #[serde(default = "default_timeout_milli")]
    pub redis_timeout_milli: u64,
    pub log_level: String,
    pub log_path: String,
    #[serde(default = "default_timeout_60")]
    pub connection_timeout_second: u64,
    #[serde(default = "default_listen_shard")]
    pub listen_shard: u32,
}

pub fn load_config_from_file<T: serde::de::DeserializeOwned>(file: &str) -> Result<T, ProxyError> {
    let config_file = String::from_utf8(std::fs::read(file)?).map_err(EncodingError::from)?;
    Ok(toml::from_str(&config_file)?)
}


fn default_timeout() -> u64 {
    10
}

fn default_listen_shard() -> u32 {
    4
}

fn default_timeout_60() -> u64 {
    60
}

fn default_timeout_milli() -> u64 {
    1000
}