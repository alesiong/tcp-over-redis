use const_format::concatcp;

pub(crate) const REDIS_KEY_PREFIX: &str = "redis-proxy";
pub(crate) const REDIS_KEY_LISTENING: &str = concatcp!(REDIS_KEY_PREFIX, ":listening:");
pub(crate) const REDIS_KEY_PREFIX_SERVER_HAND_SHAKE: &str = concatcp!(REDIS_KEY_PREFIX, ":server:handshake:");
pub(crate) const REDIS_KEY_PREFIX_CLIENT_HAND_SHAKE: &str = concatcp!(REDIS_KEY_PREFIX, ":client:handshake:");
pub(crate) const REDIS_KEY_PREFIX_SERVER_DATA: &str = concatcp!(REDIS_KEY_PREFIX, ":server:data:");
pub(crate) const REDIS_KEY_PREFIX_CLIENT_DATA: &str = concatcp!(REDIS_KEY_PREFIX, ":client:data:");
pub(crate) const REDIS_KEY_GLOBAL_REGISTRY: &str = concatcp!("{}:registry", REDIS_KEY_PREFIX);