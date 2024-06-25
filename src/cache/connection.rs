use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;
use tracing::{Instrument, instrument, trace, trace_span};

use crate::cache::consts::{REDIS_KEY_LISTENING, REDIS_KEY_PREFIX_CLIENT_DATA, REDIS_KEY_PREFIX_SERVER_DATA, REDIS_KEY_PREFIX_SERVER_HAND_SHAKE};
use crate::cache::pipe;
use crate::cache::pipe::ReceiveHandle;
use crate::cache::redis::RedisClient;
use crate::error::ProxyError;

pub struct Connection {
    pub(crate) cache: Arc<RedisClient>,
    pub(crate) redis_timeout: Duration,
    pub(crate) connection_timeout: Duration,
    pub(crate) read_pipe: ReceiveHandle,
    pub(crate) write_pipe_name: String,
}

pub(crate) struct ConnectionWriter {
    cache: Arc<RedisClient>,
    redis_timeout: Duration,
    connection_timeout: Duration,
    write_pipe_name: String,
}

pub(crate) struct ConnectionReader {
    redis_timeout: Duration,
    connection_timeout: Duration,
    read_pipe: ReceiveHandle,
}

impl Connection {
    #[instrument(level = "trace", skip(cache), err(Debug))]
    pub async fn dial(deadline: Option<Instant>, redis_timeout: Duration,
                      connection_timeout: Duration, cache: Arc<RedisClient>, shard_count: i32) -> Result<Connection, ProxyError> {
        let seq: u64 = rand::random();
        let seq_bytes = &seq.to_be_bytes();

        async {
            // TODO: log
            let server_handshake_pipe = format!("{}{}", REDIS_KEY_PREFIX_SERVER_HAND_SHAKE, seq);
            let recv_handshake = ReceiveHandle::start_receive(deadline, redis_timeout, &cache, &server_handshake_pipe);

            let client_data_pipe = format!("{}{}", REDIS_KEY_PREFIX_CLIENT_DATA, seq);
            let recv_client_data = ReceiveHandle::start_receive(deadline, redis_timeout, &cache, &client_data_pipe);

            let (mut server_handshake_handler, client_data_handler) = tokio::try_join!(recv_handshake, recv_client_data)?;
            trace!("started receive handshake and client data");
            
            let listening = format!("{}{}", REDIS_KEY_LISTENING, rand::thread_rng().gen_range(0..shard_count));
            let conn_req = pipe::send(deadline, connection_timeout, &cache, &listening, seq_bytes);
            let recv_handshake = server_handshake_handler.receive(deadline);

            tokio::try_join!(conn_req, recv_handshake)?;
            trace!("sent listening to {} and received handshake", listening);

            Ok(Connection {
                cache,
                redis_timeout,
                connection_timeout,
                read_pipe: client_data_handler,
                write_pipe_name: format!("{}{}", REDIS_KEY_PREFIX_SERVER_DATA, seq),
            })
        }
            .instrument(trace_span!("dial", seq))
            .await
    }

    pub fn split(self) -> (ConnectionWriter, ConnectionReader) {
        (
            ConnectionWriter {
                cache: self.cache,
                redis_timeout: self.redis_timeout,
                connection_timeout: self.connection_timeout,
                write_pipe_name: self.write_pipe_name,
            },
            ConnectionReader {
                redis_timeout: self.redis_timeout,
                connection_timeout: self.connection_timeout,
                read_pipe: self.read_pipe,
            }
        )
    }
}

impl ConnectionReader {
    #[instrument(level="trace", skip(self), fields(pipe = self.read_pipe.pipe_name()), err(Debug))]
    pub async fn read(&mut self) -> Result<Vec<u8>, ProxyError> {
        self.read_pipe.receive(Some(Instant::now() + self.connection_timeout)).await
    }
}

impl ConnectionWriter {
    #[instrument(level="trace", skip(self, data), fields(pipe = self.write_pipe_name, len = data.len()), err(Debug))]
    pub async fn write(&self, data: &[u8]) -> Result<(), ProxyError> {
        pipe::send(Some(Instant::now() + self.redis_timeout), self.connection_timeout, &self.cache, &self.write_pipe_name, data).await?;

        Ok(())
    }
    
    pub async fn close(self) -> Result<(), ProxyError> {
        self.write(&[]).await
    }
}