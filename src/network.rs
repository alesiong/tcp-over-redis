use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp;
use tracing::{debug, instrument};

use crate::cache::connection::{ConnectionReader, ConnectionWriter};
use crate::error::ProxyError;

#[instrument(level = "info", skip_all, err(Debug))]
pub async fn copy(mut read: impl Readable, mut write: impl WriteClosable) -> Result<(), ProxyError> {
    let r = async {
        loop {
            let data = read.read().await?;
            if data.is_empty() {
                // EOF
                debug!("eof");
                break;
            }
            write.write(&data).await?;
        }
        Ok::<(), ProxyError>(())
    }.await;
    write.close().await?;
    r
}

trait Readable {
    async fn read(&mut self) -> Result<Vec<u8>, ProxyError>;
}

trait WriteClosable {
    async fn close(self) -> Result<(), ProxyError>;
    async fn write(&mut self, data: &[u8]) -> Result<(), ProxyError>;
}


impl Readable for tcp::OwnedReadHalf {
    async fn read(&mut self) -> Result<Vec<u8>, ProxyError> {
        let mut buf = vec![0u8; 0x10000];
        let n = AsyncReadExt::read(self, &mut buf).await?;
        buf.truncate(n);
        Ok(buf)
    }
}

impl Readable for ConnectionReader {
    async fn read(&mut self) -> Result<Vec<u8>, ProxyError> {
        ConnectionReader::read(self).await
    }
}

impl WriteClosable for tcp::OwnedWriteHalf {
    async fn close(mut self) -> Result<(), ProxyError> {
        self.shutdown().await?;
        Ok(())
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), ProxyError> {
        self.write_all(data).await?;
        Ok(())
    }
}

impl WriteClosable for ConnectionWriter {
    async fn close(self) -> Result<(), ProxyError> {
        ConnectionWriter::close(self).await
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), ProxyError> {
        ConnectionWriter::write(self, data).await
    }
}