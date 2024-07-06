use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tracing::{debug, instrument};

use crate::cache::connection::{ConnectionReader, ConnectionWriter};
use crate::error::ProxyError;

#[instrument(level = "info", skip_all, err(Debug))]
pub async fn copy_from_net(mut net: OwnedReadHalf, conn: ConnectionWriter) -> Result<(), ProxyError> {
    let mut buf = vec![0u8; 0x10000];
    let r = async {
        loop {
            let n = net.read(&mut buf).await?;
            // TODO: deal with RST
            if n == 0 {
                // EOF
                debug!("eof");
                break;
            }
            conn.write(&buf[..n]).await?;
        }
        Ok::<(), ProxyError>(())
    }.await;
    conn.close().await?;
    r
}

#[instrument(level = "info", skip_all, err(Debug))]
pub async fn copy_from_conn(mut conn: ConnectionReader, mut net: OwnedWriteHalf) -> Result<(), ProxyError> {
    let r = async {
        loop {
            let buf = conn.read().await?;
            if buf.is_empty() {
                // EOF
                debug!("eof");
                break;
            }
            net.write_all(&buf).await?;
        }
        Ok::<(), ProxyError>(())
    }.await;

    net.shutdown().await?;
    r
}