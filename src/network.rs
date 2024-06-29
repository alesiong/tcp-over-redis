use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::cache::connection::{ConnectionReader, ConnectionWriter};
use crate::error::ProxyError;

pub async fn copy_from_net(mut net: OwnedReadHalf, conn: ConnectionWriter) -> Result<(), ProxyError> {
    let mut buf = vec![0u8; 0x10000];
    loop {
        let n = net.read(&mut buf).await?;
        // TODO: deal with RST
        if n == 0 {
            // EOF
            // TODO: log
            break;
        }
        conn.write(&buf[..n]).await?
    }
    conn.close().await?;
    Ok(())
}

pub async fn copy_from_conn(mut conn: ConnectionReader, mut net: OwnedWriteHalf) -> Result<(), ProxyError> {
    loop {
        let buf = conn.read().await?;
        if buf.is_empty() {
            // EOF
            // TODO: log
            break;
        }
        net.write_all(&buf).await?;
    }

    net.shutdown().await?;

    Ok(())
}