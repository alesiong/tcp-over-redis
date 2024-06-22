use std::future::Future;
use std::time::{Duration, Instant};

pub(crate) async fn timeout_at<T>(deadline: Option<Instant>, future: impl Future<Output=T>) -> Result<T, tokio::time::error::Elapsed> {
    if let Some(deadline) = deadline {
        tokio::time::timeout_at(deadline.into(), future).await
    } else {
        Ok(future.await)
    }
}

pub(crate) async fn timeout<T>(duration: Duration, future: impl Future<Output=T>) -> Result<T, tokio::time::error::Elapsed> {
    tokio::time::timeout(duration, future).await
}
