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

#[cfg(test)]
pub(crate) mod tracker {
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;

    use crate::error::ProxyError;

    pub(crate) static PIPE_RECEIVER: Mutex<Vec<(String, JoinHandle<Result<(), ProxyError>>)>> = Mutex::const_new(Vec::new());
}