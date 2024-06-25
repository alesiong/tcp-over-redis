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
    pub(crate) static LISTENER_HANDLER: Mutex<Vec<(String, JoinHandle<()>)>> = Mutex::const_new(Vec::new());

    pub(crate) async fn get_leak_tasks() -> Vec<(String, String)> {
        let mut result = vec![];
        for (name, handle) in PIPE_RECEIVER.lock().await.iter() {
            if handle.is_finished() {
                continue;
            }
            result.push(("PIPE".to_string(), name.clone()));
        }
        for (name, handle) in LISTENER_HANDLER.lock().await.iter() {
            if handle.is_finished() {
                continue;
            }
            result.push(("LISTENER".to_string(), name.clone()));
        }

        result
    }
}