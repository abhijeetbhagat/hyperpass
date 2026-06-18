use log::*;
use tokio::task::JoinHandle;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub struct ShutdownHandler {
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl ShutdownHandler {
    pub fn new() -> Self {
        Self {
            cancellation_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn shutdown_signalled(&self) {
        self.cancellation_token.cancelled().await;
    }

    pub async fn shutdown(&self) {
        info!("waiting for all tasks to complete ...");
        self.cancellation_token.cancel();
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }

    pub fn spawn<F>(&self, f: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static + Send,
        F::Output: Send + 'static,
    {
        self.task_tracker.spawn(f)
    }
}
