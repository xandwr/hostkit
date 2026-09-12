//! Native asynchronous task ownership and scheduling.
//!
//! This module tracks work started by a request context and preserves cancellation,
//! cleanup, and structured results throughout each operation's lifetime.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};

use tokio::sync::{Notify, oneshot};
use tokio::task::{AbortHandle, JoinError, JoinHandle};

use super::cancellation::CancellationToken;
use super::error::{ErrorCode, RuntimeError, RuntimeResult};

type TaskId = u64;

/// Owns the cancellation state and asynchronous work for one request.
pub struct RequestContext {
    inner: Arc<RequestContextInner>,
}

impl RequestContext {
    /// Creates an active request context with no owned tasks.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RequestContextInner {
                cancellation: CancellationToken::new(),
                state: Mutex::new(ContextState {
                    accepting_tasks: true,
                    next_task_id: 0,
                    tasks: HashMap::new(),
                }),
                idle: Notify::new(),
            }),
        }
    }

    /// Returns a read-only view of this request's cancellation state.
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    /// Starts and registers work owned by this request.
    pub fn spawn<F, T>(&self, future: F) -> RuntimeResult<OwnedTask<T>>
    where
        F: Future<Output = RuntimeResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let task_cancellation = self.inner.cancellation.clone();
        self.spawn_registered(async move {
            tokio::select! {
                biased;
                _ = task_cancellation.cancelled() => Err(RuntimeError::cancelled()),
                result = future => result,
            }
        })
    }

    pub(crate) fn spawn_cooperative<Factory, TaskFuture, T>(
        &self,
        factory: Factory,
    ) -> RuntimeResult<OwnedTask<T>>
    where
        Factory: FnOnce(CancellationToken) -> TaskFuture + Send + 'static,
        TaskFuture: Future<Output = RuntimeResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let task_cancellation = self.inner.cancellation.clone();
        self.spawn_registered(async move {
            let future = factory(task_cancellation.clone());
            tokio::pin!(future);

            tokio::select! {
                biased;
                _ = task_cancellation.cancelled() => {
                    match future.await {
                        Err(error) if error.code() != ErrorCode::Cancelled => Err(error),
                        _ => Err(RuntimeError::cancelled()),
                    }
                }
                result = &mut future => result,
            }
        })
    }

    fn spawn_registered<F, T>(&self, future: F) -> RuntimeResult<OwnedTask<T>>
    where
        F: Future<Output = RuntimeResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        let mut state = self.inner.state();
        if !state.accepting_tasks {
            return Err(RuntimeError::cancelled());
        }

        let task_id = state.next_task_id;
        state.next_task_id = state
            .next_task_id
            .checked_add(1)
            .expect("request task identifier exhausted");

        let cancellation = self.inner.cancellation.clone();
        let context = Arc::downgrade(&self.inner);
        let (start_sender, start_receiver) = oneshot::channel();
        let handle = tokio::spawn(async move {
            if start_receiver.await.is_err() {
                return Err(RuntimeError::task_failed("task registration failed"));
            }

            let _registration = TaskRegistration { context, task_id };
            future.await
        });

        state.tasks.insert(task_id, handle.abort_handle());
        drop(state);
        let _ = start_sender.send(());

        Ok(OwnedTask {
            handle,
            cancellation,
        })
    }

    /// Cancels the request and waits until every owned task has stopped.
    ///
    /// Router-dispatched handlers are allowed to finish resource cleanup before
    /// stopping, so this remains pending until each cooperative handler resolves.
    pub async fn cancel(&self) {
        {
            let mut state = self.inner.state();
            state.accepting_tasks = false;
            self.inner.cancellation.cancel();
        }
        self.inner.wait_until_idle().await;
    }

    /// Returns the number of tasks currently owned by this request.
    pub fn task_count(&self) -> usize {
        self.inner.state().tasks.len()
    }
}

impl Default for RequestContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for RequestContext {
    fn drop(&mut self) {
        self.inner.cancellation.cancel();
        let mut state = self.inner.state();
        state.accepting_tasks = false;
        for (_, task) in state.tasks.drain() {
            task.abort();
        }
    }
}

/// The boundary result of a task that remains owned by its request context.
#[must_use = "dropping the result handle does not cancel its request-owned task"]
pub struct OwnedTask<T> {
    handle: JoinHandle<RuntimeResult<T>>,
    cancellation: CancellationToken,
}

impl<T> Future for OwnedTask<T> {
    type Output = RuntimeResult<T>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.handle).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(error)) => {
                Poll::Ready(Err(map_join_error(error, self.cancellation.is_cancelled())))
            }
        }
    }
}

struct RequestContextInner {
    cancellation: CancellationToken,
    state: Mutex<ContextState>,
    idle: Notify,
}

impl RequestContextInner {
    fn state(&self) -> MutexGuard<'_, ContextState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn unregister(&self, task_id: TaskId) {
        let became_idle = {
            let mut state = self.state();
            state.tasks.remove(&task_id);
            state.tasks.is_empty()
        };
        if became_idle {
            self.idle.notify_waiters();
        }
    }

    async fn wait_until_idle(&self) {
        loop {
            let idle = self.idle.notified();
            if self.state().tasks.is_empty() {
                return;
            }
            idle.await;
        }
    }
}

struct ContextState {
    accepting_tasks: bool,
    next_task_id: TaskId,
    tasks: HashMap<TaskId, AbortHandle>,
}

struct TaskRegistration {
    context: Weak<RequestContextInner>,
    task_id: TaskId,
}

impl Drop for TaskRegistration {
    fn drop(&mut self) {
        if let Some(context) = self.context.upgrade() {
            context.unregister(self.task_id);
        }
    }
}

fn map_join_error(error: JoinError, request_cancelled: bool) -> RuntimeError {
    if error.is_panic() {
        RuntimeError::task_failed("owned task panicked")
    } else if request_cancelled || error.is_cancelled() {
        RuntimeError::cancelled()
    } else {
        RuntimeError::task_failed("owned task stopped without a result")
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::mpsc;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::runtime::error::ErrorCode;

    struct DropProbe(mpsc::Sender<()>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[tokio::test]
    async fn cancellation_drops_task_cleans_context_and_crosses_boundary() {
        let context = RequestContext::new();
        let (started_sender, started_receiver) = oneshot::channel();
        let (dropped_sender, dropped_receiver) = mpsc::channel();

        let task = context
            .spawn(async move {
                let _drop_probe = DropProbe(dropped_sender);
                let _ = started_sender.send(());
                pending::<RuntimeResult<()>>().await
            })
            .unwrap();

        timeout(Duration::from_secs(1), started_receiver)
            .await
            .expect("owned task did not start")
            .expect("owned task stopped before reporting startup");
        assert_eq!(context.task_count(), 1);

        timeout(Duration::from_secs(1), context.cancel())
            .await
            .expect("owned task did not stop after cancellation");

        dropped_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("owned task future was not dropped");
        assert_eq!(context.task_count(), 0);
        assert_eq!(task.await.unwrap_err().code(), ErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn cancelled_context_rejects_new_tasks() {
        let context = RequestContext::new();
        context.cancel().await;

        let error = match context.spawn(async { Ok(()) }) {
            Ok(_) => panic!("cancelled context accepted a new task"),
            Err(error) => error,
        };

        assert_eq!(error.code(), ErrorCode::Cancelled);
        assert_eq!(context.task_count(), 0);
    }

    #[tokio::test]
    async fn panic_is_mapped_to_a_structured_boundary_error() {
        let context = RequestContext::new();
        let task = context
            .spawn(async {
                panic!("dummy panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();

        let error = task.await.unwrap_err();

        assert_eq!(error.code(), ErrorCode::TaskFailed);
        assert_eq!(error.message(), "owned task panicked");
        assert_eq!(context.task_count(), 0);
    }

    #[tokio::test]
    async fn cooperative_cancellation_preserves_cleanup_failure() {
        let context = Arc::new(RequestContext::new());
        let (started_sender, started_receiver) = oneshot::channel();
        let task = context
            .spawn_cooperative(move |cancellation| async move {
                let _ = started_sender.send(());
                cancellation.cancelled().await;
                Err::<(), _>(RuntimeError::task_failed("cleanup failed"))
            })
            .unwrap();
        started_receiver.await.unwrap();
        let cancelling_context = Arc::clone(&context);

        timeout(
            Duration::from_secs(1),
            tokio::spawn(async move { cancelling_context.cancel().await }),
        )
        .await
        .expect("request cancellation did not finish")
        .expect("request cancellation task failed");

        let error = task.await.unwrap_err();
        assert_eq!(error.code(), ErrorCode::TaskFailed);
        assert_eq!(error.message(), "cleanup failed");
        assert_eq!(context.task_count(), 0);
    }

    #[tokio::test]
    async fn dropping_context_hard_aborts_uncooperative_task() {
        let context = RequestContext::new();
        let (started_sender, started_receiver) = oneshot::channel();
        let (dropped_sender, dropped_receiver) = mpsc::channel();
        let task = context
            .spawn_cooperative(move |_| async move {
                let _drop_probe = DropProbe(dropped_sender);
                let _ = started_sender.send(());
                pending::<RuntimeResult<()>>().await
            })
            .unwrap();
        started_receiver.await.unwrap();

        drop(context);

        let error = timeout(Duration::from_secs(1), task)
            .await
            .expect("uncooperative task was not aborted")
            .unwrap_err();
        dropped_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("uncooperative task future was not dropped");
        assert_eq!(error.code(), ErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn panic_during_cooperative_cleanup_is_not_hidden_by_cancellation() {
        let context = RequestContext::new();
        let (started_sender, started_receiver) = oneshot::channel();
        let task = context
            .spawn_cooperative(move |cancellation| async move {
                let _ = started_sender.send(());
                cancellation.cancelled().await;
                panic!("dummy cleanup panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();
        started_receiver.await.unwrap();

        timeout(Duration::from_secs(1), context.cancel())
            .await
            .expect("request cancellation did not finish after cleanup panic");

        let error = task.await.unwrap_err();
        assert_eq!(error.code(), ErrorCode::TaskFailed);
        assert_eq!(error.message(), "owned task panicked");
        assert_eq!(context.task_count(), 0);
    }
}
