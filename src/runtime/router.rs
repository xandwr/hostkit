//! Language-neutral capability request routing.
//!
//! This module dispatches requests through explicitly granted capabilities while
//! preserving request context, resource ownership, cancellation, and structured
//! errors without depending on a transport or client runtime.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;

use super::cancellation::CancellationToken;
use super::error::{RuntimeError, RuntimeResult};
use super::task::{OwnedTask, RequestContext};

/// A boxed capability operation returned by a route handler.
pub type RouteFuture<Response> =
    Pin<Box<dyn Future<Output = RuntimeResult<Response>> + Send + 'static>>;

/// Handles one request for a granted capability.
pub trait RouteHandler<Request, Response>: Send + Sync + 'static {
    /// Starts the capability operation associated with `request`.
    ///
    /// After cancellation is signalled, the returned future must release its
    /// resources and resolve. Request cancellation waits for that cleanup.
    fn handle(&self, cancellation: CancellationToken, request: Request) -> RouteFuture<Response>;
}

impl<Request, Response, Handler, HandlerFuture> RouteHandler<Request, Response> for Handler
where
    Handler: Fn(CancellationToken, Request) -> HandlerFuture + Send + Sync + 'static,
    HandlerFuture: Future<Output = RuntimeResult<Response>> + Send + 'static,
{
    fn handle(&self, cancellation: CancellationToken, request: Request) -> RouteFuture<Response> {
        Box::pin(self(cancellation, request))
    }
}

/// Routes consumer-defined requests through an explicitly granted capability set.
pub struct Router<Key, Request, Response> {
    handlers: HashMap<Key, Arc<dyn RouteHandler<Request, Response>>>,
}

impl<Key, Request, Response> Router<Key, Request, Response>
where
    Key: Eq + Hash,
    Request: Send + 'static,
    Response: Send + 'static,
{
    /// Creates a router with no granted capabilities.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Grants a capability by associating its key with a handler.
    ///
    /// Returns `false` without replacing the existing handler when the key is
    /// already granted.
    pub fn grant<Handler>(&mut self, key: Key, handler: Handler) -> bool
    where
        Handler: RouteHandler<Request, Response>,
    {
        if self.handlers.contains_key(&key) {
            return false;
        }

        self.handlers.insert(key, Arc::new(handler));
        true
    }

    /// Revokes a capability and returns whether it was previously granted.
    pub fn revoke(&mut self, key: &Key) -> bool {
        self.handlers.remove(key).is_some()
    }

    /// Returns whether a capability is currently granted.
    pub fn is_granted(&self, key: &Key) -> bool {
        self.handlers.contains_key(key)
    }

    /// Returns the number of currently granted capabilities.
    pub fn grant_count(&self) -> usize {
        self.handlers.len()
    }

    /// Dispatches a request as work owned by `context`.
    ///
    /// A denied request does not start a task. Once started, a task retains its
    /// handler even when the corresponding capability is later revoked.
    pub fn dispatch(
        &self,
        context: &RequestContext,
        key: &Key,
        request: Request,
    ) -> RuntimeResult<OwnedTask<Response>> {
        let handler = self
            .handlers
            .get(key)
            .cloned()
            .ok_or_else(RuntimeError::capability_denied)?;
        context.spawn_cooperative(move |cancellation| handler.handle(cancellation, request))
    }
}

impl<Key, Request, Response> Default for Router<Key, Request, Response>
where
    Key: Eq + Hash,
    Request: Send + 'static,
    Response: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::Notify;
    use tokio::time::timeout;

    use super::*;
    use crate::runtime::error::ErrorCode;

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    enum Capability {
        Echo,
        Wait,
    }

    #[tokio::test]
    async fn dispatches_granted_capability_inside_request_context() {
        let mut router = Router::new();
        assert!(
            router.grant(Capability::Echo, |_, request: String| async move {
                Ok(request.to_uppercase())
            })
        );
        let context = RequestContext::new();

        let task = router
            .dispatch(&context, &Capability::Echo, "hello".to_owned())
            .unwrap();

        assert_eq!(context.task_count(), 1);
        assert_eq!(task.await.unwrap(), "HELLO");
        assert_eq!(context.task_count(), 0);
    }

    #[tokio::test]
    async fn denied_capability_does_not_start_or_invoke_work() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let handler_invocations = Arc::clone(&invocations);
        let mut router = Router::<Capability, String, String>::new();
        router.grant(Capability::Echo, move |_, request| {
            handler_invocations.fetch_add(1, Ordering::SeqCst);
            async move { Ok(request) }
        });
        let context = RequestContext::new();

        let error = match router.dispatch(&context, &Capability::Wait, "hello".to_owned()) {
            Ok(_) => panic!("ungranted capability was dispatched"),
            Err(error) => error,
        };

        assert_eq!(error.code(), ErrorCode::CapabilityDenied);
        assert_eq!(error.message(), "capability not granted");
        assert_eq!(invocations.load(Ordering::SeqCst), 0);
        assert_eq!(context.task_count(), 0);
    }

    #[tokio::test]
    async fn cancelled_context_does_not_invoke_granted_handler() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let handler_invocations = Arc::clone(&invocations);
        let mut router = Router::<Capability, (), ()>::new();
        router.grant(Capability::Echo, move |_, ()| {
            handler_invocations.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
        let context = RequestContext::new();
        context.cancel().await;

        let error = match router.dispatch(&context, &Capability::Echo, ()) {
            Ok(_) => panic!("cancelled request context dispatched work"),
            Err(error) => error,
        };

        assert_eq!(error.code(), ErrorCode::Cancelled);
        assert_eq!(invocations.load(Ordering::SeqCst), 0);
        assert_eq!(context.task_count(), 0);
    }

    #[tokio::test]
    async fn request_cancellation_waits_for_handler_cleanup() {
        let started = Arc::new(Notify::new());
        let cleanup_started = Arc::new(Notify::new());
        let finish_cleanup = Arc::new(Notify::new());
        let handler_started = Arc::clone(&started);
        let handler_cleanup_started = Arc::clone(&cleanup_started);
        let handler_finish_cleanup = Arc::clone(&finish_cleanup);
        let mut router = Router::<Capability, (), ()>::new();
        router.grant(
            Capability::Wait,
            move |cancellation: CancellationToken, ()| {
                let started = Arc::clone(&handler_started);
                let cleanup_started = Arc::clone(&handler_cleanup_started);
                let finish_cleanup = Arc::clone(&handler_finish_cleanup);
                async move {
                    started.notify_one();
                    cancellation.cancelled().await;
                    cleanup_started.notify_one();
                    finish_cleanup.notified().await;
                    Ok(())
                }
            },
        );
        let context = Arc::new(RequestContext::new());
        let task = router.dispatch(&context, &Capability::Wait, ()).unwrap();
        timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("handler did not start");
        let cancelling_context = Arc::clone(&context);
        let cancellation = tokio::spawn(async move { cancelling_context.cancel().await });

        timeout(Duration::from_secs(1), cleanup_started.notified())
            .await
            .expect("handler did not begin cancellation cleanup");
        assert!(!cancellation.is_finished());
        assert_eq!(context.task_count(), 1);

        finish_cleanup.notify_one();
        timeout(Duration::from_secs(1), cancellation)
            .await
            .expect("request cancellation did not finish after cleanup")
            .expect("request cancellation task failed");

        assert_eq!(task.await.unwrap_err().code(), ErrorCode::Cancelled);
        assert_eq!(context.task_count(), 0);
    }

    #[tokio::test]
    async fn duplicate_grant_preserves_original_handler() {
        let mut router = Router::new();
        assert!(router.grant(Capability::Echo, |_, _: ()| async { Ok(1) }));
        assert!(!router.grant(Capability::Echo, |_, _: ()| async { Ok(2) }));
        assert_eq!(router.grant_count(), 1);
        let context = RequestContext::new();

        let response = router
            .dispatch(&context, &Capability::Echo, ())
            .unwrap()
            .await
            .unwrap();

        assert_eq!(response, 1);
    }

    #[tokio::test]
    async fn revocation_denies_new_work_without_stopping_in_flight_work() {
        let started = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let handler_started = Arc::clone(&started);
        let handler_finish = Arc::clone(&finish);
        let mut router = Router::new();
        router.grant(Capability::Wait, move |_, _: ()| {
            let started = Arc::clone(&handler_started);
            let finish = Arc::clone(&handler_finish);
            async move {
                started.notify_one();
                finish.notified().await;
                Ok(7)
            }
        });
        let context = RequestContext::new();
        let task = router.dispatch(&context, &Capability::Wait, ()).unwrap();
        started.notified().await;

        assert!(router.revoke(&Capability::Wait));
        assert!(!router.is_granted(&Capability::Wait));
        assert!(!router.revoke(&Capability::Wait));
        let denied = match router.dispatch(&context, &Capability::Wait, ()) {
            Ok(_) => panic!("revoked capability was dispatched"),
            Err(error) => error,
        };
        assert_eq!(denied.code(), ErrorCode::CapabilityDenied);

        finish.notify_one();
        assert_eq!(task.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn handler_panic_is_mapped_by_request_task_boundary() {
        let mut router = Router::<Capability, (), ()>::new();
        router.grant(Capability::Echo, |_, ()| async move {
            panic!("dummy panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        let context = RequestContext::new();

        let error = router
            .dispatch(&context, &Capability::Echo, ())
            .unwrap()
            .await
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::TaskFailed);
        assert_eq!(error.message(), "owned task panicked");
        assert_eq!(context.task_count(), 0);
    }

    #[tokio::test]
    async fn panic_while_handler_starts_is_mapped_by_request_task_boundary() {
        let mut router = Router::<Capability, (), ()>::new();
        router.grant(Capability::Echo, |_, ()| -> std::future::Ready<_> {
            panic!("dummy startup panic")
        });
        let context = RequestContext::new();

        let error = router
            .dispatch(&context, &Capability::Echo, ())
            .unwrap()
            .await
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::TaskFailed);
        assert_eq!(error.message(), "owned task panicked");
        assert_eq!(context.task_count(), 0);
    }
}
