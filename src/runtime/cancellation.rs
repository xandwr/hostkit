//! Shared cancellation propagation.
//!
//! This module coordinates one cancellation signal across native tasks and every
//! native operation they start, allowing interruption to stop network, process,
//! storage, and analysis work as a single action.

use tokio_util::sync::CancellationToken as TokioCancellationToken;

/// A clonable, read-only view of a request's cancellation state.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    inner: TokioCancellationToken,
}

impl CancellationToken {
    pub(crate) fn new() -> Self {
        Self {
            inner: TokioCancellationToken::new(),
        }
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Resolves when cancellation is requested.
    pub async fn cancelled(&self) {
        self.inner.cancelled().await;
    }

    pub(crate) fn cancel(&self) {
        self.inner.cancel();
    }
}
