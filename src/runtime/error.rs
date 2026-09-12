//! Structured errors crossing the public capability boundary.
//!
//! This module preserves actionable native failure details while presenting a
//! stable error model shared by every capability and runtime operation.

use std::error::Error;
use std::fmt;

/// Stable categories for failures returned through the runtime boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    /// The owning request was cancelled before the operation completed.
    Cancelled,
    /// The requested capability was not granted to this router.
    CapabilityDenied,
    /// A native capability operation failed.
    CapabilityFailed,
    /// A resource limit prevented an operation from completing.
    ResourceLimitExceeded,
    /// An owned task stopped without producing a capability result.
    TaskFailed,
}

/// A structured failure returned by the runtime or a capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeError {
    code: ErrorCode,
    message: String,
}

impl RuntimeError {
    /// Creates a cancellation error.
    pub fn cancelled() -> Self {
        Self {
            code: ErrorCode::Cancelled,
            message: "request cancelled".to_owned(),
        }
    }

    /// Creates an error for a capability that is not granted.
    pub fn capability_denied() -> Self {
        Self {
            code: ErrorCode::CapabilityDenied,
            message: "capability not granted".to_owned(),
        }
    }

    pub(crate) fn capability_failed(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::CapabilityFailed,
            message: message.into(),
        }
    }

    pub(crate) fn resource_limit_exceeded(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::ResourceLimitExceeded,
            message: message.into(),
        }
    }

    /// Returns the stable category of this failure.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the human-readable failure detail.
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn task_failed(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::TaskFailed,
            message: message.into(),
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for RuntimeError {}

/// Result type shared by runtime and capability operations.
pub type RuntimeResult<T> = Result<T, RuntimeError>;
