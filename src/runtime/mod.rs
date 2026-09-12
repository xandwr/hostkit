//! The execution boundary between clients and native capabilities.
//!
//! The runtime coordinates request contexts, routing, tasks, cancellation,
//! resource ownership, and error mapping without adopting client-specific
//! semantics.

pub mod cancellation;
pub mod error;
pub mod router;
pub mod task;
