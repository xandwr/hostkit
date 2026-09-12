//! The execution boundary between Tokio, native capabilities, and Lua.
//!
//! The runtime coordinates VM ownership, tasks, cancellation, and error mapping
//! so asynchronous Rust operations behave like straightforward Lua calls.

pub mod cancellation;
pub mod error;
pub mod lua;
pub mod task;
