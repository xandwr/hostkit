//! Native asynchronous task ownership and scheduling.
//!
//! This module tracks work started by a request context and preserves cancellation,
//! cleanup, and structured results throughout each operation's lifetime.
