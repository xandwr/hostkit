//! Buffered and streaming network access for Lua.
//!
//! This capability owns Tokio-driven transport, pooling, TLS, timeouts, and
//! cancellation. Lua remains responsible for constructing provider requests and
//! interpreting response protocols.
