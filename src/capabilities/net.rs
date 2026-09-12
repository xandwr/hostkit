//! Buffered and streaming network access.
//!
//! This capability owns Tokio-driven transport, pooling, TLS, timeouts, and
//! cancellation while leaving protocol-specific payloads and response
//! interpretation to consumers.
