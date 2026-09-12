//! Durable namespaced state.
//!
//! This capability hides SQLite behind a small transactional key-value model so
//! consumers own their schemas without owning connection, concurrency, or
//! storage correctness.
