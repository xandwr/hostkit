//! Durable namespaced state for Lua.
//!
//! This capability hides SQLite behind a small transactional key-value model so
//! scripts own their schemas without owning connection, concurrency, or storage
//! correctness.
