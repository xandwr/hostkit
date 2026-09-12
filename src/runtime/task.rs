//! Scheduling between Lua coroutines and Tokio tasks.
//!
//! This module suspends Lua while native futures are pending, resumes it with a
//! value or structured error, and preserves cancellation and task ownership
//! throughout the round trip.
