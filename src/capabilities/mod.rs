//! Native capabilities made available to Lua.
//!
//! Each module owns one small, testable system boundary. The runtime grants
//! scoped access and presents their results through one consistent Lua-facing
//! error, cancellation, and scheduling model.

pub mod ast;
pub mod fs;
pub mod kv;
pub mod net;
pub mod secrets;
pub mod sys;
