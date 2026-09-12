//! Native capabilities exposed through the shared runtime boundary.
//!
//! Each module owns one small, testable system boundary. The runtime grants
//! scoped access and presents results through one consistent error, cancellation,
//! and resource ownership model.

pub mod ast;
pub mod fs;
pub mod kv;
pub mod net;
pub mod secrets;
pub mod sys;
