//! Tau's native host library.
//!
//! Rust owns dependable system mechanisms while Lua owns agent policy. This
//! crate keeps that boundary narrow by routing capability modules through the
//! runtime that makes asynchronous native work feel natural to Lua code.

pub mod capabilities;
pub mod runtime;
