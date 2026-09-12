//! Lua VM lifecycle and capability registration.
//!
//! This module owns `mlua` integration, grants only the capabilities selected
//! for a script, and keeps Rust internals behind a deliberate Lua-facing API.
