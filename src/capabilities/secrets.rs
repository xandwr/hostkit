//! Controlled secret access for Lua.
//!
//! This capability supplies credentials without treating them as ordinary
//! configuration data, allowing other native capabilities to consume secrets
//! while logs, errors, and serialization keep them redacted.
