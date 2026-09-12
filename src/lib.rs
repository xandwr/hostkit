//! Pure Rust foundation for Hostkit's native capability service.
//!
//! The crate owns dependable system mechanisms and routes them through a
//! language-neutral runtime boundary. Client runtimes and product policy remain
//! separate consumers of the same native contract.

pub mod capabilities;
pub mod runtime;
