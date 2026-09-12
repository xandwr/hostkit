//! Language-neutral capability request routing.
//!
//! This module dispatches requests through explicitly granted capabilities while
//! preserving request context, resource ownership, cancellation, and structured
//! errors without depending on a transport or client runtime.
