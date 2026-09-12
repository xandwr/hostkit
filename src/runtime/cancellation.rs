//! Shared cancellation propagation.
//!
//! This module coordinates one cancellation signal across native tasks and every
//! native operation they start, allowing interruption to stop network, process,
//! storage, and analysis work as a single action.
