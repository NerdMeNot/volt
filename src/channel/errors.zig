//! Unified error vocabulary for the channel family.
//!
//! Every channel type (`Channel`, `Oneshot`, `Watch`, `Broadcast`)
//! re-exports these so consumers can write a single `error.Closed`
//! handler that works across the family. Avoids drift like
//! `AlreadySent` / `AlreadyClosed` / `RecvCancelError` that crept in
//! during early development.
//!
//! ## The set
//!
//! - `error.Closed` — the channel was closed before the operation
//!   completed. For send paths this includes "the channel could no
//!   longer accept this value" (e.g., a `Oneshot` that has already
//!   delivered its single message is "closed" from the second
//!   sender's perspective).
//! - `error.Cancelled` — the calling coroutine was cancelled while
//!   parked. Surfaces uniformly across every blocking primitive in
//!   Volt (channels, sync, timers, I/O).

pub const SendError = error{Closed};
pub const RecvError = error{ Closed, Cancelled };
