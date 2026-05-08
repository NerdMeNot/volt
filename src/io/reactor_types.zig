//! Backend-agnostic reactor types: `EventKind` and `ReactorError`.
//!
//! Lives in its own file so each backend (`reactor_kqueue.zig`,
//! `reactor_epoll.zig`, `reactor_iouring.zig`, `reactor_iocp.zig`) can
//! import these types without forming an import cycle through the
//! dispatcher. `reactor.zig` re-exports both names so consumers can
//! continue to write `@import("reactor.zig").EventKind`.
//!
//! Adding a new backend? Use these types verbatim. Returning a
//! backend-specific error from a public method is the kind of drift
//! the `reactor.zig` conformance check exists to catch.

const std = @import("std");

/// Readiness kind a coroutine waits on. Same on every backend.
pub const EventKind = enum(u8) {
    readable,
    writable,
};

/// Closed error set every reactor backend translates its kernel-side
/// errors into at the public-method boundary. Consumers (`io/wait.zig`,
/// `time/Sleep.zig`, etc.) widen these into `IoError` sub-sets without
/// hiding context behind `else =>` arms.
///
/// Backend-specific names (`error.EpollCtlFailed`, `error.EventNotFound`,
/// io_uring SQE errors, IOCP error codes) **must not** leak through the
/// public surface — translate at the boundary.
pub const ReactorError = error{
    /// Backing kernel object couldn't be created (`epoll_create1`,
    /// `kqueue`, `io_uring_setup`, `CreateIoCompletionPort`, eventfd /
    /// timerfd / AFD handle creation).
    InitFailed,
    /// Registering a wait or timer with the kernel failed
    /// (kqueue/epoll/io_uring submit failures, IOCP associate failures,
    /// timerfd_create / timerfd_settime errors). The backend has
    /// already cleaned up any partial state before returning this.
    RegistrationFailed,
    /// Polling the kernel queue failed in a non-recoverable way (the
    /// reactor handle is dead, ring is in an unrecoverable state).
    /// Workers treat this as a runtime-fatal condition.
    PollFailed,
    /// Backend doesn't implement the requested operation on this OS
    /// configuration (e.g. timer registration on a not-yet-ported IOCP
    /// path). Distinct from `RegistrationFailed` so callers can fall
    /// back to a different mechanism instead of bubbling a hard error.
    NotImplemented,
    /// Reactor-internal allocator failure (waiter hashmap insert,
    /// timer-entry slab grow).
    OutOfMemory,
};
