//! Reactor dispatch shim.
//!
//! Picks the per-platform reactor backend at comptime based on
//! `builtin.os.tag`. Re-exports the public surface (`Reactor` type
//! plus the I/O helper free functions `setNonblock`, `readAsync`,
//! `writeAsync`, `readFull`, `writeAll`) so the rest of the
//! codebase (`runtime.zig`, `net.zig`, `lib.zig`) imports a single
//! file regardless of platform.
//!
//! Each backend lives in its own file:
//!   * `reactor_kqueue.zig`   — Darwin / BSD
//!   * `reactor_epoll.zig`    — Linux (readiness; the only public Linux backend)
//!   * `reactor_io_uring.zig` — Linux (completion; internal, reserved for the
//!                              future async-file-I/O path — not user-selectable)
//!   * `reactor_iocp.zig`     — Windows
//!
//! Adding a backend is a self-contained file + an entry in the
//! switch below. The interface (`Reactor.init`/`deinit`/
//! `waitReadable`/`waitWritable`/`waitTimer`/`poll`/`pendingCount`)
//! stays stable; backends differ only in their syscall plumbing.

const builtin = @import("builtin");

const backend = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .netbsd, .openbsd, .dragonfly => @import("reactor_kqueue.zig"),
    .linux => @import("reactor_linux.zig"), // tagged-union dispatch for epoll/io_uring
    .windows => @import("reactor_iocp.zig"),
    else => @compileError("Volt: no reactor backend for this platform — see src/reactor.zig"),
};

pub const Reactor = backend.Reactor;

// ─── Public error vocabulary ─────────────────────────────────────
//
// One categorical surface for every reactor backend. Library
// authors can `catch err switch (err) { ... }` without caring
// which platform produced it. New variants get added here as
// downstream libraries (HTTP, TLS, S3) tell us what they need to
// distinguish.

/// Errors from `setNonblock` / `bind` / `accept` / `connect`-type
/// setup syscalls. Includes the kernel-resource exhaustion cases
/// that downstream libraries actively retry on.
pub const ReactorSetupError = error{
    /// Process or system file-descriptor table exhausted (EMFILE
    /// / ENFILE / WSAEMFILE). Library should back off + retry.
    OutOfDescriptors,
    /// Kernel out of memory (ENOMEM / WSAENOBUFS).
    SystemResources,
    /// Generic fall-through — error code didn't match any
    /// categorical variant. errno/GLE was already printed in the
    /// panic path if this came from there.
    Unexpected,
};

/// Errors from `waitReadable` / `waitWritable` / `waitTimer`.
/// These ride alongside `ReactorSetupError` because the underlying
/// register syscall (kevent / epoll_ctl / io_uring SQE / IOCP
/// submit) can hit the same resource limits.
pub const ReactorWaitError = ReactorSetupError || error{
    /// The fd was closed (or never valid) before the registration
    /// landed — EBADF / WSAENOTSOCK.
    BadDescriptor,
    /// Timer duration exceeds `std.math.maxInt(i64)` nanoseconds
    /// (~292 years). All four backends' kernel timer types are
    /// signed 64-bit; values above that would wrap silently. The
    /// public timer API caps at i64_max ns so callers passing
    /// `std.math.maxInt(u64)` as a sentinel get a clean error
    /// rather than an unpredictable wrap.
    TimeoutOutOfRange,
};

/// Errors from `readAsync` / `writeAsync` / `readFull` / `writeAll`.
/// Categorised per common errno; rare ones collapse into
/// `Unexpected`. Includes everything in `ReactorWaitError` because
/// the I/O loop yields through `wait*` on EAGAIN.
pub const IoError = ReactorWaitError || error{
    /// Peer reset the connection (ECONNRESET / WSAECONNRESET).
    ConnectionReset,
    /// Local write side closed (EPIPE / WSAESHUTDOWN on write).
    BrokenPipe,
    /// Local abort, in-flight ops cancelled
    /// (ECONNABORTED / WSAECONNABORTED).
    ConnectionAborted,
    /// Op on an unconnected socket (ENOTCONN / WSAENOTCONN).
    NotConnected,
    /// Connect target rejected — usable to discriminate retry vs
    /// bail in client code (ECONNREFUSED / WSAECONNREFUSED).
    ConnectionRefused,
    /// Kernel-side IO timeout. Distinct from app-level timeout
    /// (ETIMEDOUT / WSAETIMEDOUT).
    Timeout,
};

// I/O helpers — same shape across platforms; impls live in each
// backend file because errno / EAGAIN / fcntl details differ.
pub const setNonblock = backend.setNonblock;
pub const readAsync = backend.readAsync;
pub const writeAsync = backend.writeAsync;
pub const readFull = backend.readFull;
pub const writeAll = backend.writeAll;
