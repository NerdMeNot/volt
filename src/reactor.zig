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
//!   * `reactor_kqueue.zig`   — Darwin / BSD (shipping)
//!   * `reactor_epoll.zig`    — Linux (stub today; impl in L2a)
//!   * `reactor_io_uring.zig` — Linux (stub today; impl in L2b,
//!                                     selected runtime-side via
//!                                     `Runtime.Config.io_backend`)
//!   * `reactor_iocp.zig`     — Windows (stub today; impl in L3)
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

/// Linux-only — `Runtime.Config.io_backend` selects between
/// `.epoll` and `.io_uring`; on other platforms the Config field
/// is accepted but ignored. Public for `runtime.zig`'s use.
pub const IoBackend = enum { auto, epoll, io_uring };

// I/O helpers — same shape across platforms; impls live in each
// backend file because errno / EAGAIN / fcntl details differ.
pub const setNonblock = backend.setNonblock;
pub const readAsync = backend.readAsync;
pub const writeAsync = backend.writeAsync;
pub const readFull = backend.readFull;
pub const writeAll = backend.writeAll;
