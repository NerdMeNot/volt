//! POSIX file-op proxies for the Reactor surface — Phase 1.
//!
//! The cross-platform Reactor exposes six fs methods (`fsRead`,
//! `fsWrite`, `fsFsync`, `fsFdatasync`, `fsOpen`, `fsClose`). On
//! every POSIX backend today (kqueue, epoll, io_uring), each one
//! routes through `spawnBlocking` — i.e. the syscall runs on a
//! blocking-pool thread and the calling coroutine parks until the
//! pool delivers the result. This is the spawnBlocking-proxy
//! implementation; behaviour is identical to what `fs/file.zig`
//! has done directly since Wave A.
//!
//! Phase 2 replaces the Linux io_uring backend's fs methods with
//! real async file I/O via per-worker io_uring rings. The other
//! POSIX backends (kqueue, epoll) stay on this proxy because
//! Darwin/BSD lack an equivalent and Linux's epoll variant is the
//! fallback when io_uring init fails.
//!
//! `FsResult { value, err }` mirrors what an io_uring CQE delivers
//! (`res` field — positive = bytes/fd, negative = -errno), so the
//! Phase 1 → Phase 2 swap doesn't change the boundary signature.

const std = @import("std");
const builtin = @import("builtin");
const lib = @import("../lib.zig");

comptime {
    if (builtin.os.tag == .windows) {
        @compileError("reactor_fs.zig is POSIX-only; Windows fs lands in a separate phase");
    }
}

// ─── Result vocabulary ───────────────────────────────────────────

/// Bundled syscall outcome. `value >= 0` is the syscall's return
/// (bytes for read/write, 0 for fsync etc.); `value < 0` means
/// failure and `err` carries the errno.
pub const FsResult = struct {
    value: isize,
    err: c_int,
};

/// `open`-shaped outcome. Same shape as `FsResult` but with `fd`
/// instead of `value` — keeps the call site self-documenting.
pub const FdResult = struct {
    fd: c_int,
    err: c_int,
};

// ─── libc externs ────────────────────────────────────────────────
//
// `fs/file.zig` carries its own copies of these for the
// non-coroutine `readRaw`/`writeRaw`/etc paths (called from the
// main thread before `run()`). The two copies are libc signatures
// with no drift risk.

const c_read = @extern(
    *const fn (c_int, [*]u8, usize) callconv(.c) isize,
    .{ .name = "read" },
);
const c_write = @extern(
    *const fn (c_int, [*]const u8, usize) callconv(.c) isize,
    .{ .name = "write" },
);
const c_pread = @extern(
    *const fn (c_int, [*]u8, usize, i64) callconv(.c) isize,
    .{ .name = "pread" },
);
const c_pwrite = @extern(
    *const fn (c_int, [*]const u8, usize, i64) callconv(.c) isize,
    .{ .name = "pwrite" },
);
const c_fsync = @extern(
    *const fn (c_int) callconv(.c) c_int,
    .{ .name = "fsync" },
);
const c_fdatasync = if (builtin.os.tag == .linux)
    @extern(*const fn (c_int) callconv(.c) c_int, .{ .name = "fdatasync" })
else
    @extern(*const fn (c_int) callconv(.c) c_int, .{ .name = "fsync" });
// `open(2)` is variadic in libc — declared with `...` so the
// `mode` arg actually rides on the stack per SysV varargs on
// Darwin x86_64. Plain `@extern` with three regular params
// silently drops the third arg on that ABI (file gets created
// mode 0o000). See `fs/syscall.zig:192-207`.
extern "c" fn open(path: [*:0]const u8, oflag: c_int, ...) c_int;
fn c_open(path: [*:0]const u8, flags: c_int, mode: c_uint) c_int {
    return open(path, flags, mode);
}
const c_close = @extern(
    *const fn (c_int) callconv(.c) c_int,
    .{ .name = "close" },
);

// errno accessor matches reactor_posix.zig.
const c_error = if (builtin.os.tag.isDarwin() or
    builtin.os.tag == .freebsd or
    builtin.os.tag == .openbsd or
    builtin.os.tag == .netbsd or
    builtin.os.tag == .dragonfly)
    @extern(*const fn () callconv(.c) *c_int, .{ .name = "__error" })
else
    @extern(*const fn () callconv(.c) *c_int, .{ .name = "__errno_location" });

inline fn errnoVal() c_int {
    return c_error().*;
}

// ENOMEM literal — used when `spawnBlocking` itself fails (pool
// shutdown / kernel OOM on thread spawn). Caller's errno → error
// mapping treats it as `SystemResources`.
const ENOMEM: c_int = 12;

// `offset = ~@as(u64, 0)` sentinel means "use current file
// position" — io_uring's convention, mirrored at our public
// surface so the Phase 2 swap is a direct passthrough.
pub const OFFSET_CUR: u64 = ~@as(u64, 0);

// ─── Sync closures ───────────────────────────────────────────────
//
// Each runs on a blocking-pool thread. Captures errno into the
// returned struct because the caller (a coroutine on a different
// OS thread) won't see the thread-local errno value.

fn syncRead(fd: c_int, buf_ptr: [*]u8, buf_len: usize, offset: u64) FsResult {
    const n = if (offset == OFFSET_CUR)
        c_read(fd, buf_ptr, buf_len)
    else
        c_pread(fd, buf_ptr, buf_len, @intCast(offset));
    return .{ .value = n, .err = if (n < 0) errnoVal() else 0 };
}

fn syncWrite(fd: c_int, buf_ptr: [*]const u8, buf_len: usize, offset: u64) FsResult {
    const n = if (offset == OFFSET_CUR)
        c_write(fd, buf_ptr, buf_len)
    else
        c_pwrite(fd, buf_ptr, buf_len, @intCast(offset));
    return .{ .value = n, .err = if (n < 0) errnoVal() else 0 };
}

fn syncFsync(fd: c_int) FsResult {
    const rc = c_fsync(fd);
    return .{ .value = rc, .err = if (rc != 0) errnoVal() else 0 };
}

fn syncFdatasync(fd: c_int) FsResult {
    const rc = c_fdatasync(fd);
    return .{ .value = rc, .err = if (rc != 0) errnoVal() else 0 };
}

fn syncOpen(path_ptr: [*:0]const u8, flags: c_int, mode: c_uint) FdResult {
    const fd = c_open(path_ptr, flags, mode);
    return .{ .fd = fd, .err = if (fd < 0) errnoVal() else 0 };
}

fn syncClose(fd: c_int) FsResult {
    const rc = c_close(fd);
    return .{ .value = rc, .err = if (rc != 0) errnoVal() else 0 };
}

// ─── Public surface — what each backend's fs* method calls ───────

/// Read up to `buf.len` bytes. `offset == OFFSET_CUR` uses the
/// current file position (read(2)); any other value does
/// pread(2). Parks the calling coroutine on a blocking-pool
/// thread; returns when the pool delivers the result.
pub fn fsRead(fd: i32, buf: []u8, offset: u64) FsResult {
    return lib.spawnBlocking(syncRead, .{
        @as(c_int, @intCast(fd)),
        buf.ptr,
        buf.len,
        offset,
    }) catch return .{ .value = -1, .err = ENOMEM };
}

/// Write up to `buf.len` bytes. `offset` semantics mirror `fsRead`.
pub fn fsWrite(fd: i32, buf: []const u8, offset: u64) FsResult {
    return lib.spawnBlocking(syncWrite, .{
        @as(c_int, @intCast(fd)),
        buf.ptr,
        buf.len,
        offset,
    }) catch return .{ .value = -1, .err = ENOMEM };
}

/// Flush data + metadata (`fsync(2)`).
pub fn fsFsync(fd: i32) FsResult {
    return lib.spawnBlocking(syncFsync, .{@as(c_int, @intCast(fd))}) catch
        return .{ .value = -1, .err = ENOMEM };
}

/// Flush data only (`fdatasync(2)` on Linux; falls back to
/// `fsync(2)` on Darwin/BSD which don't expose fdatasync).
pub fn fsFdatasync(fd: i32) FsResult {
    return lib.spawnBlocking(syncFdatasync, .{@as(c_int, @intCast(fd))}) catch
        return .{ .value = -1, .err = ENOMEM };
}

/// Open a file (`open(2)`). NUL-terminated path; caller passes
/// `O_CLOEXEC | O_RDONLY | ...` flags directly.
pub fn fsOpen(path_ptr: [*:0]const u8, flags: c_int, mode: c_uint) FdResult {
    return lib.spawnBlocking(syncOpen, .{ path_ptr, flags, mode }) catch
        return .{ .fd = -1, .err = ENOMEM };
}

/// Close an fd. Errors are reported but the fd is no longer
/// valid regardless — POSIX-defined behaviour.
pub fn fsClose(fd: i32) FsResult {
    return lib.spawnBlocking(syncClose, .{@as(c_int, @intCast(fd))}) catch
        return .{ .value = -1, .err = ENOMEM };
}
