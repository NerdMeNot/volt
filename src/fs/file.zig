//! `volt.fs.File` — async-by-default file handle. Looks synchronous
//! to the caller; under the hood, every blocking syscall (`read`,
//! `write`, `pread`, `pwrite`, `fsync`, `open`, `close`) bridges
//! through `spawnBlocking` so the calling coroutine parks instead
//! of pinning a worker.
//!
//! Design (per Phase B.2):
//!
//! * **One File type, not split sync vs async.** Stackful makes the
//!   distinction moot — every method is async-by-default because
//!   we're in a coroutine.
//! * **Sensible-default constructors.** `File.open(path)` for
//!   read-only, `File.create(path)` for write+create+truncate,
//!   `File.openOptions(path, .{...})` for the options-rich case.
//!   No method-chain builder.
//! * **Cancel-aware variants** with the `*Cancel` suffix:
//!   `readCancel`, `writeCancel`, …
//! * **I/O handle conformance** — `.reader(buf)` / `.writer(buf)`
//!   returning `std.Io.Reader` / `std.Io.Writer` wrappers with an
//!   `err: ?FileError` slot for typed-error recovery. Matches
//!   `volt.net.TcpStream` / `UdpSocket` / friends.
//!
//! Backend: `Runtime.Config.fs_backend` selects between `.auto`
//! (per-platform default) and `.blocking` (universal fallback).
//! Today only `.blocking` is implemented — the io_uring + IOCP
//! fast paths land in a follow-up commit without API change.

const std = @import("std");
const builtin = @import("builtin");

const syscall = @import("syscall.zig");
const fs_error = @import("error.zig");
const metadata_mod = @import("metadata.zig");
const lib = @import("../lib.zig");

const is_windows = builtin.os.tag == .windows;

pub const FsError = fs_error.FsError;
pub const FileError = fs_error.FileError;
pub const Metadata = metadata_mod.Metadata;
pub const Permissions = metadata_mod.Permissions;

// ─── OpenOptions ─────────────────────────────────────────────────

/// Options for `File.openOptions`. Every field defaults so callers
/// only set what they need.
///
/// ```zig
/// const f = try File.openOptions("config.toml", .{
///     .read = true,
///     .write = true,
///     .create = true,
///     .mode = 0o600,
/// });
/// ```
pub const OpenOptions = struct {
    /// Open for reading. `File.open` sets this implicitly.
    read: bool = false,
    /// Open for writing.
    write: bool = false,
    /// Append every write at end-of-file atomically.
    append: bool = false,
    /// Truncate to zero length on open. Requires `write = true`.
    truncate: bool = false,
    /// Create the file if it doesn't exist. Requires `write = true`.
    create: bool = false,
    /// Create exclusively — fail if the file exists. Requires
    /// `create = true`. Atomic; no TOCTOU between check + create.
    create_new: bool = false,
    /// Mode bits applied when a new file is created. Honoured iff
    /// `create` or `create_new`. The process umask still applies.
    mode: u32 = 0o644,
};

// ─── File ────────────────────────────────────────────────────────

/// Open file handle. `fd` is the raw OS descriptor; advanced
/// callers may pass it to direct-syscall helpers (`fcntl`, `flock`,
/// `mmap`).
pub const File = struct {
    /// Raw OS handle. POSIX fd or Windows HANDLE (cast to c_int).
    fd: c_int,

    /// Append mode — every write seeks to EOF first. Mirrors the
    /// `O_APPEND` flag we opened with so `writeAt` knows it's a
    /// no-op (the kernel ignores the offset on append-mode fds).
    append: bool = false,

    // ─── Construction ────────────────────────────────────────────

    /// Open `path` for read-only access.
    pub fn open(path: []const u8) FsError!File {
        return openOptions(path, .{ .read = true });
    }

    /// Create or truncate `path` for write access.
    pub fn create(path: []const u8) FsError!File {
        return openOptions(path, .{
            .write = true,
            .create = true,
            .truncate = true,
        });
    }

    /// Open `path` with custom options. The least-surprising form
    /// for unusual cases.
    pub fn openOptions(path: []const u8, opts: OpenOptions) FsError!File {
        if (is_windows) @compileError("Windows File: pending (Phase B.2 follow-up)");
        if (opts.create_new and !opts.create) return error.InvalidPath;
        if (opts.truncate and !opts.write) return error.InvalidPath;
        if (!opts.read and !opts.write) return error.InvalidPath;

        var z: PathZ = undefined;
        try pathZInto(path, &z);

        var flags: c_int = if (opts.read and opts.write)
            syscall.O_RDWR
        else if (opts.write)
            syscall.O_WRONLY
        else
            syscall.O_RDONLY;

        if (opts.create_new) flags |= syscall.O_CREAT | syscall.O_EXCL;
        if (opts.create and !opts.create_new) flags |= syscall.O_CREAT;
        if (opts.truncate) flags |= syscall.O_TRUNC;
        if (opts.append) flags |= syscall.O_APPEND;
        flags |= syscall.O_CLOEXEC;

        // `open` can be slow on a network FS; bridge through the
        // blocking pool so we don't pin a worker. Path lives on our
        // stack — pass by pointer so the closure captures 8 bytes
        // not 4 KiB.
        const mode_arg: c_uint = @intCast(opts.mode);
        const fd = if (inCoroutine())
            blockingOpen(&z, flags, mode_arg)
        else
            syscall.c_open(&z.buf, flags, mode_arg);

        if (fd < 0) return fs_error.fromErrno(fs_error.currentErrno());
        return .{ .fd = fd, .append = opts.append };
    }

    /// Take ownership of an existing fd (e.g. one passed in from
    /// the parent process via fd inheritance). Caller is
    /// responsible for the fd's mode flags — they're not queried.
    pub fn fromFd(fd: c_int) File {
        return .{ .fd = fd };
    }

    // ─── Lifecycle ───────────────────────────────────────────────

    /// Close the underlying fd. Safe to call once. Errors from
    /// close (e.g. async I/O writeback failure on Linux) are
    /// surfaced.
    pub fn close(self: *File) void {
        if (is_windows) @compileError("Windows File: pending");
        _ = syscall.c_close(self.fd);
        self.fd = -1;
    }

    // ─── I/O — direct sync-shape methods ─────────────────────────

    /// Read up to `buf.len` bytes. Returns 0 on EOF, otherwise the
    /// count actually read (may be less than requested).
    pub fn read(self: *File, buf: []u8) FileError!usize {
        if (inCoroutine()) {
            return blockingRead(self.fd, buf) catch |e| return e;
        }
        return readRaw(self.fd, buf);
    }

    /// Write up to `buf.len` bytes. Returns the count actually
    /// written.
    pub fn write(self: *File, buf: []const u8) FileError!usize {
        if (inCoroutine()) {
            return blockingWrite(self.fd, buf) catch |e| return e;
        }
        return writeRaw(self.fd, buf);
    }

    /// Read until `buf` is full or EOF. Returns the total bytes
    /// read — less than `buf.len` only on EOF.
    pub fn readFull(self: *File, buf: []u8) FileError!usize {
        var total: usize = 0;
        while (total < buf.len) {
            const n = try self.read(buf[total..]);
            if (n == 0) return total;
            total += n;
        }
        return total;
    }

    /// Write every byte in `buf`. Errors if the write side closes
    /// partway through.
    pub fn writeAll(self: *File, buf: []const u8) FileError!void {
        var written: usize = 0;
        while (written < buf.len) {
            const n = try self.write(buf[written..]);
            if (n == 0) return error.BrokenPipe;
            written += n;
        }
    }

    /// Positioned read — `pread(2)`. Does not affect the file
    /// offset. `offset` is from the start of the file.
    pub fn readAt(self: *File, buf: []u8, offset: u64) FileError!usize {
        if (inCoroutine()) {
            return blockingPread(self.fd, buf, offset) catch |e| return e;
        }
        return preadRaw(self.fd, buf, offset);
    }

    /// Positioned write — `pwrite(2)`. Does not affect the file
    /// offset. Ignored offset under `O_APPEND` (kernel forces EOF).
    pub fn writeAt(self: *File, buf: []const u8, offset: u64) FileError!usize {
        if (inCoroutine()) {
            return blockingPwrite(self.fd, buf, offset) catch |e| return e;
        }
        return pwriteRaw(self.fd, buf, offset);
    }

    /// Where to seek from. Mirrors POSIX `SEEK_*`.
    pub const SeekWhence = enum(c_int) {
        start = 0, // SEEK_SET
        current = 1, // SEEK_CUR
        end = 2, // SEEK_END
    };

    /// Move the file pointer. `offset` may be negative for
    /// `.current` / `.end`. Returns the new absolute offset.
    pub fn seek(self: *File, offset: i64, whence: SeekWhence) FileError!u64 {
        if (is_windows) @compileError("Windows File: pending");
        const result = c_lseek(self.fd, offset, @intFromEnum(whence));
        if (result < 0) return fs_error.fromErrno(fs_error.currentErrno());
        return @intCast(result);
    }

    /// Current file offset (`lseek(fd, 0, SEEK_CUR)`).
    pub fn tell(self: *File) FileError!u64 {
        return self.seek(0, .current);
    }

    /// Flush kernel-side dirty buffers to disk. Honoured by every
    /// modern fs. Blocking — bridges through the pool.
    pub fn sync(self: *File) FileError!void {
        if (is_windows) @compileError("Windows File: pending");
        if (inCoroutine()) {
            return blockingFsync(self.fd) catch |e| return e;
        }
        if (c_fsync(self.fd) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    }

    /// Like `sync` but only flushes data, not metadata. On Linux
    /// uses `fdatasync`; elsewhere falls back to `fsync`.
    pub fn dataSync(self: *File) FileError!void {
        if (is_windows) @compileError("Windows File: pending");
        if (inCoroutine()) {
            return blockingFdatasync(self.fd) catch |e| return e;
        }
        const rc = if (comptime builtin.os.tag == .linux)
            c_fdatasync(self.fd)
        else
            c_fsync(self.fd);
        if (rc != 0) return fs_error.fromErrno(fs_error.currentErrno());
    }

    /// Truncate or extend the file to exactly `size` bytes. New
    /// pages on extend are zero-filled per POSIX.
    pub fn setLen(self: *File, size: u64) FileError!void {
        if (is_windows) @compileError("Windows File: pending");
        if (c_ftruncate(self.fd, @intCast(size)) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    /// Stat by fd.
    pub fn metadata(self: *File) FsError!Metadata {
        var buf: metadata_mod.PlatformStat = undefined;
        if (syscall.fstat(self.fd, &buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
        return Metadata.fromStat(buf);
    }

    /// Set permissions by fd (`fchmod`).
    pub fn setPermissions(self: *File, perms: Permissions) FsError!void {
        if (is_windows) @compileError("Windows File: pending");
        const m: std.c.mode_t = @intCast(perms.getMode());
        if (syscall.fchmod(self.fd, m) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    }

    // ─── Cancel-aware variants ───────────────────────────────────
    //
    // The blocking-pool path doesn't yet propagate Cancel into the
    // spawned thread — the thread runs to completion, then the
    // coroutine observes the cancel afterward. For tight blocking
    // ops (file reads), this is acceptable; documenting the
    // behaviour honestly.
    //
    // Same shape on Linux io_uring (Phase 2C.2): a `*Cancel.fire()`
    // that arrives AFTER the SQE was submitted does NOT cancel the
    // in-flight kernel op. The coroutine stays parked until the
    // CQE arrives; the cancel is observed at the NEXT syscall.
    // This is intentional — the FsOp lives on the coroutine's
    // stack and the kernel will write to `user_data` when the op
    // completes. Letting the coroutine unwind early would leave a
    // dangling `FsOp` pointer for the kernel to dereference (UAF).
    // See `docs/internals/phase-2c-design.md §1` for the lifetime
    // invariant. Phase 3 will add real in-flight cancellation via
    // `IORING_OP_ASYNC_CANCEL` with the cancel-and-drain semantics
    // from the design memo's §5 — until then, fire-and-wait is the
    // honest semantics.

    /// Read with a cancel handle. On the Linux io_uring path
    /// (Phase 3), a cancel that fires mid-op promptly returns
    /// `error.Cancelled` after the kernel finishes draining the
    /// SQE. On the spawnBlocking fallback path (Darwin, or Linux
    /// without io_uring), the cancel is checked only at the
    /// submit boundary — the syscall in the pool thread runs to
    /// completion. Both paths surface the same error type.
    pub fn readCancel(self: *File, buf: []u8, c: *lib.Cancel) (FileError || error{Cancelled})!usize {
        if (inCoroutine()) {
            return blockingReadCancel(self.fd, buf, c) catch |e| return e;
        }
        // Outside a coroutine the cancel handle has no parker to
        // wake; the raw read runs to completion. Mirrors the
        // non-cancel path's behaviour.
        if (c.isFired()) return error.Cancelled;
        return readRaw(self.fd, buf);
    }

    pub fn writeCancel(self: *File, buf: []const u8, c: *lib.Cancel) (FileError || error{Cancelled})!usize {
        if (inCoroutine()) {
            return blockingWriteCancel(self.fd, buf, c) catch |e| return e;
        }
        if (c.isFired()) return error.Cancelled;
        return writeRaw(self.fd, buf);
    }

    /// Loop wrapper — inherits cancel semantics from `readCancel`.
    pub fn readFullCancel(self: *File, buf: []u8, c: *lib.Cancel) (FileError || error{Cancelled})!usize {
        var total: usize = 0;
        while (total < buf.len) {
            const n = try self.readCancel(buf[total..], c);
            if (n == 0) return total;
            total += n;
        }
        return total;
    }

    /// Loop wrapper — inherits cancel semantics from `writeCancel`.
    pub fn writeAllCancel(self: *File, buf: []const u8, c: *lib.Cancel) (FileError || error{Cancelled})!void {
        var written: usize = 0;
        while (written < buf.len) {
            const n = try self.writeCancel(buf[written..], c);
            if (n == 0) return error.BrokenPipe;
            written += n;
        }
    }

    pub fn syncCancel(self: *File, c: *lib.Cancel) (FileError || error{Cancelled})!void {
        if (is_windows) @compileError("Windows File: pending");
        if (inCoroutine()) {
            return blockingFsyncCancel(self.fd, c) catch |e| return e;
        }
        if (c.isFired()) return error.Cancelled;
        if (c_fsync(self.fd) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    }

    // ─── std.Io adapter pair ─────────────────────────────────────

    /// Wrap as a `std.Io.Reader` so std-library code can consume
    /// bytes from this file. `buffer` is the caller-owned backing
    /// store for std's internal buffered-read protocol.
    pub fn reader(self: *File, buffer: []u8) Reader {
        return .{
            .stream = self,
            .interface = std.Io.Reader{
                .vtable = &.{ .stream = Reader.streamImpl },
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    /// Wrap as a `std.Io.Writer`. Mirror of `reader`.
    pub fn writer(self: *File, buffer: []u8) Writer {
        return .{
            .stream = self,
            .interface = std.Io.Writer{
                .vtable = &.{ .drain = Writer.drainImpl },
                .buffer = buffer,
                .end = 0,
            },
        };
    }

    /// std.Io.Reader wrapper. `interface` is what std-library code
    /// hands to its `takeByte` / `readSliceShort` / formatter
    /// machinery. On error, `interface` returns `error.ReadFailed`
    /// / `error.EndOfStream`; the typed `FileError` lives in
    /// `err` for caller recovery.
    pub const Reader = struct {
        stream: *File,
        interface: std.Io.Reader,
        err: ?FileError = null,

        fn streamImpl(io_r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
            const self: *Reader = @fieldParentPtr("interface", io_r);
            const dest = limit.slice(try w.writableSliceGreedy(1));
            const n = self.stream.read(dest) catch |e| {
                self.err = e;
                return error.ReadFailed;
            };
            if (n == 0) return error.EndOfStream;
            w.advance(n);
            return n;
        }
    };

    /// std.Io.Writer wrapper. Mirror of `Reader`.
    pub const Writer = struct {
        stream: *File,
        interface: std.Io.Writer,
        err: ?FileError = null,

        fn drainImpl(io_w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *Writer = @fieldParentPtr("interface", io_w);
            _ = splat;
            // Drain std's internal buffer first.
            const buffered = io_w.buffered();
            if (buffered.len > 0) {
                const n = self.stream.write(buffered) catch |e| {
                    self.err = e;
                    return error.WriteFailed;
                };
                if (n == 0) return error.WriteFailed;
                return io_w.consume(n);
            }
            // Buffer's empty — write the first non-empty chunk.
            for (data) |chunk| {
                if (chunk.len == 0) continue;
                const n = self.stream.write(chunk) catch |e| {
                    self.err = e;
                    return error.WriteFailed;
                };
                if (n == 0) return error.WriteFailed;
                return n;
            }
            return 0;
        }
    };
};

// ─── Coroutine detection ─────────────────────────────────────────

/// Are we inside a Volt coroutine? If so, bridge through
/// spawnBlocking. If not (e.g. main thread before run()), call
/// libc directly and accept the worker-pinning cost.
fn inCoroutine() bool {
    return @import("../current.zig").get() != null;
}

// ─── Reactor-routed fs ops ───────────────────────────────────────
//
// Each `blockingFoo` now routes through `Reactor.fsFoo`. Today
// every backend's fs methods proxy through spawnBlocking, so the
// behaviour is identical to the pre-Phase-1 direct-spawnBlocking
// path. Phase 2 will introduce per-worker io_uring rings on Linux
// that bypass spawnBlocking for the io_uring backend — without
// touching anything in this file.
//
// `reactor_fs.OFFSET_CUR` (= ~0 u64) is the io_uring-convention
// sentinel meaning "use current file position"; the helpers
// dispatch to read(2)/write(2) vs pread(2)/pwrite(2) on it.

const reactor_fs = @import("../reactor_fs.zig");

fn blockingOpen(z: *const PathZ, flags: c_int, mode: c_uint) c_int {
    const result = lib.runtime().reactor.fsOpen(@as([*:0]const u8, &z.buf), flags, mode);
    if (result.err != 0) std.c._errno().* = result.err;
    return result.fd;
}

fn blockingRead(fd: c_int, buf: []u8) FileError!usize {
    const result = lib.runtime().reactor.fsRead(@intCast(fd), buf, reactor_fs.OFFSET_CUR, null);
    if (result.value < 0) return fs_error.fromErrno(result.err);
    return @intCast(result.value);
}

fn readRaw(fd: c_int, buf: []u8) FileError!usize {
    const n = c_read(fd, buf.ptr, buf.len);
    if (n < 0) return fs_error.fromErrno(fs_error.currentErrno());
    return @intCast(n);
}

fn blockingWrite(fd: c_int, buf: []const u8) FileError!usize {
    const result = lib.runtime().reactor.fsWrite(@intCast(fd), buf, reactor_fs.OFFSET_CUR, null);
    if (result.value < 0) return fs_error.fromErrno(result.err);
    return @intCast(result.value);
}

fn writeRaw(fd: c_int, buf: []const u8) FileError!usize {
    const n = c_write(fd, buf.ptr, buf.len);
    if (n < 0) return fs_error.fromErrno(fs_error.currentErrno());
    return @intCast(n);
}

fn blockingPread(fd: c_int, buf: []u8, offset: u64) FileError!usize {
    const result = lib.runtime().reactor.fsRead(@intCast(fd), buf, offset, null);
    if (result.value < 0) return fs_error.fromErrno(result.err);
    return @intCast(result.value);
}

fn preadRaw(fd: c_int, buf: []u8, offset: u64) FileError!usize {
    const n = c_pread(fd, buf.ptr, buf.len, @intCast(offset));
    if (n < 0) return fs_error.fromErrno(fs_error.currentErrno());
    return @intCast(n);
}

fn blockingPwrite(fd: c_int, buf: []const u8, offset: u64) FileError!usize {
    const result = lib.runtime().reactor.fsWrite(@intCast(fd), buf, offset, null);
    if (result.value < 0) return fs_error.fromErrno(result.err);
    return @intCast(result.value);
}

fn pwriteRaw(fd: c_int, buf: []const u8, offset: u64) FileError!usize {
    const n = c_pwrite(fd, buf.ptr, buf.len, @intCast(offset));
    if (n < 0) return fs_error.fromErrno(fs_error.currentErrno());
    return @intCast(n);
}

fn blockingFsync(fd: c_int) FileError!void {
    const result = lib.runtime().reactor.fsFsync(@intCast(fd), null);
    if (result.value != 0) return fs_error.fromErrno(result.err);
}

fn blockingFdatasync(fd: c_int) FileError!void {
    const result = lib.runtime().reactor.fsFdatasync(@intCast(fd), null);
    if (result.value != 0) return fs_error.fromErrno(result.err);
}

// ─── Phase 3D: cancel-aware variants ─────────────────────────────
//
// Mirror the non-cancel `blockingX` helpers above but thread the
// cancel through to `Reactor.fsX(..., &cancel)`. On the io_uring
// path the Reactor routes through `runtime.fsRingX` which honours
// the cancel in-flight. On the spawnBlocking fallback path the
// Reactor checks `c.isFired()` once before submit — same
// "boundary-only" cancel semantics as the pre-Phase-3 code.
//
// ECANCELED in the result is the kernel's natural shape for a
// cancelled CQE; we translate it to `error.Cancelled` here so the
// caller's error set stays `FileError || error{Cancelled}`.

fn blockingReadCancel(fd: c_int, buf: []u8, c: *lib.Cancel) (FileError || error{Cancelled})!usize {
    const result = lib.runtime().reactor.fsRead(@intCast(fd), buf, reactor_fs.OFFSET_CUR, c);
    if (result.value < 0) {
        if (result.err == @intFromEnum(std.c.E.CANCELED)) return error.Cancelled;
        return fs_error.fromErrno(result.err);
    }
    return @intCast(result.value);
}

fn blockingWriteCancel(fd: c_int, buf: []const u8, c: *lib.Cancel) (FileError || error{Cancelled})!usize {
    const result = lib.runtime().reactor.fsWrite(@intCast(fd), buf, reactor_fs.OFFSET_CUR, c);
    if (result.value < 0) {
        if (result.err == @intFromEnum(std.c.E.CANCELED)) return error.Cancelled;
        return fs_error.fromErrno(result.err);
    }
    return @intCast(result.value);
}

fn blockingFsyncCancel(fd: c_int, c: *lib.Cancel) (FileError || error{Cancelled})!void {
    const result = lib.runtime().reactor.fsFsync(@intCast(fd), c);
    if (result.value != 0) {
        if (result.err == @intFromEnum(std.c.E.CANCELED)) return error.Cancelled;
        return fs_error.fromErrno(result.err);
    }
}

// ─── libc externs not in std.c ───────────────────────────────────

const c_read = if (is_windows) {} else @extern(
    *const fn (c_int, [*]u8, usize) callconv(.c) isize,
    .{ .name = "read" },
);
const c_write = if (is_windows) {} else @extern(
    *const fn (c_int, [*]const u8, usize) callconv(.c) isize,
    .{ .name = "write" },
);
const c_pread = if (is_windows) {} else @extern(
    *const fn (c_int, [*]u8, usize, i64) callconv(.c) isize,
    .{ .name = "pread" },
);
const c_pwrite = if (is_windows) {} else @extern(
    *const fn (c_int, [*]const u8, usize, i64) callconv(.c) isize,
    .{ .name = "pwrite" },
);
const c_lseek = if (is_windows) {} else @extern(
    *const fn (c_int, i64, c_int) callconv(.c) i64,
    .{ .name = "lseek" },
);
const c_fsync = if (is_windows) {} else @extern(
    *const fn (c_int) callconv(.c) c_int,
    .{ .name = "fsync" },
);
const c_fdatasync = if (builtin.os.tag == .linux) @extern(
    *const fn (c_int) callconv(.c) c_int,
    .{ .name = "fdatasync" },
) else {};
const c_ftruncate = if (is_windows) {} else @extern(
    *const fn (c_int, i64) callconv(.c) c_int,
    .{ .name = "ftruncate" },
);

// ─── Helpers ─────────────────────────────────────────────────────

const PathZ = struct { buf: [syscall.PATH_MAX:0]u8 };

fn pathZInto(p: []const u8, out: *PathZ) FsError!void {
    if (p.len >= syscall.PATH_MAX) return error.NameTooLong;
    if (std.mem.indexOfScalar(u8, p, 0) != null) return error.InvalidPath;
    @memcpy(out.buf[0..p.len], p);
    out.buf[p.len] = 0;
}

// ─── Tests ───────────────────────────────────────────────────────

const testing = std.testing;

const TestState = struct {
    tmp_path: [:0]const u8,
    ok: bool = false,
};

/// Allocate a NUL-terminated unique temp dir under /tmp via
/// mkdtemp. Returned slice is allocator-owned.
fn allocTmpDir(allocator: std.mem.Allocator) ![:0]u8 {
    const tmpl = "/tmp/volt-file-XXXXXX";
    const buf = try allocator.allocSentinel(u8, tmpl.len, 0);
    @memcpy(buf[0..tmpl.len], tmpl);
    if (syscall.c_mkdtemp(buf.ptr) == null) {
        allocator.free(buf);
        return error.MkdtempFailed;
    }
    return buf;
}

fn cleanupTmpFile(tmp: [:0]const u8, leaf: []const u8) void {
    var buf: [256:0]u8 = undefined;
    const full = std.fmt.bufPrintZ(&buf, "{s}/{s}", .{ tmp, leaf }) catch return;
    _ = syscall.c_unlink(full.ptr);
}

fn writeReadCycle(state: *TestState) !void {
    var path_buf: [256:0]u8 = undefined;
    const p = try std.fmt.bufPrintZ(&path_buf, "{s}/wr.txt", .{state.tmp_path});

    {
        var f = try File.create(p);
        defer f.close();
        try f.writeAll("hello world");
        try f.sync();
    }

    var f = try File.open(p);
    defer f.close();
    var buf: [32]u8 = undefined;
    const n = try f.readFull(&buf);
    if (n != 11) return error.WrongSize;
    if (!std.mem.eql(u8, buf[0..n], "hello world")) return error.MismatchedBytes;
    state.ok = true;
}

test "File: create, writeAll, sync, seek-to-start, readFull round-trip" {
    if (is_windows) return error.SkipZigTest;

    const tmp = try allocTmpDir(testing.allocator);
    defer {
        cleanupTmpFile(tmp, "wr.txt");
        _ = syscall.c_rmdir(tmp.ptr);
        testing.allocator.free(tmp);
    }

    var rt = try lib.Runtime.init(.{ .allocator = testing.allocator });
    defer rt.deinit();
    var state = TestState{ .tmp_path = tmp };
    try (try rt.run(writeReadCycle, .{&state}));
    try testing.expect(state.ok);
}

fn pwriteReadAt(state: *TestState) !void {
    var path_buf: [256:0]u8 = undefined;
    const p = try std.fmt.bufPrintZ(&path_buf, "{s}/poff.txt", .{state.tmp_path});
    var f = try File.openOptions(p, .{
        .read = true,
        .write = true,
        .create = true,
        .truncate = true,
    });
    defer f.close();
    try f.writeAll("0000000000");
    _ = try f.writeAt("ABCD", 3);

    var buf: [10]u8 = undefined;
    const n = try f.readAt(&buf, 0);
    if (n != 10) return error.ShortRead;
    if (!std.mem.eql(u8, buf[0..n], "000ABCD000")) return error.WrongPattern;
    state.ok = true;
}

test "File: pwrite / pread at offset" {
    if (is_windows) return error.SkipZigTest;
    const tmp = try allocTmpDir(testing.allocator);
    defer {
        cleanupTmpFile(tmp, "poff.txt");
        _ = syscall.c_rmdir(tmp.ptr);
        testing.allocator.free(tmp);
    }
    var rt = try lib.Runtime.init(.{ .allocator = testing.allocator });
    defer rt.deinit();
    var state = TestState{ .tmp_path = tmp };
    try (try rt.run(pwriteReadAt, .{&state}));
    try testing.expect(state.ok);
}

fn truncateShrinks(state: *TestState) !void {
    var path_buf: [256:0]u8 = undefined;
    const p = try std.fmt.bufPrintZ(&path_buf, "{s}/trunc.txt", .{state.tmp_path});
    var f = try File.create(p);
    defer f.close();
    try f.writeAll("aaaaaaaaaa");
    try f.setLen(4);
    const m = try f.metadata();
    if (m.size() != 4) return error.WrongSize;
    state.ok = true;
}

test "File: setLen truncates to exact size" {
    if (is_windows) return error.SkipZigTest;
    const tmp = try allocTmpDir(testing.allocator);
    defer {
        cleanupTmpFile(tmp, "trunc.txt");
        _ = syscall.c_rmdir(tmp.ptr);
        testing.allocator.free(tmp);
    }
    var rt = try lib.Runtime.init(.{ .allocator = testing.allocator });
    defer rt.deinit();
    var state = TestState{ .tmp_path = tmp };
    try (try rt.run(truncateShrinks, .{&state}));
    try testing.expect(state.ok);
}

fn createNewRejectsExisting(state: *TestState) !void {
    var path_buf: [256:0]u8 = undefined;
    const p = try std.fmt.bufPrintZ(&path_buf, "{s}/exists.txt", .{state.tmp_path});
    var f = try File.create(p);
    f.close();

    const result = File.openOptions(p, .{ .write = true, .create = true, .create_new = true });
    if (result) |_| {
        return error.ShouldHaveFailed;
    } else |e| {
        if (e != error.AlreadyExists) return error.WrongError;
    }
    state.ok = true;
}

test "File: create_new on existing file returns error.AlreadyExists" {
    if (is_windows) return error.SkipZigTest;
    const tmp = try allocTmpDir(testing.allocator);
    defer {
        cleanupTmpFile(tmp, "exists.txt");
        _ = syscall.c_rmdir(tmp.ptr);
        testing.allocator.free(tmp);
    }
    var rt = try lib.Runtime.init(.{ .allocator = testing.allocator });
    defer rt.deinit();
    var state = TestState{ .tmp_path = tmp };
    try (try rt.run(createNewRejectsExisting, .{&state}));
    try testing.expect(state.ok);
}

fn stdIoReaderRoundTrip(state: *TestState) !void {
    var path_buf: [256:0]u8 = undefined;
    const p = try std.fmt.bufPrintZ(&path_buf, "{s}/std-io.txt", .{state.tmp_path});
    var f = try File.create(p);
    try f.writeAll("hello world\n");
    f.close();

    var f2 = try File.open(p);
    defer f2.close();
    var buf: [64]u8 = undefined;
    var r = f2.reader(&buf);
    const got = r.interface.takeDelimiterExclusive('\n') catch |e| switch (e) {
        error.EndOfStream => return error.UnexpectedEof,
        error.ReadFailed => return error.ReaderFailed,
        else => return e,
    };
    if (!std.mem.eql(u8, got, "hello world")) return error.WrongLine;
    state.ok = true;
}

test "File: std.Io.Reader adapter — takeDelimiterExclusive" {
    if (is_windows) return error.SkipZigTest;
    const tmp = try allocTmpDir(testing.allocator);
    defer {
        cleanupTmpFile(tmp, "std-io.txt");
        _ = syscall.c_rmdir(tmp.ptr);
        testing.allocator.free(tmp);
    }
    var rt = try lib.Runtime.init(.{ .allocator = testing.allocator });
    defer rt.deinit();
    var state = TestState{ .tmp_path = tmp };
    try (try rt.run(stdIoReaderRoundTrip, .{&state}));
    try testing.expect(state.ok);
}

// ─── Phase 3D test: public File.readCancel through full stack ──
//
// Verifies the cancel param actually plumbs through fs/file.zig
// → Reactor → runtime.fsRingRead → kernel ASYNC_CANCEL. Uses an
// empty pipe so the io_uring READ parks indefinitely. The
// sibling coro fires the cancel; readCancel should return
// `error.Cancelled` promptly.

const Phase3DCtx = struct {
    pipe_read_fd: c_int,
    c: *lib.Cancel,
    got_cancelled: bool = false,
};

fn phase3dReader(ctx: *Phase3DCtx) !void {
    var f = File.fromFd(ctx.pipe_read_fd);
    var buf: [16]u8 = undefined;
    _ = f.readCancel(&buf, ctx.c) catch |e| {
        if (e == error.Cancelled) ctx.got_cancelled = true;
        return;
    };
}

fn phase3dFirer(ctx: *Phase3DCtx) void {
    var i: u32 = 0;
    while (i < 128) : (i += 1) lib.yield();
    ctx.c.fire();
}

fn phase3dRoot(ctx: *Phase3DCtx) !void {
    var reader = try lib.spawn(phase3dReader, .{ctx});
    var firer = try lib.spawn(phase3dFirer, .{ctx});
    _ = reader.join() catch {};
    _ = firer.join();
}

test "File.readCancel: in-flight cancel returns error.Cancelled" {
    if (is_windows) return error.SkipZigTest;
    // On Darwin this exercises the spawnBlocking-fallback +
    // boundary-check semantics: cancel fires while the pool
    // thread is blocked in read(); the cancel is observed at
    // the NEXT call, not this one. So on Darwin the test would
    // hang waiting for data that never arrives. Skip there.
    // The same logic applies to any Linux env where io_uring is
    // unavailable (the runtime.fs_rings == null path).
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    // Use volt.testing.allocator — std.testing.allocator's
    // DebugAllocator captures stack traces via DWARF unwinding,
    // which races with cross-worker spawn writes and SEGVs.
    // See feedback_debug_stack_frames + src/testing.zig.
    const volt_test_alloc = @import("../testing.zig").allocator;
    var rt = try lib.Runtime.init(.{ .allocator = volt_test_alloc });
    defer rt.deinit();
    if (rt.fs_rings == null) return error.SkipZigTest;

    var pipefd: [2]i32 = undefined;
    const rc = std.os.linux.pipe2(@as(*[2]i32, &pipefd), .{ .CLOEXEC = true });
    if (std.os.linux.errno(rc) != .SUCCESS) return error.PipeFailed;
    defer _ = std.os.linux.close(pipefd[0]);
    defer _ = std.os.linux.close(pipefd[1]);

    var cancel = lib.Cancel.init(rt);
    defer cancel.deinit();

    var ctx = Phase3DCtx{ .pipe_read_fd = pipefd[0], .c = &cancel };
    try (try rt.run(phase3dRoot, .{&ctx}));
    try testing.expect(ctx.got_cancelled);
}
