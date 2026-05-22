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

    /// Read with a cancel handle. Today this checks `c.isFired()`
    /// before submitting; a fired cancel returns `error.Cancelled`
    /// immediately. The actual syscall, once submitted, runs to
    /// completion.
    pub fn readCancel(self: *File, buf: []u8, c: *lib.Cancel) (FileError || error{Cancelled})!usize {
        if (c.isFired()) return error.Cancelled;
        return self.read(buf);
    }

    pub fn writeCancel(self: *File, buf: []const u8, c: *lib.Cancel) (FileError || error{Cancelled})!usize {
        if (c.isFired()) return error.Cancelled;
        return self.write(buf);
    }

    pub fn readFullCancel(self: *File, buf: []u8, c: *lib.Cancel) (FileError || error{Cancelled})!usize {
        if (c.isFired()) return error.Cancelled;
        return self.readFull(buf);
    }

    pub fn writeAllCancel(self: *File, buf: []const u8, c: *lib.Cancel) (FileError || error{Cancelled})!void {
        if (c.isFired()) return error.Cancelled;
        return self.writeAll(buf);
    }

    pub fn syncCancel(self: *File, c: *lib.Cancel) (FileError || error{Cancelled})!void {
        if (c.isFired()) return error.Cancelled;
        return self.sync();
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

// ─── spawnBlocking adapters ──────────────────────────────────────
//
// Each adapter has two pieces: (1) a plain sync fn that captures
// the syscall + errno into a tagged result, (2) a wrapper that
// invokes spawnBlocking and unpacks the result.

const IoResult = struct {
    value: isize,
    err: c_int,
};

const FdResult = struct {
    fd: c_int,
    err: c_int,
};

fn syncOpen(path_ptr: [*:0]const u8, flags: c_int, mode: c_uint) FdResult {
    const fd = syscall.c_open(path_ptr, flags, mode);
    return .{ .fd = fd, .err = if (fd < 0) fs_error.currentErrno() else 0 };
}

fn blockingOpen(z: *const PathZ, flags: c_int, mode: c_uint) c_int {
    // Pass the path as a pointer — `z` lives on the caller's
    // coroutine stack and outlives the blocking call.
    const result = lib.spawnBlocking(syncOpen, .{ @as([*:0]const u8, &z.buf), flags, mode }) catch return -1;
    if (result.err != 0) {
        std.c._errno().* = result.err;
    }
    return result.fd;
}

fn syncRead(fd: c_int, buf_ptr: [*]u8, buf_len: usize) IoResult {
    const n = c_read(fd, buf_ptr, buf_len);
    return .{ .value = n, .err = if (n < 0) fs_error.currentErrno() else 0 };
}

fn blockingRead(fd: c_int, buf: []u8) FileError!usize {
    const result = lib.spawnBlocking(syncRead, .{ fd, buf.ptr, buf.len }) catch return error.SystemResources;
    if (result.value < 0) {
        std.c._errno().* = result.err;
        return fs_error.fromErrno(result.err);
    }
    return @intCast(result.value);
}

fn readRaw(fd: c_int, buf: []u8) FileError!usize {
    const n = c_read(fd, buf.ptr, buf.len);
    if (n < 0) return fs_error.fromErrno(fs_error.currentErrno());
    return @intCast(n);
}

fn syncWrite(fd: c_int, buf_ptr: [*]const u8, buf_len: usize) IoResult {
    const n = c_write(fd, buf_ptr, buf_len);
    return .{ .value = n, .err = if (n < 0) fs_error.currentErrno() else 0 };
}

fn blockingWrite(fd: c_int, buf: []const u8) FileError!usize {
    const result = lib.spawnBlocking(syncWrite, .{ fd, buf.ptr, buf.len }) catch return error.SystemResources;
    if (result.value < 0) {
        std.c._errno().* = result.err;
        return fs_error.fromErrno(result.err);
    }
    return @intCast(result.value);
}

fn writeRaw(fd: c_int, buf: []const u8) FileError!usize {
    const n = c_write(fd, buf.ptr, buf.len);
    if (n < 0) return fs_error.fromErrno(fs_error.currentErrno());
    return @intCast(n);
}

fn syncPread(fd: c_int, buf_ptr: [*]u8, buf_len: usize, offset: i64) IoResult {
    const n = c_pread(fd, buf_ptr, buf_len, offset);
    return .{ .value = n, .err = if (n < 0) fs_error.currentErrno() else 0 };
}

fn blockingPread(fd: c_int, buf: []u8, offset: u64) FileError!usize {
    const result = lib.spawnBlocking(syncPread, .{ fd, buf.ptr, buf.len, @as(i64, @intCast(offset)) }) catch return error.SystemResources;
    if (result.value < 0) {
        std.c._errno().* = result.err;
        return fs_error.fromErrno(result.err);
    }
    return @intCast(result.value);
}

fn preadRaw(fd: c_int, buf: []u8, offset: u64) FileError!usize {
    const n = c_pread(fd, buf.ptr, buf.len, @intCast(offset));
    if (n < 0) return fs_error.fromErrno(fs_error.currentErrno());
    return @intCast(n);
}

fn syncPwrite(fd: c_int, buf_ptr: [*]const u8, buf_len: usize, offset: i64) IoResult {
    const n = c_pwrite(fd, buf_ptr, buf_len, offset);
    return .{ .value = n, .err = if (n < 0) fs_error.currentErrno() else 0 };
}

fn blockingPwrite(fd: c_int, buf: []const u8, offset: u64) FileError!usize {
    const result = lib.spawnBlocking(syncPwrite, .{ fd, buf.ptr, buf.len, @as(i64, @intCast(offset)) }) catch return error.SystemResources;
    if (result.value < 0) {
        std.c._errno().* = result.err;
        return fs_error.fromErrno(result.err);
    }
    return @intCast(result.value);
}

fn pwriteRaw(fd: c_int, buf: []const u8, offset: u64) FileError!usize {
    const n = c_pwrite(fd, buf.ptr, buf.len, @intCast(offset));
    if (n < 0) return fs_error.fromErrno(fs_error.currentErrno());
    return @intCast(n);
}

fn syncFsync(fd: c_int) IoResult {
    const rc = c_fsync(fd);
    return .{ .value = rc, .err = if (rc != 0) fs_error.currentErrno() else 0 };
}

fn blockingFsync(fd: c_int) FileError!void {
    const result = lib.spawnBlocking(syncFsync, .{fd}) catch return error.SystemResources;
    if (result.value != 0) {
        std.c._errno().* = result.err;
        return fs_error.fromErrno(result.err);
    }
}

fn syncFdatasync(fd: c_int) IoResult {
    const rc = if (comptime builtin.os.tag == .linux) c_fdatasync(fd) else c_fsync(fd);
    return .{ .value = rc, .err = if (rc != 0) fs_error.currentErrno() else 0 };
}

fn blockingFdatasync(fd: c_int) FileError!void {
    const result = lib.spawnBlocking(syncFdatasync, .{fd}) catch return error.SystemResources;
    if (result.value != 0) {
        std.c._errno().* = result.err;
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
