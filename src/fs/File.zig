//! `volt.fs.File` — async file handle.
//!
//! Wraps a regular-file fd and routes blocking I/O through the
//! blocking pool. The calling coroutine parks while the pool thread
//! does the syscall; the worker thread is free to run other
//! coroutines.
//!
//! ## Why blocking-pool dispatch
//!
//! Regular files don't fit in kqueue / epoll readiness models —
//! they're "always ready" but the actual `read` can stall the worker
//! on slow disk or network-mounted filesystems. The blocking pool is
//! Volt's escape hatch: pay a thread-handoff cost per call to keep
//! workers responsive. Tokio + asyncio + .NET all do the same shape.
//!
//! When io_uring lands as a Linux backend (post-v1.1), File ops will
//! flip to true-async on the reactor without changing this API.
//!
//! ## Trait surface
//!
//! Implements all six `volt.io` traits. `as_fd` is populated, so
//! `volt.io.copy(socket.writer(), file.reader())` will dispatch to
//! `sendfile` once P3.E wires it in.

const std = @import("std");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const spawnBlocking = @import("../api/spawn_blocking.zig").spawnBlocking;
const traits = @import("../io/traits/traits.zig");
const io_errors = @import("../io/errors.zig");
const Metadata = @import("Metadata.zig").Metadata;
const OpenOptionsT = @import("OpenOptions.zig").OpenOptions;

pub const File = struct {
    fd: posix.fd_t,

    /// Open a file with the given options (convenience over
    /// `OpenOptions.open`).
    pub fn open(path: []const u8, opts: OpenOptionsT) !File {
        return opts.open(path);
    }

    /// Open for reading. Convenience for `OpenOptions{ .read = true }`.
    pub fn openRead(path: []const u8) !File {
        return (OpenOptionsT{ .read = true }).open(path);
    }

    /// Create or truncate for writing.
    pub fn create(path: []const u8) !File {
        return (OpenOptionsT{ .write = true, .create = true, .truncate = true }).open(path);
    }

    pub fn close(self: *File) void {
        syscall.close(self.fd);
    }

    // ── I/O ────────────────────────────────────────────────────────────

    pub fn read(self: *File, buf: []u8) !usize {
        const Args = struct { fd: posix.fd_t, buf: []u8 };
        var args = Args{ .fd = self.fd, .buf = buf };
        return spawnBlocking(struct {
            fn run(a: *Args) !usize {
                return syscall.read(a.fd, a.buf);
            }
        }.run, .{&args});
    }

    pub fn write(self: *File, buf: []const u8) !usize {
        const Args = struct { fd: posix.fd_t, buf: []const u8 };
        var args = Args{ .fd = self.fd, .buf = buf };
        return spawnBlocking(struct {
            fn run(a: *Args) !usize {
                return syscall.write(a.fd, a.buf);
            }
        }.run, .{&args});
    }

    pub fn writeAll(self: *File, buf: []const u8) !void {
        var written: usize = 0;
        while (written < buf.len) {
            const n = try self.write(buf[written..]);
            if (n == 0) return error.WriteFailed;
            written += n;
        }
    }

    /// Positional read — doesn't move the seek cursor. Multiple
    /// coroutines can call `pread` on the same File concurrently
    /// without seek-then-read races.
    pub fn pread(self: *File, buf: []u8, offset: u64) !usize {
        const Args = struct { fd: posix.fd_t, buf: []u8, off: u64 };
        var args = Args{ .fd = self.fd, .buf = buf, .off = offset };
        return spawnBlocking(struct {
            fn run(a: *Args) !usize {
                return syscall.pread(a.fd, a.buf, a.off);
            }
        }.run, .{&args});
    }

    pub fn pwrite(self: *File, buf: []const u8, offset: u64) !usize {
        const Args = struct { fd: posix.fd_t, buf: []const u8, off: u64 };
        var args = Args{ .fd = self.fd, .buf = buf, .off = offset };
        return spawnBlocking(struct {
            fn run(a: *Args) !usize {
                return syscall.pwrite(a.fd, a.buf, a.off);
            }
        }.run, .{&args});
    }

    // ── Seek / size ────────────────────────────────────────────────────
    //
    // lseek is fast (no I/O); call it directly without blocking-pool
    // dispatch. Same goes for ftruncate (path is fast for size 0;
    // slow only for large pre-allocations, which most callers don't
    // hit).

    pub fn seekTo(self: *File, pos: u64) !void {
        return syscall.lseekSet(self.fd, pos);
    }

    pub fn seekBy(self: *File, delta: i64) !void {
        return syscall.lseekCur(self.fd, delta);
    }

    pub fn getPos(self: *File) !u64 {
        return syscall.lseekCurPos(self.fd);
    }

    pub fn getEndPos(self: *File) !u64 {
        const orig = try syscall.lseekCurPos(self.fd);
        const end = try syscall.lseekEnd(self.fd, 0);
        try syscall.lseekSet(self.fd, orig);
        return end;
    }

    pub fn truncate(self: *File, len: u64) !void {
        return syscall.ftruncate(self.fd, len);
    }

    // ── Sync / metadata ────────────────────────────────────────────────

    /// Flush all buffered data + metadata to disk. Blocking-pool
    /// dispatched — fsync can take seconds on slow disks.
    pub fn sync(self: *File) !void {
        const Args = struct { fd: posix.fd_t };
        var args = Args{ .fd = self.fd };
        return spawnBlocking(struct {
            fn run(a: *Args) !void {
                return syscall.fsync(a.fd);
            }
        }.run, .{&args});
    }

    /// Like `sync` but doesn't flush metadata. Faster on filesystems
    /// that distinguish (ext4, xfs); equivalent to sync on others.
    /// Today this is identical to `sync` — fdatasync wrapper lands
    /// alongside io_uring file ops.
    pub fn syncData(self: *File) !void {
        return self.sync();
    }

    pub fn metadata(self: *File) !Metadata {
        const Args = struct { fd: posix.fd_t };
        var args = Args{ .fd = self.fd };
        const stat = try spawnBlocking(struct {
            fn run(a: *Args) !posix.Stat {
                return syscall.fstat(a.fd);
            }
        }.run, .{&args});
        return Metadata.fromPosixStat(stat);
    }

    // ── Trait surface ──────────────────────────────────────────────────
    //
    // Each method returns a trait wrapper backed by the File's fd.
    // The vtable methods do the same blocking-pool dispatch as the
    // direct File methods — composition with BufReader / copy() / etc.
    // works the same as for TcpStream.

    pub fn reader(self: *File) traits.Reader {
        return .{ .ctx = @ptrCast(self), .vtable = &reader_vtable };
    }

    pub fn writer(self: *File) traits.Writer {
        return .{ .ctx = @ptrCast(self), .vtable = &writer_vtable };
    }

    pub fn closer(self: *File) traits.Closer {
        return .{ .ctx = @ptrCast(self), .vtable = &closer_vtable };
    }

    pub fn readerAt(self: *File) traits.ReaderAt {
        return .{ .ctx = @ptrCast(self), .vtable = &reader_at_vtable };
    }

    pub fn writerAt(self: *File) traits.WriterAt {
        return .{ .ctx = @ptrCast(self), .vtable = &writer_at_vtable };
    }

    pub fn seeker(self: *File) traits.Seeker {
        return .{ .ctx = @ptrCast(self), .vtable = &seeker_vtable };
    }

    const reader_vtable: traits.Reader.VTable = .{
        .read = &traitRead,
        .as_fd = &traitAsFd,
    };

    const writer_vtable: traits.Writer.VTable = .{
        .write = &traitWrite,
        .as_fd = &traitAsFd,
    };

    const closer_vtable: traits.Closer.VTable = .{ .close = &traitClose };

    const reader_at_vtable: traits.ReaderAt.VTable = .{ .readAt = &traitReadAt };
    const writer_at_vtable: traits.WriterAt.VTable = .{ .writeAt = &traitWriteAt };

    const seeker_vtable: traits.Seeker.VTable = .{
        .seekTo = &traitSeekTo,
        .seekBy = &traitSeekBy,
        .getPos = &traitGetPos,
        .getEndPos = &traitGetEndPos,
    };

    fn traitRead(ctx: *anyopaque, buf: []u8) io_errors.ReadError!usize {
        const self: *File = @ptrCast(@alignCast(ctx));
        return self.read(buf) catch |e| widenRead(e);
    }

    fn traitWrite(ctx: *anyopaque, buf: []const u8) io_errors.WriteError!usize {
        const self: *File = @ptrCast(@alignCast(ctx));
        return self.write(buf) catch |e| widenWrite(e);
    }

    fn traitReadAt(ctx: *anyopaque, buf: []u8, offset: u64) io_errors.ReadError!usize {
        const self: *File = @ptrCast(@alignCast(ctx));
        return self.pread(buf, offset) catch |e| widenRead(e);
    }

    fn traitWriteAt(ctx: *anyopaque, buf: []const u8, offset: u64) io_errors.WriteError!usize {
        const self: *File = @ptrCast(@alignCast(ctx));
        return self.pwrite(buf, offset) catch |e| widenWrite(e);
    }

    fn traitClose(ctx: *anyopaque) void {
        const self: *File = @ptrCast(@alignCast(ctx));
        syscall.close(self.fd);
    }

    fn traitAsFd(ctx: *anyopaque) ?posix.fd_t {
        const self: *File = @ptrCast(@alignCast(ctx));
        return self.fd;
    }

    fn traitSeekTo(ctx: *anyopaque, pos: u64) io_errors.SeekError!void {
        const self: *File = @ptrCast(@alignCast(ctx));
        return self.seekTo(pos) catch error.Unseekable;
    }

    fn traitSeekBy(ctx: *anyopaque, delta: i64) io_errors.SeekError!void {
        const self: *File = @ptrCast(@alignCast(ctx));
        return self.seekBy(delta) catch error.Unseekable;
    }

    fn traitGetPos(ctx: *anyopaque) io_errors.SeekError!u64 {
        const self: *File = @ptrCast(@alignCast(ctx));
        return self.getPos() catch error.Unseekable;
    }

    fn traitGetEndPos(ctx: *anyopaque) io_errors.SeekError!u64 {
        const self: *File = @ptrCast(@alignCast(ctx));
        return self.getEndPos() catch error.Unseekable;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Trait error widening — File's I/O methods return inferred error
// sets (anyerror) because spawnBlocking widens to anyerror internally.
// The trait vtables narrow the result to the trait's declared error
// set via @errorCast.
// ─────────────────────────────────────────────────────────────────────

fn widenRead(err: anyerror) io_errors.ReadError {
    return @errorCast(err);
}

fn widenWrite(err: anyerror) io_errors.WriteError {
    return @errorCast(err);
}
