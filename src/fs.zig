//! Filesystem namespace facade — `volt.fs.{path, Metadata, ...}`.
//!
//! Volt's fs surface targets Node.js / Go-scale exhaustiveness with
//! a stackful-coroutine ergonomic. Today (Phase B.1) exposes the
//! metadata vocabulary + path-only stat / access / chmod helpers.
//! Subsequent Phase B waves add `File` (B.2), `Dir` (B.3),
//! `MappedFile` (B.4), `Watcher` (B.5), and the streaming
//! convenience facade (B.6: `readFile`, `copyFile`, `tempFile`, …).

const std = @import("std");
const builtin = @import("builtin");
const syscall = @import("fs/syscall.zig");
const fs_error = @import("fs/error.zig");

const is_windows = builtin.os.tag == .windows;

// ─── Re-exports ──────────────────────────────────────────────────

pub const path = @import("fs/path.zig");

pub const Metadata = @import("fs/metadata.zig").Metadata;
pub const FileType = @import("fs/metadata.zig").FileType;
pub const Permissions = @import("fs/metadata.zig").Permissions;
pub const SystemTime = @import("fs/metadata.zig").SystemTime;
pub const PlatformStat = @import("fs/metadata.zig").PlatformStat;

pub const File = @import("fs/file.zig").File;
pub const OpenOptions = @import("fs/file.zig").OpenOptions;

pub const Dir = @import("fs/dir.zig").Dir;
pub const DirEntry = @import("fs/dir.zig").Entry;
pub const WalkAction = @import("fs/dir.zig").WalkAction;
pub const WalkOptions = @import("fs/dir.zig").WalkOptions;
pub const glob = @import("fs/dir.zig").glob;

pub const MappedFile = @import("fs/mmap.zig").MappedFile;
pub const MmapProtection = @import("fs/mmap.zig").Protection;
pub const MmapSharing = @import("fs/mmap.zig").Sharing;
pub const MmapOptions = @import("fs/mmap.zig").MapOptions;
pub const MmapAnonOptions = @import("fs/mmap.zig").AnonOptions;
pub const MmapAdvice = @import("fs/mmap.zig").Advice;
pub const mapFile = @import("fs/mmap.zig").mapFile;
pub const mapAnonymous = @import("fs/mmap.zig").mapAnonymous;

pub const FsError = fs_error.FsError;
pub const FileError = fs_error.FileError;

// ─── Stat / lstat / fstat ────────────────────────────────────────

/// Look up metadata for `path`. Follows symlinks — to inspect the
/// link itself, use `lstat`.
///
/// Common errors: `FsError.NotFound` if the path doesn't exist,
/// `FsError.AccessDenied` on a component without search permission.
pub fn stat(file_path: []const u8) FsError!Metadata {
    if (is_windows) @compileError("Windows stat: pending (Phase B.2)");
    var z = try pathZ(file_path);
    var buf: std.c.Stat = undefined;
    if (syscall.stat(&z.buf, &buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    return Metadata.fromStat(buf);
}

/// Like `stat` but does not follow a terminal symlink — returns
/// info about the symlink itself.
pub fn lstat(file_path: []const u8) FsError!Metadata {
    if (is_windows) @compileError("Windows lstat: pending (Phase B.2)");
    var z = try pathZ(file_path);
    var buf: std.c.Stat = undefined;
    if (syscall.lstat(&z.buf, &buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    return Metadata.fromStat(buf);
}

/// Stat by file descriptor. The handle must be currently open;
/// works for any fd that's bound to a filesystem object (regular
/// file, directory, socket).
pub fn fstat(fd: c_int) FsError!Metadata {
    if (is_windows) @compileError("Windows fstat: pending (Phase B.2)");
    var buf: std.c.Stat = undefined;
    if (syscall.fstat(fd, &buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    return Metadata.fromStat(buf);
}

// ─── Existence + access checks ───────────────────────────────────

/// `true` if the path exists. Hides "not found" but propagates
/// permission-style errors via panic-free `false` — for the strict
/// version that distinguishes, use `tryExists`.
pub fn exists(file_path: []const u8) bool {
    return tryExists(file_path) catch false;
}

/// `true` if the path exists. Errors for everything except
/// `NotFound` — that way callers can distinguish "definitely
/// doesn't exist" from "couldn't tell" (e.g. ACL on a parent dir).
pub fn tryExists(file_path: []const u8) FsError!bool {
    if (is_windows) @compileError("Windows tryExists: pending (Phase B.2)");
    var z = try pathZ(file_path);
    var buf: std.c.Stat = undefined;
    if (syscall.lstat(&z.buf, &buf) == 0) return true;
    const e = fs_error.fromErrno(fs_error.currentErrno());
    if (e == error.NotFound) return false;
    return e;
}

/// Permissions vector for `access()`. Combine with `.{}`-style
/// optional fields — every unset field defaults to `false` (don't
/// check). `F_OK` (file-exists check) is implicit when no other
/// bits are set.
pub const AccessMode = struct {
    read: bool = false,
    write: bool = false,
    execute: bool = false,
};

/// `true` if the caller's effective uid/gid has the requested
/// permissions on `path`. Wraps POSIX `access(2)`. Note the
/// classic TOCTOU caveat — between the `access` check and any
/// subsequent open, permissions can change.
pub fn access(file_path: []const u8, mode: AccessMode) bool {
    if (is_windows) @compileError("Windows access: pending (Phase B.2)");
    var z = pathZ(file_path) catch return false;
    var bits: c_uint = syscall.F_OK;
    if (mode.read) bits |= syscall.R_OK;
    if (mode.write) bits |= syscall.W_OK;
    if (mode.execute) bits |= syscall.X_OK;
    return syscall.access(&z.buf, bits) == 0;
}

// ─── Permissions + ownership ─────────────────────────────────────

/// Set the permission bits on `path`. Replaces the entire mode —
/// to flip a single bit, `stat` first to get the current
/// permissions, mutate, then `chmod` back.
pub fn chmod(file_path: []const u8, perms: Permissions) FsError!void {
    if (is_windows) @compileError("Windows chmod: pending (Phase B.2)");
    var z = try pathZ(file_path);
    const mode: std.c.mode_t = @intCast(perms.getMode());
    if (syscall.chmod(&z.buf, mode) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// Change ownership of `path`. Most callers need to be root for
/// this to succeed; the typical use case is install scripts.
pub fn chown(file_path: []const u8, uid: u32, gid: u32) FsError!void {
    if (is_windows) @compileError("Windows chown: pending (Phase B.2)");
    var z = try pathZ(file_path);
    if (syscall.chown(&z.buf, @intCast(uid), @intCast(gid)) != 0) {
        return fs_error.fromErrno(fs_error.currentErrno());
    }
}

/// Set access + modification times on `path`. Nanosecond-precision
/// via `utimensat`; the kernel rounds to the filesystem's
/// resolution (commonly 1µs on ext4, 1s on FAT).
pub fn setTimes(file_path: []const u8, atime: SystemTime, mtime: SystemTime) FsError!void {
    if (is_windows) @compileError("Windows setTimes: pending (Phase B.2)");
    var z = try pathZ(file_path);
    const times = [2]std.c.timespec{ atime.toTimespec(), mtime.toTimespec() };
    if (syscall.utimensat(syscall.AT_FDCWD, &z.buf, &times, 0) != 0) {
        return fs_error.fromErrno(fs_error.currentErrno());
    }
}

/// Resolve `path` to an absolute, symlink-free, `..`-free form.
/// Returns an owned slice; caller frees with the same allocator.
///
/// Errors for paths that don't fully resolve (e.g. a component is
/// missing or unreadable).
pub fn canonicalize(allocator: std.mem.Allocator, file_path: []const u8) (FsError || error{OutOfMemory})![]u8 {
    if (is_windows) @compileError("Windows canonicalize: pending (Phase B.2)");
    var z = try pathZ(file_path);
    // realpath wants a PATH_MAX buffer. Stack-allocate, then dup
    // into the caller's allocator.
    var buf: [syscall.PATH_MAX]u8 = undefined;
    const result = syscall.realpath(&z.buf, &buf);
    if (result == null) return fs_error.fromErrno(fs_error.currentErrno());
    const len = std.mem.len(result.?);
    return try allocator.dupe(u8, result.?[0..len]);
}

// ─── Internal: NUL-terminate a path on the stack ─────────────────

const PathZ = struct {
    buf: [syscall.PATH_MAX:0]u8,
};

/// Copy `p` into a stack buffer with a trailing NUL for handing to
/// libc. Errors if `p` is too long or contains an embedded NUL
/// (libc would silently truncate).
fn pathZ(p: []const u8) FsError!PathZ {
    if (p.len >= syscall.PATH_MAX) return error.NameTooLong;
    if (std.mem.indexOfScalar(u8, p, 0) != null) return error.InvalidPath;
    var z: PathZ = undefined;
    @memcpy(z.buf[0..p.len], p);
    z.buf[p.len] = 0;
    return z;
}

// ─── Test discovery anchors ──────────────────────────────────────

test {
    _ = @import("fs/path.zig");
    _ = @import("fs/metadata.zig");
    _ = @import("fs/syscall.zig");
    _ = @import("fs/error.zig");
    _ = @import("fs/file.zig");
    _ = @import("fs/dir.zig");
    _ = @import("fs/mmap.zig");
}

// ─── Tests ───────────────────────────────────────────────────────

const testing = std.testing;

/// Minimal scoped temp dir built on libc — std.Io.Dir wants an `io`
/// arg in Zig 0.16 and we don't run an Io instance in unit tests.
const TmpDir = struct {
    path: [:0]u8,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) !TmpDir {
        const template = "/tmp/volt-fstest-XXXXXX";
        const buf = try allocator.allocSentinel(u8, template.len, 0);
        @memcpy(buf[0..template.len], template);
        const result = syscall.c_mkdtemp(buf.ptr);
        if (result == null) {
            allocator.free(buf);
            return error.MkdtempFailed;
        }
        return .{ .path = buf, .allocator = allocator };
    }

    fn deinit(self: *TmpDir) void {
        // Best-effort cleanup: rmdir; leftover children leak (tests
        // are responsible for cleaning them up).
        _ = syscall.c_rmdir(self.path.ptr);
        self.allocator.free(self.path);
    }

    /// Make `path/sub` and return an owned NUL-terminated path.
    fn touchFile(self: *TmpDir, sub: []const u8) ![:0]u8 {
        const full = try std.fmt.allocPrintSentinel(self.allocator, "{s}/{s}", .{ self.path, sub }, 0);
        const fd = syscall.c_open(full.ptr, syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC, 0o644);
        if (fd < 0) {
            self.allocator.free(full);
            return error.OpenFailed;
        }
        _ = syscall.c_close(fd);
        return full;
    }

    fn writeFile(self: *TmpDir, sub: []const u8, data: []const u8) ![:0]u8 {
        const full = try std.fmt.allocPrintSentinel(self.allocator, "{s}/{s}", .{ self.path, sub }, 0);
        const fd = syscall.c_open(full.ptr, syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC, 0o644);
        if (fd < 0) {
            self.allocator.free(full);
            return error.OpenFailed;
        }
        defer _ = syscall.c_close(fd);
        if (data.len > 0) {
            const n = syscall.c_write(fd, data.ptr, data.len);
            if (n != @as(isize, @intCast(data.len))) {
                self.allocator.free(full);
                return error.WriteFailed;
            }
        }
        return full;
    }

    fn mkSubdir(self: *TmpDir, sub: []const u8) ![:0]u8 {
        const full = try std.fmt.allocPrintSentinel(self.allocator, "{s}/{s}", .{ self.path, sub }, 0);
        if (syscall.c_mkdir(full.ptr, 0o755) != 0) {
            self.allocator.free(full);
            return error.MkdirFailed;
        }
        return full;
    }

    fn rm(self: *TmpDir, p: [:0]u8) void {
        _ = syscall.c_unlink(p.ptr);
        self.allocator.free(p);
    }

    fn rmdir(self: *TmpDir, p: [:0]u8) void {
        _ = syscall.c_rmdir(p.ptr);
        self.allocator.free(p);
    }
};

test "fs.stat: round-trip on temp file" {
    if (is_windows) return error.SkipZigTest;
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();
    const fp = try tmp.writeFile("stat-test.txt", "hello");
    defer tmp.rm(fp);

    // Pin the mode so the assertion isn't subject to the process
    // umask of the test runner.
    try chmod(fp, Permissions.fromOctal(0o644));

    const m = try stat(fp);
    try testing.expect(m.isFile());
    try testing.expectEqual(@as(u64, 5), m.size());
    try testing.expect(m.permissions().ownerRead());
    try testing.expect(m.permissions().ownerWrite());
    try testing.expect(!m.permissions().ownerExecute());
}

test "fs.exists: real path returns true; missing returns false" {
    if (is_windows) return error.SkipZigTest;
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();
    const fp = try tmp.touchFile("here.txt");
    defer tmp.rm(fp);

    try testing.expect(exists(fp));

    var miss_buf: [256]u8 = undefined;
    const miss = try std.fmt.bufPrint(&miss_buf, "{s}/missing.txt", .{tmp.path});
    try testing.expect(!exists(miss));
}

test "fs.access: write check on read-only file returns false" {
    if (is_windows) return error.SkipZigTest;
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();
    const fp = try tmp.touchFile("ro.txt");
    defer {
        // Restore so unlink can succeed.
        _ = syscall.chmod(fp.ptr, 0o644);
        tmp.rm(fp);
    }

    try chmod(fp, Permissions.fromOctal(0o444));
    try testing.expect(access(fp, .{ .read = true }));
    try testing.expect(!access(fp, .{ .write = true }));
}

test "fs.canonicalize: resolves '..' segment" {
    if (is_windows) return error.SkipZigTest;
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();
    const subdir = try tmp.mkSubdir("sub");
    defer tmp.rmdir(subdir);
    const fp = try tmp.writeFile("sub/c.txt", "");
    defer tmp.rm(fp);

    var dotted_buf: [512]u8 = undefined;
    const dotted = try std.fmt.bufPrint(
        &dotted_buf,
        "{s}/sub/../sub/c.txt",
        .{tmp.path},
    );

    const resolved = try canonicalize(testing.allocator, dotted);
    defer testing.allocator.free(resolved);

    // realpath also resolves /tmp itself (e.g. /tmp -> /private/tmp on
    // Darwin), so canonicalize the expected too.
    const expected = try canonicalize(testing.allocator, fp);
    defer testing.allocator.free(expected);
    try testing.expectEqualStrings(expected, resolved);
}

test "fs.setTimes: round-trip preserves mtime" {
    if (is_windows) return error.SkipZigTest;
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();
    const fp = try tmp.touchFile("times.txt");
    defer tmp.rm(fp);

    const target_atime = SystemTime.fromSecs(1_700_000_000);
    const target_mtime = SystemTime.fromSecs(1_700_000_001);
    try setTimes(fp, target_atime, target_mtime);

    const m = try stat(fp);
    try testing.expectEqual(@as(i64, 1_700_000_001), m.modified().secs);
}

test "fs.lstat: symlink reports itself, not target" {
    if (is_windows) return error.SkipZigTest;
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();
    const target = try tmp.writeFile("real.txt", "hello");
    defer tmp.rm(target);

    const link = try std.fmt.allocPrintSentinel(testing.allocator, "{s}/link.txt", .{tmp.path}, 0);
    defer {
        _ = syscall.c_unlink(link.ptr);
        testing.allocator.free(link);
    }
    if (syscall.c_symlink(target.ptr, link.ptr) != 0) return error.SymlinkFailed;

    const via_stat = try stat(link); // follows
    const via_lstat = try lstat(link); // does not
    try testing.expect(via_stat.isFile());
    try testing.expect(via_lstat.isSymlink());
}
