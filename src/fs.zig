//! Filesystem namespace facade — `volt.fs.{path, File, Dir, ...}`.
//!
//! Volt's fs surface targets Node.js / Go-scale exhaustiveness with
//! a stackful-coroutine ergonomic. Exposes:
//!
//! * **path** (`volt.fs.path`) — pure-string path utilities
//! * **Metadata vocabulary** — Metadata, Permissions, SystemTime,
//!   FileType, plus stat / lstat / fstat / exists / access / chmod /
//!   chown / setTimes / canonicalize
//! * **File** — async-by-default file handle with std.Io.Reader /
//!   Writer adapters, cancel-aware variants
//! * **Dir** — opendir-style iterator + create / remove (with
//!   recursive variants) + walk + glob
//! * **MappedFile** — mmap / madvise / mlock / mprotect / msync
//! * **Watcher** — polling-based file-change watcher (native inotify
//!   / FSEvents / RDC backends land later)
//! * **Convenience facade** — readFile / writeFile / appendFile /
//!   copyFile / rename / hardLink / symlink / readLink / unlink /
//!   tempDir / tempFile

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
pub const DirCreateOptions = @import("fs/dir.zig").CreateOptions;
pub const DirRemoveOptions = @import("fs/dir.zig").RemoveOptions;
pub const makeDir = @import("fs/dir.zig").create;
pub const removeDir = @import("fs/dir.zig").remove;
pub const dirEntries = @import("fs/dir.zig").entries;
pub const freeDirEntries = @import("fs/dir.zig").freeEntries;
pub const glob = @import("fs/dir.zig").glob;

pub const MappedFile = @import("fs/mmap.zig").MappedFile;
pub const MmapProtection = @import("fs/mmap.zig").Protection;
pub const MmapSharing = @import("fs/mmap.zig").Sharing;
pub const MmapOptions = @import("fs/mmap.zig").MapOptions;
pub const MmapAnonOptions = @import("fs/mmap.zig").AnonOptions;
pub const MmapAdvice = @import("fs/mmap.zig").Advice;
pub const mapFile = @import("fs/mmap.zig").mapFile;
pub const mapAnonymous = @import("fs/mmap.zig").mapAnonymous;

pub const Watcher = @import("fs/watcher.zig").Watcher;
pub const WatchEvent = @import("fs/watcher.zig").Event;
pub const WatchEventKind = @import("fs/watcher.zig").EventKind;
pub const WatcherOptions = @import("fs/watcher.zig").Options;
pub const WatcherWatchOptions = @import("fs/watcher.zig").WatchOptions;

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
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    var buf: PlatformStat = undefined;
    if (syscall.stat(&z.buf, &buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    return Metadata.fromStat(buf);
}

/// Like `stat` but does not follow a terminal symlink — returns
/// info about the symlink itself.
pub fn lstat(file_path: []const u8) FsError!Metadata {
    if (is_windows) @compileError("Windows lstat: pending (Phase B.2)");
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    var buf: PlatformStat = undefined;
    if (syscall.lstat(&z.buf, &buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    return Metadata.fromStat(buf);
}

/// Stat by file descriptor. The handle must be currently open;
/// works for any fd that's bound to a filesystem object (regular
/// file, directory, socket).
pub fn fstat(fd: c_int) FsError!Metadata {
    if (is_windows) @compileError("Windows fstat: pending (Phase B.2)");
    var buf: PlatformStat = undefined;
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
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    var buf: PlatformStat = undefined;
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
    var z: PathZ = undefined;
    pathZInto(file_path, &z) catch return false;
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
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    const mode: std.c.mode_t = @intCast(perms.getMode());
    if (syscall.chmod(&z.buf, mode) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// Change ownership of `path`. Most callers need to be root for
/// this to succeed; the typical use case is install scripts.
pub fn chown(file_path: []const u8, uid: u32, gid: u32) FsError!void {
    if (is_windows) @compileError("Windows chown: pending (Phase B.2)");
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    if (syscall.chown(&z.buf, @intCast(uid), @intCast(gid)) != 0) {
        return fs_error.fromErrno(fs_error.currentErrno());
    }
}

/// Set access + modification times on `path`. Nanosecond-precision
/// via `utimensat`; the kernel rounds to the filesystem's
/// resolution (commonly 1µs on ext4, 1s on FAT).
pub fn setTimes(file_path: []const u8, atime: SystemTime, mtime: SystemTime) FsError!void {
    if (is_windows) @compileError("Windows setTimes: pending (Phase B.2)");
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
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
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    // realpath wants a PATH_MAX buffer. Stack-allocate, then dup
    // into the caller's allocator.
    var buf: [syscall.PATH_MAX]u8 = undefined;
    const result = syscall.realpath(&z.buf, &buf);
    if (result == null) return fs_error.fromErrno(fs_error.currentErrno());
    const len = std.mem.len(result.?);
    return try allocator.dupe(u8, result.?[0..len]);
}

// ─── Convenience: read/write/copy/rename/link/temp ───────────────

/// Read the whole file into a freshly-allocated buffer. Caller
/// frees with the same allocator. Bridges through `spawnBlocking`
/// per File's normal behaviour.
pub fn readFile(allocator: std.mem.Allocator, file_path: []const u8) (FileError || error{OutOfMemory})![]u8 {
    var f = try File.open(file_path);
    defer f.close();
    const m = try f.metadata();
    const size: usize = @intCast(m.size());
    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);
    const n = try f.readFull(buf);
    if (n != size) return allocator.realloc(buf, n);
    return buf;
}

/// Like `readFile` but validates UTF-8 — useful when the file is
/// supposed to be text and you don't want to silently accept binary.
pub fn readFileString(allocator: std.mem.Allocator, file_path: []const u8) (FileError || error{ OutOfMemory, InvalidUtf8 })![]u8 {
    const bytes = try readFile(allocator, file_path);
    errdefer allocator.free(bytes);
    if (!std.unicode.utf8ValidateSlice(bytes)) return error.InvalidUtf8;
    return bytes;
}

/// Create-or-truncate `file_path` and write `data`. Atomic from the
/// reader's perspective only with respect to write boundaries — for
/// truly atomic write (no partial-content visibility), write to a
/// sibling temp file + rename.
pub fn writeFile(file_path: []const u8, data: []const u8) FileError!void {
    var f = try File.create(file_path);
    defer f.close();
    try f.writeAll(data);
}

/// Append `data` to `file_path`. Creates the file if missing.
pub fn appendFile(file_path: []const u8, data: []const u8) FileError!void {
    var f = try File.openOptions(file_path, .{ .write = true, .create = true, .append = true });
    defer f.close();
    try f.writeAll(data);
}

/// Copy `src` to `dst`, byte-for-byte. Streams in 4 KiB chunks
/// (keeps the function's stack frame coroutine-friendly). `dst`'s
/// mode mirrors `src`'s.
///
/// **DO NOT INLINE THIS DELEGATION.** copyFileImpl has a 4 KiB
/// `chunk` local that crashes (SIGILL) when the Zig 0.16 inliner
/// pulls the body into a coroutine root frame on macOS arm64. The
/// thin wrapper forces a real function call, isolating the frame.
/// Tracked in GitHub issue (see ISSUES.md). If you delete the
/// wrapper "for cleanliness," the fs facade tests crash.
pub fn copyFile(src: []const u8, dst: []const u8) FileError!void {
    return copyFileImpl(src, dst);
}

fn copyFileImpl(src: []const u8, dst: []const u8) FileError!void {
    var sf = try File.open(src);
    defer sf.close();
    const src_meta = try sf.metadata();

    var df = try File.openOptions(dst, .{
        .write = true,
        .create = true,
        .truncate = true,
        .mode = src_meta.permissions().getMode(),
    });
    defer df.close();

    var chunk: [4096]u8 = undefined;
    while (true) {
        const n = try sf.read(&chunk);
        if (n == 0) break;
        try df.writeAll(chunk[0..n]);
    }
}

/// Rename or move `old_path` to `new_path`. Atomic if both live on
/// the same filesystem; cross-device renames surface
/// `FsError.CrossDevice` (caller can fall back to copy + delete).
pub fn rename(old_path: []const u8, new_path: []const u8) FsError!void {
    if (is_windows) @compileError("Windows rename: pending");
    var z_old: PathZ = undefined;
    try pathZInto(old_path, &z_old);
    var z_new: PathZ = undefined;
    try pathZInto(new_path, &z_new);
    if (c_rename(&z_old.buf, &z_new.buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// Make a hard link from `link_path` to `target`. Both must live
/// on the same filesystem (POSIX hard-link constraint).
pub fn hardLink(target: []const u8, link_path: []const u8) FsError!void {
    if (is_windows) @compileError("Windows hardlink: pending");
    var z_t: PathZ = undefined;
    try pathZInto(target, &z_t);
    var z_l: PathZ = undefined;
    try pathZInto(link_path, &z_l);
    if (c_link(&z_t.buf, &z_l.buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// Create a symbolic link `link_path` pointing at `target`. Unlike
/// `hardLink`, `target` need not exist when the link is created.
pub fn symlink(target: []const u8, link_path: []const u8) FsError!void {
    if (is_windows) @compileError("Windows symlink: pending");
    var z_t: PathZ = undefined;
    try pathZInto(target, &z_t);
    var z_l: PathZ = undefined;
    try pathZInto(link_path, &z_l);
    if (syscall.c_symlink(&z_t.buf, &z_l.buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// Read the target of a symbolic link. Returns owned slice.
pub fn readLink(allocator: std.mem.Allocator, link_path: []const u8) (FsError || error{OutOfMemory})![]u8 {
    if (is_windows) @compileError("Windows readlink: pending");
    var z: PathZ = undefined;
    try pathZInto(link_path, &z);
    var buf: [syscall.PATH_MAX]u8 = undefined;
    const n = syscall.c_readlink(&z.buf, &buf, buf.len);
    if (n < 0) return fs_error.fromErrno(fs_error.currentErrno());
    return try allocator.dupe(u8, buf[0..@intCast(n)]);
}

/// Unlink a file (regular or symbolic link). For directories, use
/// `Dir.remove`.
pub fn unlink(file_path: []const u8) FsError!void {
    if (is_windows) @compileError("Windows unlink: pending");
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    if (syscall.c_unlink(&z.buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// The system temp directory (`TMPDIR` env var on POSIX with a
/// `/tmp` fallback). Returned slice is owned by `allocator`.
pub fn tempDir(allocator: std.mem.Allocator) error{OutOfMemory}![]u8 {
    if (is_windows) @compileError("Windows TEMP: pending");
    if (std.c.getenv("TMPDIR")) |t| {
        const len = std.mem.len(t);
        const slice = t[0..len];
        const trimmed = std.mem.trimEnd(u8, slice, "/");
        return try allocator.dupe(u8, if (trimmed.len > 0) trimmed else "/tmp");
    }
    return try allocator.dupe(u8, "/tmp");
}

/// Create a uniquely-named temp file. Returns the open file +
/// the allocated path; caller closes the file + frees the path.
/// Caller should also `unlink` the path when done (this fn doesn't
/// auto-clean — explicit lifetimes only).
pub fn tempFile(allocator: std.mem.Allocator, prefix: []const u8) (FileError || error{OutOfMemory})!struct { path: []u8, file: File } {
    if (is_windows) @compileError("Windows tempFile: pending");
    const dir = try tempDir(allocator);
    defer allocator.free(dir);

    const template = try std.fmt.allocPrintSentinel(allocator, "{s}/{s}-XXXXXX", .{ dir, prefix }, 0);
    errdefer allocator.free(template);
    const fd = syscall.c_mkstemp(template.ptr);
    if (fd < 0) return fs_error.fromErrno(fs_error.currentErrno());

    // Convert from [:0]u8 back to a regular []u8 so the caller
    // doesn't have to deal with sentinel-slice typing.
    const path_owned = try allocator.dupe(u8, template);
    allocator.free(template);
    return .{ .path = path_owned, .file = .{ .fd = fd, .append = false } };
}

const c_rename = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8, [*:0]const u8) callconv(.c) c_int,
    .{ .name = "rename" },
);
const c_link = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8, [*:0]const u8) callconv(.c) c_int,
    .{ .name = "link" },
);

// ─── Internal: NUL-terminate a path on the stack ─────────────────

const PathZ = struct {
    buf: [syscall.PATH_MAX:0]u8,
};

/// Fill an out-buffer with `p` followed by a NUL sentinel. We
/// don't return PathZ by value — that copies 4 KiB through the
/// caller's stack and crashes coroutine frames where the size
/// stresses Zig's return-value-by-hidden-pointer codegen on macOS
/// arm64.
fn pathZInto(p: []const u8, out: *PathZ) FsError!void {
    if (p.len >= syscall.PATH_MAX) return error.NameTooLong;
    if (std.mem.indexOfScalar(u8, p, 0) != null) return error.InvalidPath;
    @memcpy(out.buf[0..p.len], p);
    out.buf[p.len] = 0;
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
    _ = @import("fs/watcher.zig");
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

const RT = @import("lib.zig");

const FacadeState = struct {
    tmp: [:0]const u8,
    ok: bool = false,
};

fn facadeReadWriteCopy(state: *FacadeState) !void {
    const allocator = @import("testing.zig").allocator;
    var path_buf: [256:0]u8 = undefined;
    const src = try std.fmt.bufPrintZ(&path_buf, "{s}/src.txt", .{state.tmp});

    try writeFile(src, "facade payload");

    const content = try readFile(allocator, src);
    defer allocator.free(content);
    if (!std.mem.eql(u8, content, "facade payload")) return error.WrongRead;

    var dst_buf: [256:0]u8 = undefined;
    const dst = try std.fmt.bufPrintZ(&dst_buf, "{s}/dst.txt", .{state.tmp});
    try copyFile(src, dst);

    const dst_content = try readFile(allocator, dst);
    defer allocator.free(dst_content);
    if (!std.mem.eql(u8, dst_content, "facade payload")) return error.WrongCopy;

    try appendFile(dst, " + more");
    const final = try readFile(allocator, dst);
    defer allocator.free(final);
    if (!std.mem.eql(u8, final, "facade payload + more")) return error.WrongAppend;

    state.ok = true;
}

test "fs facade: readFile / writeFile / copyFile / appendFile round-trip" {
    if (is_windows) return error.SkipZigTest;
    var tmpl = "/tmp/volt-facade-XXXXXX\x00".*;
    if (syscall.c_mkdtemp(@ptrCast(&tmpl)) == null) return error.MkdtempFailed;
    const tmp: [:0]const u8 = tmpl[0 .. tmpl.len - 1 :0];
    defer {
        for ([_][]const u8{ "src.txt", "dst.txt" }) |name| {
            var p: [256:0]u8 = undefined;
            const z = std.fmt.bufPrintZ(&p, "{s}/{s}", .{ tmp, name }) catch continue;
            _ = syscall.c_unlink(z.ptr);
        }
        _ = syscall.c_rmdir(tmp.ptr);
    }

    var rt = try RT.Runtime.init(.{ .allocator = @import("testing.zig").allocator });
    defer rt.deinit();
    var state = FacadeState{ .tmp = tmp };
    try (try rt.run(facadeReadWriteCopy, .{&state}));
    try testing.expect(state.ok);
}

test "fs.tempDir: returns an existing directory" {
    if (is_windows) return error.SkipZigTest;
    const t = try tempDir(testing.allocator);
    defer testing.allocator.free(t);
    try testing.expect(exists(t));
}

test "fs.symlink + readLink: round-trip the target" {
    if (is_windows) return error.SkipZigTest;
    var tmpl = "/tmp/volt-facade-XXXXXX\x00".*;
    if (syscall.c_mkdtemp(@ptrCast(&tmpl)) == null) return error.MkdtempFailed;
    const tmp: [:0]const u8 = tmpl[0 .. tmpl.len - 1 :0];
    defer _ = syscall.c_rmdir(tmp.ptr);

    var target_buf: [256:0]u8 = undefined;
    const target = try std.fmt.bufPrintZ(&target_buf, "{s}/target.txt", .{tmp});
    try writeFile(target, "");
    defer _ = syscall.c_unlink(target.ptr);

    var link_buf: [256:0]u8 = undefined;
    const link = try std.fmt.bufPrintZ(&link_buf, "{s}/link.txt", .{tmp});
    try symlink(target, link);
    defer _ = syscall.c_unlink(link.ptr);

    const got = try readLink(testing.allocator, link);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(target, got);
}

test "fs.rename: moves a file under the same dir" {
    if (is_windows) return error.SkipZigTest;
    var tmpl = "/tmp/volt-facade-XXXXXX\x00".*;
    if (syscall.c_mkdtemp(@ptrCast(&tmpl)) == null) return error.MkdtempFailed;
    const tmp: [:0]const u8 = tmpl[0 .. tmpl.len - 1 :0];
    defer _ = syscall.c_rmdir(tmp.ptr);

    var src_buf: [256:0]u8 = undefined;
    const src = try std.fmt.bufPrintZ(&src_buf, "{s}/a.txt", .{tmp});
    try writeFile(src, "X");

    var dst_buf: [256:0]u8 = undefined;
    const dst = try std.fmt.bufPrintZ(&dst_buf, "{s}/b.txt", .{tmp});
    try rename(src, dst);
    defer _ = syscall.c_unlink(dst.ptr);

    try testing.expect(!exists(src));
    try testing.expect(exists(dst));
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
