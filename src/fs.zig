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
const win32 = @import("fs/win32.zig");
const file_mod = @import("fs/file.zig");
const metadata_mod = @import("fs/metadata.zig");

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
    if (is_windows) return winStatPath(file_path, true);
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    var buf: PlatformStat = undefined;
    if (syscall.stat(&z.buf, &buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    return Metadata.fromStat(buf);
}

/// Like `stat` but does not follow a terminal symlink — returns
/// info about the symlink itself.
pub fn lstat(file_path: []const u8) FsError!Metadata {
    if (is_windows) return winStatPath(file_path, false);
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    var buf: PlatformStat = undefined;
    if (syscall.lstat(&z.buf, &buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
    return Metadata.fromStat(buf);
}

/// Stat by file descriptor. The handle must be currently open;
/// works for any fd that's bound to a filesystem object (regular
/// file, directory, socket).
pub fn fstat(fd: file_mod.Handle) FsError!Metadata {
    // Delegate to File.metadata so both platforms share one path
    // (fstat on POSIX, GetFileInformationByHandle on Windows). The
    // handle is not consumed/closed.
    var f = File.fromFd(fd);
    return f.metadata();
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
    if (is_windows) {
        const z = try win32.WPathZ.fromUtf8(file_path);
        if (win32.GetFileAttributesW(z.ptr()) != win32.INVALID_FILE_ATTRIBUTES) return true;
        const code = win32.GetLastError();
        if (code == win32.ERROR_FILE_NOT_FOUND or code == win32.ERROR_PATH_NOT_FOUND) return false;
        return win32.fromLastError(code);
    }
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
    if (is_windows) {
        // Windows has no access(2). Approximate: the file must exist,
        // and a write check fails on a read-only file. Read/execute
        // are not meaningfully gated at the attribute level.
        const z = win32.WPathZ.fromUtf8(file_path) catch return false;
        const attrs = win32.GetFileAttributesW(z.ptr());
        if (attrs == win32.INVALID_FILE_ATTRIBUTES) return false;
        if (mode.write and (attrs & win32.FILE_ATTRIBUTE_READONLY) != 0) return false;
        return true;
    }
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
    if (is_windows) {
        // Only the read-only bit maps to the Windows attribute model.
        const z = try win32.WPathZ.fromUtf8(file_path);
        const cur = win32.GetFileAttributesW(z.ptr());
        if (cur == win32.INVALID_FILE_ATTRIBUTES) return win32.fromLastError(win32.GetLastError());
        var attrs = cur;
        if (perms.readonly()) {
            attrs |= win32.FILE_ATTRIBUTE_READONLY;
        } else {
            attrs &= ~win32.FILE_ATTRIBUTE_READONLY;
        }
        if (attrs == 0) attrs = win32.FILE_ATTRIBUTE_NORMAL;
        if (win32.SetFileAttributesW(z.ptr(), attrs) == 0) return win32.fromLastError(win32.GetLastError());
        return;
    }
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    const mode: std.c.mode_t = @intCast(perms.getMode());
    if (syscall.chmod(&z.buf, mode) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// Change ownership of `path`. Most callers need to be root for
/// this to succeed; the typical use case is install scripts.
pub fn chown(file_path: []const u8, uid: u32, gid: u32) FsError!void {
    if (is_windows) {
        // Windows uses ACLs/SIDs, not POSIX uid/gid — no faithful
        // mapping. No-op (success) so cross-platform callers don't
        // have to special-case the platform. (Params are unused on
        // this path; Zig allows unused function parameters.)
        return;
    }
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
    if (is_windows) {
        const z = try win32.WPathZ.fromUtf8(file_path);
        const h = win32.CreateFileW(z.ptr(), win32.FILE_WRITE_ATTRIBUTES, win32.FILE_SHARE_ALL, null, win32.OPEN_EXISTING, win32.FILE_FLAG_BACKUP_SEMANTICS, null);
        if (h == win32.INVALID_HANDLE_VALUE) return win32.fromLastError(win32.GetLastError());
        defer _ = win32.CloseHandle(h);
        const at = win32.unixToFiletime(atime.secs, atime.nsecs);
        const mt = win32.unixToFiletime(mtime.secs, mtime.nsecs);
        if (win32.SetFileTime(h, null, &at, &mt) == 0) return win32.fromLastError(win32.GetLastError());
        return;
    }
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
    if (is_windows) {
        const z = try win32.WPathZ.fromUtf8(file_path);
        const h = win32.CreateFileW(z.ptr(), win32.FILE_READ_ATTRIBUTES, win32.FILE_SHARE_ALL, null, win32.OPEN_EXISTING, win32.FILE_FLAG_BACKUP_SEMANTICS, null);
        if (h == win32.INVALID_HANDLE_VALUE) return win32.fromLastError(win32.GetLastError());
        defer _ = win32.CloseHandle(h);
        var wbuf: [win32.WPATH_MAX]u16 = undefined;
        const n = win32.GetFinalPathNameByHandleW(h, &wbuf, win32.WPATH_MAX, 0);
        if (n == 0) return win32.fromLastError(win32.GetLastError());
        if (n >= win32.WPATH_MAX) return error.NameTooLong;
        var wslice = wbuf[0..n];
        // GetFinalPathNameByHandleW prepends the `\\?\` long-path
        // namespace prefix; strip it for a conventional path.
        const dos_prefix = [_]u16{ '\\', '\\', '?', '\\' };
        if (wslice.len >= 4 and std.mem.eql(u16, wslice[0..4], &dos_prefix)) wslice = wslice[4..];
        return win32Utf16ToUtf8Dupe(allocator, wslice);
    }
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
    if (is_windows) {
        const zo = try win32.WPathZ.fromUtf8(old_path);
        const zn = try win32.WPathZ.fromUtf8(new_path);
        // REPLACE_EXISTING matches POSIX rename's overwrite; COPY_ALLOWED
        // lets the kernel fall back to copy+delete across volumes.
        if (win32.MoveFileExW(zo.ptr(), zn.ptr(), win32.MOVEFILE_REPLACE_EXISTING | win32.MOVEFILE_COPY_ALLOWED) == 0) {
            return win32.fromLastError(win32.GetLastError());
        }
        return;
    }
    var z_old: PathZ = undefined;
    try pathZInto(old_path, &z_old);
    var z_new: PathZ = undefined;
    try pathZInto(new_path, &z_new);
    if (c_rename(&z_old.buf, &z_new.buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// Make a hard link from `link_path` to `target`. Both must live
/// on the same filesystem (POSIX hard-link constraint).
pub fn hardLink(target: []const u8, link_path: []const u8) FsError!void {
    if (is_windows) {
        const zt = try win32.WPathZ.fromUtf8(target);
        const zl = try win32.WPathZ.fromUtf8(link_path);
        if (win32.CreateHardLinkW(zl.ptr(), zt.ptr(), null) == 0) {
            return win32.fromLastError(win32.GetLastError());
        }
        return;
    }
    var z_t: PathZ = undefined;
    try pathZInto(target, &z_t);
    var z_l: PathZ = undefined;
    try pathZInto(link_path, &z_l);
    if (c_link(&z_t.buf, &z_l.buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// Create a symbolic link `link_path` pointing at `target`. Unlike
/// `hardLink`, `target` need not exist when the link is created.
pub fn symlink(target: []const u8, link_path: []const u8) FsError!void {
    if (is_windows) {
        const zt = try win32.WPathZ.fromUtf8(target);
        const zl = try win32.WPathZ.fromUtf8(link_path);
        var flags: win32.DWORD = win32.SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE;
        // A symlink to a directory needs the DIRECTORY flag. Probe the
        // target's attributes; if it doesn't exist yet, assume a file
        // link (matches the common case).
        const tattr = win32.GetFileAttributesW(zt.ptr());
        if (tattr != win32.INVALID_FILE_ATTRIBUTES and (tattr & win32.FILE_ATTRIBUTE_DIRECTORY) != 0) {
            flags |= win32.SYMBOLIC_LINK_FLAG_DIRECTORY;
        }
        if (win32.CreateSymbolicLinkW(zl.ptr(), zt.ptr(), flags) == 0) {
            return win32.fromLastError(win32.GetLastError());
        }
        return;
    }
    var z_t: PathZ = undefined;
    try pathZInto(target, &z_t);
    var z_l: PathZ = undefined;
    try pathZInto(link_path, &z_l);
    if (syscall.c_symlink(&z_t.buf, &z_l.buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// Read the target of a symbolic link. Returns owned slice.
pub fn readLink(allocator: std.mem.Allocator, link_path: []const u8) (FsError || error{OutOfMemory})![]u8 {
    if (is_windows) return winReadLink(allocator, link_path);
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
    if (is_windows) {
        const z = try win32.WPathZ.fromUtf8(file_path);
        if (win32.DeleteFileW(z.ptr()) != 0) return;
        const code = win32.GetLastError();
        // POSIX unlink removes read-only files; Windows DeleteFileW
        // refuses them. Emulate by clearing the bit and retrying once.
        if (code == win32.ERROR_ACCESS_DENIED) {
            const attrs = win32.GetFileAttributesW(z.ptr());
            if (attrs != win32.INVALID_FILE_ATTRIBUTES and (attrs & win32.FILE_ATTRIBUTE_READONLY) != 0) {
                _ = win32.SetFileAttributesW(z.ptr(), attrs & ~win32.FILE_ATTRIBUTE_READONLY);
                if (win32.DeleteFileW(z.ptr()) != 0) return;
            }
        }
        return win32.fromLastError(code);
    }
    var z: PathZ = undefined;
    try pathZInto(file_path, &z);
    if (syscall.c_unlink(&z.buf) != 0) return fs_error.fromErrno(fs_error.currentErrno());
}

/// The system temp directory (`TMPDIR` env var on POSIX with a
/// `/tmp` fallback). Returned slice is owned by `allocator`.
pub fn tempDir(allocator: std.mem.Allocator) error{OutOfMemory}![]u8 {
    if (is_windows) {
        var wbuf: [win32.WPATH_MAX]u16 = undefined;
        const n = win32.GetTempPathW(win32.WPATH_MAX, &wbuf);
        if (n == 0 or n >= win32.WPATH_MAX) return try allocator.dupe(u8, "C:\\Windows\\Temp");
        var u8buf: [win32.WPATH_MAX * 3]u8 = undefined;
        const u8len = std.unicode.utf16LeToUtf8(&u8buf, wbuf[0..n]) catch
            return try allocator.dupe(u8, "C:\\Windows\\Temp");
        // GetTempPathW returns a trailing backslash; trim for parity
        // with the POSIX (no-trailing-slash) result.
        const trimmed = std.mem.trimEnd(u8, u8buf[0..u8len], "\\");
        return try allocator.dupe(u8, if (trimmed.len > 0) trimmed else "C:\\Windows\\Temp");
    }
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
    if (is_windows) {
        const dir = try tempDir(allocator);
        defer allocator.free(dir);
        const wdir = try win32.WPathZ.fromUtf8(dir);
        // GetTempFileNameW uses only the first 3 chars of the prefix.
        const wprefix = try win32.WPathZ.fromUtf8(prefix);
        var wname: [win32.WPATH_MAX]u16 = undefined;
        // uUnique = 0 ⇒ the API generates a unique name AND creates
        // the (empty) file atomically.
        if (win32.GetTempFileNameW(wdir.ptr(), wprefix.ptr(), 0, &wname) == 0) {
            return win32.fromLastError(win32.GetLastError());
        }
        const wlen = std.mem.indexOfScalar(u16, &wname, 0) orelse wname.len;
        const path_owned = try win32Utf16ToUtf8Dupe(allocator, wname[0..wlen]);
        errdefer allocator.free(path_owned);
        // Re-open the created file with read+write for the caller.
        const f = try File.openOptions(path_owned, .{ .read = true, .write = true });
        return .{ .path = path_owned, .file = f };
    }
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

// ─── Windows path-op helpers ─────────────────────────────────────
// Only referenced from `is_windows` comptime branches → never
// analysed on POSIX.

/// Open an attribute-only handle and stat it. `follow = false` opens
/// the reparse point itself (the lstat analog).
fn winStatPath(file_path: []const u8, follow: bool) FsError!Metadata {
    const z = try win32.WPathZ.fromUtf8(file_path);
    var flags = win32.FILE_FLAG_BACKUP_SEMANTICS;
    if (!follow) flags |= win32.FILE_FLAG_OPEN_REPARSE_POINT;
    const h = win32.CreateFileW(z.ptr(), win32.FILE_READ_ATTRIBUTES, win32.FILE_SHARE_ALL, null, win32.OPEN_EXISTING, flags, null);
    if (h == win32.INVALID_HANDLE_VALUE) return win32.fromLastError(win32.GetLastError());
    defer _ = win32.CloseHandle(h);
    var info: win32.BY_HANDLE_FILE_INFORMATION = undefined;
    if (win32.GetFileInformationByHandle(h, &info) == 0) return win32.fromLastError(win32.GetLastError());
    return metadata_mod.metadataFromWindowsInfo(info);
}

fn win32Utf16ToUtf8Dupe(allocator: std.mem.Allocator, utf16: []const u16) (FsError || error{OutOfMemory})![]u8 {
    var u8buf: [win32.WPATH_MAX * 3]u8 = undefined;
    const n = std.unicode.utf16LeToUtf8(&u8buf, utf16) catch return error.InvalidPath;
    return allocator.dupe(u8, u8buf[0..n]);
}

/// Read a symlink/junction target via FSCTL_GET_REPARSE_POINT, parsing
/// the REPARSE_DATA_BUFFER and returning the (user-facing) print name.
fn winReadLink(allocator: std.mem.Allocator, link_path: []const u8) (FsError || error{OutOfMemory})![]u8 {
    const z = try win32.WPathZ.fromUtf8(link_path);
    const h = win32.CreateFileW(z.ptr(), win32.FILE_READ_ATTRIBUTES, win32.FILE_SHARE_ALL, null, win32.OPEN_EXISTING, win32.FILE_FLAG_BACKUP_SEMANTICS | win32.FILE_FLAG_OPEN_REPARSE_POINT, null);
    if (h == win32.INVALID_HANDLE_VALUE) return win32.fromLastError(win32.GetLastError());
    defer _ = win32.CloseHandle(h);

    var buf: [win32.MAXIMUM_REPARSE_DATA_BUFFER_SIZE]u8 align(8) = undefined;
    var returned: win32.DWORD = 0;
    if (win32.DeviceIoControl(h, win32.FSCTL_GET_REPARSE_POINT, null, 0, &buf, buf.len, &returned, null) == 0) {
        return win32.fromLastError(win32.GetLastError());
    }

    // REPARSE_DATA_BUFFER: ReparseTag(u32) ReparseDataLength(u16)
    // Reserved(u16), then the per-tag union at offset 8. The symlink
    // union has an extra Flags(u32) before PathBuffer; the mount-point
    // union does not.
    const tag = std.mem.readInt(u32, buf[0..4], .little);
    const print_off = std.mem.readInt(u16, buf[12..14], .little);
    const print_len = std.mem.readInt(u16, buf[14..16], .little);
    const path_buffer_at: usize = switch (tag) {
        win32.IO_REPARSE_TAG_SYMLINK => 20,
        win32.IO_REPARSE_TAG_MOUNT_POINT => 16,
        else => return error.NotSupported,
    };

    const start = path_buffer_at + print_off;
    const wcount = print_len / 2;
    if (start + print_len > buf.len) return error.Unexpected;

    // Copy out as aligned u16s (the byte slice may be 2-aligned only).
    var wide: [win32.WPATH_MAX]u16 = undefined;
    if (wcount > wide.len) return error.NameTooLong;
    var i: usize = 0;
    while (i < wcount) : (i += 1) {
        wide[i] = std.mem.readInt(u16, buf[start + i * 2 ..][0..2], .little);
    }
    return win32Utf16ToUtf8Dupe(allocator, wide[0..wcount]);
}

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
const tu = @import("testing.zig");

/// Cross-platform scoped temp dir for fs tests. Thin wrapper over
/// `volt.testing.TempDir` that keeps the method names the existing
/// tests use, built on volt's own (cross-platform) fs API so the
/// same tests run on POSIX and Windows. Bare `writeFile` / `makeDir`
/// inside the methods resolve to the file-scope fs functions, not the
/// struct methods (Zig members aren't ambient identifiers).
const TmpDir = struct {
    inner: tu.TempDir,
    path: []const u8,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) !TmpDir {
        const inner = try tu.TempDir.create(allocator);
        return .{ .inner = inner, .path = inner.path, .allocator = allocator };
    }

    fn deinit(self: *TmpDir) void {
        self.inner.deinit();
    }

    /// Create an empty file `path/sub`; returns an owned NUL-term path.
    fn touchFile(self: *TmpDir, sub: []const u8) ![:0]u8 {
        const full = try std.fmt.allocPrintSentinel(self.allocator, "{s}/{s}", .{ self.path, sub }, 0);
        errdefer self.allocator.free(full);
        try @import("fs.zig").writeFile(full, "");
        return full;
    }

    fn writeFile(self: *TmpDir, sub: []const u8, data: []const u8) ![:0]u8 {
        const full = try std.fmt.allocPrintSentinel(self.allocator, "{s}/{s}", .{ self.path, sub }, 0);
        errdefer self.allocator.free(full);
        try @import("fs.zig").writeFile(full, data);
        return full;
    }

    fn mkSubdir(self: *TmpDir, sub: []const u8) ![:0]u8 {
        const full = try std.fmt.allocPrintSentinel(self.allocator, "{s}/{s}", .{ self.path, sub }, 0);
        errdefer self.allocator.free(full);
        try makeDir(full, .{});
        return full;
    }

    fn rm(self: *TmpDir, p: [:0]u8) void {
        unlink(p) catch {};
        self.allocator.free(p);
    }

    fn rmdir(self: *TmpDir, p: [:0]u8) void {
        removeDir(self.allocator, p, .{}) catch {};
        self.allocator.free(p);
    }
};

test "fs.stat: round-trip on temp file" {
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
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();
    const fp = try tmp.touchFile("ro.txt");
    defer {
        // Restore writability so cleanup can remove it.
        chmod(fp, Permissions.fromOctal(0o644)) catch {};
        tmp.rm(fp);
    }

    try chmod(fp, Permissions.fromOctal(0o444));
    try testing.expect(access(fp, .{ .read = true }));
    try testing.expect(!access(fp, .{ .write = true }));
}

test "fs.canonicalize: resolves '..' segment" {
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
    tmp: []const u8,
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
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();

    var rt = try RT.Runtime.init(.{ .allocator = @import("testing.zig").allocator });
    defer rt.deinit();
    var state = FacadeState{ .tmp = tmp.path };
    try (try rt.run(facadeReadWriteCopy, .{&state}));
    try testing.expect(state.ok);
}

test "fs.tempDir: returns an existing directory" {
    const t = try tempDir(testing.allocator);
    defer testing.allocator.free(t);
    try testing.expect(exists(t));
}

test "fs.symlink + readLink: round-trip the target" {
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();

    const target = try tmp.writeFile("target.txt", "");
    defer tmp.rm(target);

    const link = try tmp.inner.childPath(testing.allocator, "link.txt");
    defer {
        unlink(link) catch {};
        testing.allocator.free(link);
    }
    symlink(target, link) catch |e| {
        // Windows requires Developer Mode / elevation to create
        // symlinks; skip where the privilege isn't held.
        if (e == error.AccessDenied) return error.SkipZigTest;
        return e;
    };

    const got = try readLink(testing.allocator, link);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(target, got);
}

test "fs.rename: moves a file under the same dir" {
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();

    const src = try tmp.writeFile("a.txt", "X");
    defer tmp.rm(src); // renamed away; rm's unlink is a no-op, frees the path

    const dst = try tmp.inner.childPath(testing.allocator, "b.txt");
    defer {
        unlink(dst) catch {};
        testing.allocator.free(dst);
    }
    try rename(src, dst);

    try testing.expect(!exists(src));
    try testing.expect(exists(dst));
}

test "fs.lstat: symlink reports itself, not target" {
    var tmp = try TmpDir.init(testing.allocator);
    defer tmp.deinit();
    const target = try tmp.writeFile("real.txt", "hello");
    defer tmp.rm(target);

    const link = try tmp.inner.childPath(testing.allocator, "link.txt");
    defer {
        unlink(link) catch {};
        testing.allocator.free(link);
    }
    symlink(target, link) catch |e| {
        if (e == error.AccessDenied) return error.SkipZigTest;
        return e;
    };

    const via_stat = try stat(link); // follows
    const via_lstat = try lstat(link); // does not
    try testing.expect(via_stat.isFile());
    try testing.expect(via_lstat.isSymlink());
}
