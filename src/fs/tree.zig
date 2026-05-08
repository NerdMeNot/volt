//! `volt.fs.tree` — tree operations: mkdirAll, removeTree, rename,
//! symlink, link, copy.
//!
//! All operations route through the blocking pool. None are atomic
//! across multiple syscalls — `removeTree` walks the tree, `mkdirAll`
//! creates ancestors one at a time. Concurrent users of the same
//! tree can observe partial states.
//!
//! ## Copy
//!
//! `copy(src, dst)` opens both files and loops `read` + `write`.
//! Kernel accelerators (`copy_file_range` on Linux, `clonefile` /
//! `fcopyfile` on Darwin) are a v1.2 follow-up — the wiring needs
//! the corresponding syscall wrappers in `internal/syscall.zig`,
//! and the accelerators need careful fallback handling for cross-
//! filesystem and unsupported-FS cases. The portable read+write
//! fallback ships now and is correct on every platform.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const c = std.c;

const syscall = @import("../internal/syscall.zig");
const spawnBlocking = @import("../api/spawn_blocking.zig").spawnBlocking;
const File = @import("File.zig").File;
const OpenOptionsT = @import("OpenOptions.zig").OpenOptions;
const Walker = @import("Walker.zig").Walker;
const Kind = @import("Metadata.zig").Kind;
const Metadata = @import("Metadata.zig").Metadata;
const Instant = @import("../time.zig").Instant;

// AT_SYMLINK_NOFOLLOW differs per platform: Linux 0x100, Darwin 0x0020.
const AT_SYMLINK_NOFOLLOW: u32 = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .visionos, .driverkit, .freebsd, .netbsd, .openbsd, .dragonfly => 0x0020,
    else => 0x100,
};

// ─────────────────────────────────────────────────────────────────────
// mkdir / mkdirAll / remove
// ─────────────────────────────────────────────────────────────────────

/// Create a directory at `path` with the given mode. Fails if the
/// path already exists.
pub fn makeDir(path: []const u8, mode: posix.mode_t) !void {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    const Args = struct { path_z: [:0]const u8, mode: posix.mode_t };
    var args = Args{ .path_z = path_z, .mode = mode };
    return spawnBlocking(struct {
        fn run(a: *Args) !void {
            return syscall.mkdiratZ(posix.AT.FDCWD, a.path_z.ptr, a.mode);
        }
    }.run, .{&args});
}

/// Create `path` and any missing intermediate directories. Existing
/// directories along the chain are tolerated; existing files at any
/// level surface as an error.
pub fn makeDirAll(path: []const u8, mode: posix.mode_t) !void {
    // Walk the path component by component. For each prefix, attempt
    // mkdir; ignore "already exists" if the existing entry is a dir.
    var i: usize = 0;
    while (i < path.len) {
        // Skip leading slashes.
        while (i < path.len and path[i] == '/') i += 1;
        // Find next separator.
        const start = i;
        while (i < path.len and path[i] != '/') i += 1;
        if (i == start) break; // empty component
        const prefix = path[0..i];
        makeDir(prefix, mode) catch |err| switch (err) {
            error.PathAlreadyExists => {}, // tolerate existing dirs
            else => return err,
        };
    }
}

/// Remove a regular file (not a directory).
pub fn removeFile(path: []const u8) !void {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    const Args = struct { path_z: [:0]const u8 };
    var args = Args{ .path_z = path_z };
    return spawnBlocking(struct {
        fn run(a: *Args) !void {
            return syscall.unlinkatZ(posix.AT.FDCWD, a.path_z.ptr, 0);
        }
    }.run, .{&args});
}

/// Recursively remove a directory and all its contents. Walks the
/// tree pre-order (Walker default) and removes in reverse so
/// children disappear before parents. Errors part-way leave the
/// tree in a half-deleted state — callers retry to finish.
pub fn removeTree(allocator: std.mem.Allocator, path: []const u8) !void {
    // Collect every entry below `path` along with its kind. Walker
    // yields paths relative to the walk root; we'll prepend `path/`
    // when calling unlink.
    const Item = struct { path: []u8, kind: Kind };
    var items = std.array_list.Managed(Item).init(allocator);
    defer {
        for (items.items) |item| allocator.free(item.path);
        items.deinit();
    }

    var w = try Walker.open(allocator, path, .{});
    defer w.deinit();
    while (try w.next()) |entry| {
        const owned = try allocator.dupe(u8, entry.path);
        try items.append(.{ .path = owned, .kind = entry.kind });
    }

    // Reverse iterate — Walker is pre-order, so going backward yields
    // children before parents.
    var i: usize = items.items.len;
    while (i > 0) {
        i -= 1;
        const item = items.items[i];
        var full_buf: [4096]u8 = undefined;
        const full = try std.fmt.bufPrint(&full_buf, "{s}/{s}", .{ path, item.path });
        if (item.kind == .directory) {
            try removeDir(full);
        } else {
            try removeFile(full);
        }
    }

    // Finally remove the root itself.
    try removeDir(path);
}

/// Remove an empty directory.
pub fn removeDir(path: []const u8) !void {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    const Args = struct { path_z: [:0]const u8 };
    var args = Args{ .path_z = path_z };
    return spawnBlocking(struct {
        fn run(a: *Args) !void {
            // AT_REMOVEDIR — flag value differs across platforms:
            //   Linux: 0x200    Darwin/BSD: 0x080
            const AT_REMOVEDIR: u32 = switch (builtin.os.tag) {
                .macos, .ios, .tvos, .watchos, .visionos, .driverkit, .freebsd, .netbsd, .openbsd, .dragonfly => 0x080,
                else => 0x200,
            };
            return syscall.unlinkatZ(posix.AT.FDCWD, a.path_z.ptr, AT_REMOVEDIR);
        }
    }.run, .{&args});
}

// ─────────────────────────────────────────────────────────────────────
// rename / link / symlink / readlink
// ─────────────────────────────────────────────────────────────────────

/// Atomically rename `old_path` to `new_path`. Fails with
/// `error.CrossDevice` if the paths span filesystems.
pub fn rename(old_path: []const u8, new_path: []const u8) !void {
    var old_buf: [4096]u8 = undefined;
    var new_buf: [4096]u8 = undefined;
    if (old_path.len >= old_buf.len or new_path.len >= new_buf.len) return error.NameTooLong;
    const old_z = std.fmt.bufPrintZ(&old_buf, "{s}", .{old_path}) catch return error.NameTooLong;
    const new_z = std.fmt.bufPrintZ(&new_buf, "{s}", .{new_path}) catch return error.NameTooLong;

    const Args = struct { old_z: [:0]const u8, new_z: [:0]const u8 };
    var args = Args{ .old_z = old_z, .new_z = new_z };
    return spawnBlocking(struct {
        fn run(a: *Args) !void {
            return syscall.renameZ(a.old_z.ptr, a.new_z.ptr);
        }
    }.run, .{&args});
}

/// Create a symbolic link at `link_path` pointing at `target`.
/// `target` is stored verbatim (not resolved).
pub fn symlink(target: []const u8, link_path: []const u8) !void {
    var target_buf: [4096]u8 = undefined;
    var link_buf: [4096]u8 = undefined;
    if (target.len >= target_buf.len or link_path.len >= link_buf.len) return error.NameTooLong;
    const target_z = std.fmt.bufPrintZ(&target_buf, "{s}", .{target}) catch return error.NameTooLong;
    const link_z = std.fmt.bufPrintZ(&link_buf, "{s}", .{link_path}) catch return error.NameTooLong;

    const Args = struct { target_z: [:0]const u8, link_z: [:0]const u8 };
    var args = Args{ .target_z = target_z, .link_z = link_z };
    return spawnBlocking(struct {
        fn run(a: *Args) !void {
            return syscall.symlinkatZ(a.target_z.ptr, posix.AT.FDCWD, a.link_z.ptr);
        }
    }.run, .{&args});
}

/// Read a symbolic link's target into a freshly-allocated slice.
/// Returns `error.NotLink` if the path isn't a symlink.
pub fn readlinkAlloc(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    // Symlink targets are bounded by PATH_MAX (4096 on Linux, 1024 on
    // Darwin). Use 4096 for both — a few KiB on the blocking-pool
    // thread's stack is fine.
    const Args = struct {
        allocator: std.mem.Allocator,
        path_z: [:0]const u8,
    };
    var args = Args{ .allocator = allocator, .path_z = path_z };
    return spawnBlocking(struct {
        fn run(a: *Args) ![]u8 {
            var scratch: [4096]u8 = undefined;
            const target = try syscall.readlinkatZ(posix.AT.FDCWD, a.path_z.ptr, &scratch);
            return a.allocator.dupe(u8, target);
        }
    }.run, .{&args});
}

// ─────────────────────────────────────────────────────────────────────
// stat / lstat / exists / access
// ─────────────────────────────────────────────────────────────────────

/// Get metadata for `path`. Follows symlinks.
pub fn stat(path: []const u8) !Metadata {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    const Args = struct { path_z: [:0]const u8 };
    var args = Args{ .path_z = path_z };
    const s = try spawnBlocking(struct {
        fn run(a: *Args) !syscall.Stat {
            return syscall.fstatatZ(posix.AT.FDCWD, a.path_z.ptr, 0);
        }
    }.run, .{&args});
    return Metadata.fromStat(s);
}

/// Like `stat` but doesn't follow symlinks — returns metadata about
/// the link itself if `path` is a symlink.
pub fn lstat(path: []const u8) !Metadata {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    const Args = struct { path_z: [:0]const u8 };
    var args = Args{ .path_z = path_z };
    const s = try spawnBlocking(struct {
        fn run(a: *Args) !syscall.Stat {
            return syscall.fstatatZ(posix.AT.FDCWD, a.path_z.ptr, AT_SYMLINK_NOFOLLOW);
        }
    }.run, .{&args});
    return Metadata.fromStat(s);
}

/// Cheap probe for "is there something at this path?". Returns
/// `false` on ENOENT, propagates other errors.
pub fn exists(path: []const u8) !bool {
    access(path, .{ .exists = true }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

/// Bitfield for `access` checks. Pass `.exists = true` for an
/// existence-only probe; combine `read`/`write`/`execute` for
/// permission checks against the calling user.
pub const AccessMode = packed struct {
    exists: bool = false,
    read: bool = false,
    write: bool = false,
    execute: bool = false,
};

/// Check that `path` exists and is accessible per `mode`. Returns
/// without error on success; surfaces `error.FileNotFound`,
/// `error.AccessDenied`, etc. on failure.
pub fn access(path: []const u8, mode: AccessMode) !void {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    var mode_bits: c_uint = 0;
    if (mode.exists or @as(u4, @bitCast(mode)) == 0) mode_bits |= std.c.F_OK;
    if (mode.read) mode_bits |= std.c.R_OK;
    if (mode.write) mode_bits |= std.c.W_OK;
    if (mode.execute) mode_bits |= std.c.X_OK;

    const Args = struct { path_z: [:0]const u8, bits: c_uint };
    var args = Args{ .path_z = path_z, .bits = mode_bits };
    return spawnBlocking(struct {
        fn run(a: *Args) !void {
            const rc = std.c.access(a.path_z.ptr, a.bits);
            if (rc == 0) return;
            return switch (posix.errno(rc)) {
                .ACCES, .PERM => error.AccessDenied,
                .NOENT => error.FileNotFound,
                .NOTDIR => error.NotDir,
                .NAMETOOLONG => error.NameTooLong,
                .LOOP => error.SymLinkLoop,
                .ROFS => error.ReadOnlyFileSystem,
                .IO => error.InputOutput,
                else => error.Unexpected,
            };
        }
    }.run, .{&args});
}

// ─────────────────────────────────────────────────────────────────────
// chmod / chown / utimes
// ─────────────────────────────────────────────────────────────────────

/// Change file mode bits.
pub fn chmod(path: []const u8, mode: posix.mode_t) !void {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    const Args = struct { path_z: [:0]const u8, mode: posix.mode_t };
    var args = Args{ .path_z = path_z, .mode = mode };
    return spawnBlocking(struct {
        fn run(a: *Args) !void {
            const rc = std.c.chmod(a.path_z.ptr, a.mode);
            if (rc == 0) return;
            return switch (posix.errno(rc)) {
                .ACCES, .PERM => error.AccessDenied,
                .NOENT => error.FileNotFound,
                .NOTDIR => error.NotDir,
                .NAMETOOLONG => error.NameTooLong,
                .LOOP => error.SymLinkLoop,
                .ROFS => error.ReadOnlyFileSystem,
                .IO => error.InputOutput,
                else => error.Unexpected,
            };
        }
    }.run, .{&args});
}

/// Change file ownership. `uid` / `gid` of `~0` (max u32) skips
/// that field per POSIX convention.
pub fn chown(path: []const u8, uid: u32, gid: u32) !void {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    const Args = struct { path_z: [:0]const u8, uid: u32, gid: u32 };
    var args = Args{ .path_z = path_z, .uid = uid, .gid = gid };
    return spawnBlocking(struct {
        fn run(a: *Args) !void {
            const rc = std.c.fchownat(posix.AT.FDCWD, a.path_z.ptr, a.uid, a.gid, 0);
            if (rc == 0) return;
            return switch (posix.errno(rc)) {
                .ACCES, .PERM => error.AccessDenied,
                .NOENT => error.FileNotFound,
                .NOTDIR => error.NotDir,
                .NAMETOOLONG => error.NameTooLong,
                .LOOP => error.SymLinkLoop,
                .ROFS => error.ReadOnlyFileSystem,
                .IO => error.InputOutput,
                else => error.Unexpected,
            };
        }
    }.run, .{&args});
}

/// Set the access and modification times.
pub fn utimes(path: []const u8, atime: Instant, mtime: Instant) !void {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    // utimes(2) takes [atime, mtime] as struct timeval (sec, usec).
    const a_secs: i64 = @intCast(@divFloor(atime.timestamp, std.time.ns_per_s));
    const a_us: i32 = @intCast(@divFloor(@mod(atime.timestamp, std.time.ns_per_s), std.time.ns_per_us));
    const m_secs: i64 = @intCast(@divFloor(mtime.timestamp, std.time.ns_per_s));
    const m_us: i32 = @intCast(@divFloor(@mod(mtime.timestamp, std.time.ns_per_s), std.time.ns_per_us));

    const Args = struct {
        path_z: [:0]const u8,
        times: [2]std.posix.timeval,
    };
    var args = Args{
        .path_z = path_z,
        .times = .{
            .{ .sec = a_secs, .usec = a_us },
            .{ .sec = m_secs, .usec = m_us },
        },
    };
    return spawnBlocking(struct {
        fn run(a: *Args) !void {
            const rc = std.c.utimes(a.path_z.ptr, &a.times);
            if (rc == 0) return;
            return switch (posix.errno(rc)) {
                .ACCES, .PERM => error.AccessDenied,
                .NOENT => error.FileNotFound,
                .NOTDIR => error.NotDir,
                .NAMETOOLONG => error.NameTooLong,
                .LOOP => error.SymLinkLoop,
                .ROFS => error.ReadOnlyFileSystem,
                else => error.Unexpected,
            };
        }
    }.run, .{&args});
}

// ─────────────────────────────────────────────────────────────────────
// canonicalize (realpath)
// ─────────────────────────────────────────────────────────────────────

/// Resolve `path` into an absolute, symlink-resolved canonical
/// form. Caller frees the returned slice.
pub fn canonicalize(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var path_buf: [4096]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

    const Args = struct { allocator: std.mem.Allocator, path_z: [:0]const u8 };
    var args = Args{ .allocator = allocator, .path_z = path_z };
    return spawnBlocking(struct {
        fn run(a: *Args) ![]u8 {
            // PATH_MAX (Linux 4096, Darwin 1024) — take the larger.
            var resolved: [4096]u8 = undefined;
            const got = std.c.realpath(a.path_z.ptr, &resolved);
            if (got == null) {
                return switch (posix.errno(@as(c_int, -1))) {
                    .ACCES => error.AccessDenied,
                    .NOENT => error.FileNotFound,
                    .NOTDIR => error.NotDir,
                    .NAMETOOLONG => error.NameTooLong,
                    .LOOP => error.SymLinkLoop,
                    .IO => error.InputOutput,
                    else => error.Unexpected,
                };
            }
            const canonical = std.mem.sliceTo(@as([*:0]const u8, @ptrCast(got.?)), 0);
            return a.allocator.dupe(u8, canonical);
        }
    }.run, .{&args});
}

// ─────────────────────────────────────────────────────────────────────
// copy — read+write fallback (accelerators in v1.2)
// ─────────────────────────────────────────────────────────────────────

/// Copy `src` to `dst`. Returns bytes copied. Today: read + write
/// loop. Kernel accelerators (`copy_file_range`, `clonefile`) land
/// in v1.2 alongside the corresponding syscall wrappers.
pub fn copy(src: []const u8, dst: []const u8) !u64 {
    var src_file = try File.openRead(src);
    defer src_file.close();
    var dst_file = try File.create(dst);
    defer dst_file.close();

    var buf: [64 * 1024]u8 = undefined;
    var total: u64 = 0;
    while (true) {
        const n = try src_file.read(&buf);
        if (n == 0) return total;
        try dst_file.writeAll(buf[0..n]);
        total += n;
    }
}
