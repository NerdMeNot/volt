//! libc extern bindings for filesystem syscalls that std.posix
//! doesn't wrap in Zig 0.16. We follow the same pattern as
//! `src/net/options.zig`: declare the libc prototype, gate by
//! platform, route Darwin's `$INODE64` aliases through `std.c.*`
//! where available.
//!
//! Bindings here are kept thin — typed errors live in
//! `src/fs/error.zig`, and the syscall wrappers return raw
//! `(result, errno)` so the caller picks the categorical mapping.

const std = @import("std");
const builtin = @import("builtin");
const metadata = @import("metadata.zig");
const PlatformStat = metadata.PlatformStat;

const is_windows = builtin.os.tag == .windows;
const is_linux = builtin.os.tag == .linux;
const is_darwin = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => true,
    else => false,
};

// On Darwin x86_64 the canonical `stat` symbol is `stat$INODE64`;
// on Darwin arm64 it's the plain `stat`. Same shape for fstat /
// lstat. std.c picks the right symbol for fstat but not for stat /
// lstat in 0.16, so we wire all three ourselves.
//
// Linux uses `statx` instead of `stat` — `std.c.Stat` is `void`
// there (glibc / musl / per-arch layouts diverge enough that std
// won't commit). `statx` has a kernel-defined stable layout
// (`std.os.linux.Statx`); we issue it via raw syscall and decode
// the errno path manually since the libc wrapper is only available
// from glibc 2.28+ and we don't want to gate on that.

const c_stat_default = if (is_windows or is_linux) {} else @extern(
    *const fn ([*:0]const u8, *std.c.Stat) callconv(.c) c_int,
    .{ .name = "stat" },
);
const c_fstat_default = if (is_windows or is_linux) {} else @extern(
    *const fn (c_int, *std.c.Stat) callconv(.c) c_int,
    .{ .name = "fstat" },
);
const c_lstat_default = if (is_windows or is_linux) {} else @extern(
    *const fn ([*:0]const u8, *std.c.Stat) callconv(.c) c_int,
    .{ .name = "lstat" },
);

const c_stat_darwin_x86_64 = if (is_darwin and builtin.cpu.arch == .x86_64) @extern(
    *const fn ([*:0]const u8, *std.c.Stat) callconv(.c) c_int,
    .{ .name = "stat$INODE64" },
) else {};
const c_fstat_darwin_x86_64 = if (is_darwin and builtin.cpu.arch == .x86_64) @extern(
    *const fn (c_int, *std.c.Stat) callconv(.c) c_int,
    .{ .name = "fstat$INODE64" },
) else {};
const c_lstat_darwin_x86_64 = if (is_darwin and builtin.cpu.arch == .x86_64) @extern(
    *const fn ([*:0]const u8, *std.c.Stat) callconv(.c) c_int,
    .{ .name = "lstat$INODE64" },
) else {};

// statx mask covering everything `Metadata`'s accessors care about
// — BASIC_STATS (0x7ff: type, mode, nlink, uid, gid, atime, mtime,
// ctime, ino, size, blocks) plus BTIME for creation-time support on
// filesystems that record it.
const STATX_DEFAULT_MASK: if (is_linux) std.os.linux.STATX else void = if (is_linux) .{
    .TYPE = true,
    .MODE = true,
    .NLINK = true,
    .UID = true,
    .GID = true,
    .ATIME = true,
    .MTIME = true,
    .CTIME = true,
    .INO = true,
    .SIZE = true,
    .BLOCKS = true,
    .BTIME = true,
} else {};

fn linuxStatxAt(dirfd: i32, path: [*:0]const u8, flags: u32, buf: *PlatformStat) c_int {
    const linux = std.os.linux;
    const ret = linux.statx(dirfd, path, flags, STATX_DEFAULT_MASK, &buf.inner);
    const sret: isize = @bitCast(ret);
    if (sret < 0) {
        std.c._errno().* = @intCast(-sret);
        return -1;
    }
    return 0;
}

pub fn stat(path: [*:0]const u8, buf: *PlatformStat) c_int {
    if (is_windows) @compileError("stat not available on Windows path");
    if (comptime is_linux) {
        return linuxStatxAt(std.os.linux.AT.FDCWD, path, 0, buf);
    }
    if (comptime is_darwin and builtin.cpu.arch == .x86_64) return c_stat_darwin_x86_64(path, &buf.inner);
    return c_stat_default(path, &buf.inner);
}

pub fn fstat(fd: c_int, buf: *PlatformStat) c_int {
    if (is_windows) @compileError("fstat not available on Windows path");
    if (comptime is_linux) {
        // AT_EMPTY_PATH (0x1000) + empty path => stat the open fd.
        const AT_EMPTY_PATH: u32 = 0x1000;
        return linuxStatxAt(fd, "", AT_EMPTY_PATH, buf);
    }
    if (comptime is_darwin and builtin.cpu.arch == .x86_64) return c_fstat_darwin_x86_64(fd, &buf.inner);
    return c_fstat_default(fd, &buf.inner);
}

pub fn lstat(path: [*:0]const u8, buf: *PlatformStat) c_int {
    if (is_windows) @compileError("lstat not available on Windows");
    if (comptime is_linux) {
        const AT_SYMLINK_NOFOLLOW: u32 = 0x100;
        return linuxStatxAt(std.os.linux.AT.FDCWD, path, AT_SYMLINK_NOFOLLOW, buf);
    }
    if (comptime is_darwin and builtin.cpu.arch == .x86_64) return c_lstat_darwin_x86_64(path, &buf.inner);
    return c_lstat_default(path, &buf.inner);
}

// `access`, `chmod`, `chown` ride on std.c.* directly — no Darwin
// aliasing.
pub const access = if (is_windows) {} else std.c.access;
pub const chmod = if (is_windows) {} else std.c.chmod;
pub const fchmod = if (is_windows) {} else std.c.fchmod;
pub const fchown = if (is_windows) {} else std.c.fchown;

pub const c_chown_extern = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8, std.c.uid_t, std.c.gid_t) callconv(.c) c_int,
    .{ .name = "chown" },
);

pub fn chown(path: [*:0]const u8, owner: std.c.uid_t, group: std.c.gid_t) c_int {
    if (is_windows) @compileError("chown not available on Windows");
    return c_chown_extern(path, owner, group);
}

// `utimensat(AT_FDCWD, path, times, 0)` updates atime + mtime by
// path with nanosecond precision. Darwin gained `utimensat` in
// 10.13; we assume it's present.
pub const utimensat = if (is_windows) {} else std.c.utimensat;

// `realpath` — Darwin needs `$DARWIN_EXTSN` for the non-PATH_MAX
// variant; std.c handles that.
pub const realpath = if (is_windows) {} else std.c.realpath;

// AT_FDCWD constant — passed as `dirfd` to `*at` syscalls when the
// path is to be resolved relative to the current working directory.
pub const AT_FDCWD: c_int = switch (builtin.os.tag) {
    .linux => -100,
    .macos, .ios, .tvos, .watchos => -2,
    .freebsd, .netbsd, .openbsd, .dragonfly => -100,
    else => -100,
};

// `access` mode bits.
pub const F_OK: c_uint = 0;
pub const R_OK: c_uint = 4;
pub const W_OK: c_uint = 2;
pub const X_OK: c_uint = 1;

// Maximum path length — used to size the buffer for `realpath`.
// POSIX guarantees `PATH_MAX` ≥ 256; in practice everyone uses
// 4096 (Linux) or 1024 (Darwin). We pick the larger to be safe.
pub const PATH_MAX: usize = 4096;

// ─── Additional libc bindings used by tests + B.3 dir ops ────────

pub const c_mkdir = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8, std.c.mode_t) callconv(.c) c_int,
    .{ .name = "mkdir" },
);
pub const c_rmdir = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8) callconv(.c) c_int,
    .{ .name = "rmdir" },
);
pub const c_unlink = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8) callconv(.c) c_int,
    .{ .name = "unlink" },
);
pub const c_mkdtemp = if (is_windows) {} else @extern(
    *const fn ([*:0]u8) callconv(.c) ?[*:0]u8,
    .{ .name = "mkdtemp" },
);
pub const c_mkstemp = if (is_windows) {} else @extern(
    *const fn ([*:0]u8) callconv(.c) c_int,
    .{ .name = "mkstemp" },
);
pub const c_close = if (is_windows) {} else @extern(
    *const fn (c_int) callconv(.c) c_int,
    .{ .name = "close" },
);
// `open` is variadic in libc — `int open(const char *, int, ...)`.
// On Darwin arm64 the variadic and non-variadic ABIs differ (varargs
// go on the stack, regulars in registers), so we MUST declare it
// variadic or the third arg (mode) is silently dropped.
//
// Zig's `extern "c" fn foo(...) ...` syntax supports varargs via
// the literal `...` in the parameter list. @extern doesn't take a
// var-arg type, so we declare via top-level `pub extern fn`.
pub extern "c" fn open(path: [*:0]const u8, oflag: c_int, ...) c_int;

/// Wrapper preserving the prior `c_open(path, flags, mode)` shape.
pub fn c_open(path: [*:0]const u8, flags: c_int, mode: c_uint) c_int {
    if (is_windows) @compileError("c_open not available on Windows");
    return open(path, flags, mode);
}
pub const c_write = if (is_windows) {} else @extern(
    *const fn (c_int, [*]const u8, usize) callconv(.c) isize,
    .{ .name = "write" },
);
pub const c_symlink = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8, [*:0]const u8) callconv(.c) c_int,
    .{ .name = "symlink" },
);
pub const c_readlink = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8, [*]u8, usize) callconv(.c) isize,
    .{ .name = "readlink" },
);

// open(2) flags. Per-platform — the bit layouts differ.
pub const O_RDONLY: c_int = 0;
pub const O_WRONLY: c_int = switch (builtin.os.tag) {
    .linux => 1,
    .macos, .ios, .tvos, .watchos, .freebsd, .netbsd, .openbsd, .dragonfly => 1,
    else => 1,
};
pub const O_RDWR: c_int = 2;
pub const O_CREAT: c_int = switch (builtin.os.tag) {
    .linux => 0o100,
    .macos, .ios, .tvos, .watchos, .freebsd, .netbsd, .openbsd, .dragonfly => 0x200,
    else => 0o100,
};
pub const O_TRUNC: c_int = switch (builtin.os.tag) {
    .linux => 0o1000,
    .macos, .ios, .tvos, .watchos, .freebsd, .netbsd, .openbsd, .dragonfly => 0x400,
    else => 0o1000,
};
pub const O_APPEND: c_int = switch (builtin.os.tag) {
    .linux => 0o2000,
    .macos, .ios, .tvos, .watchos, .freebsd, .netbsd, .openbsd, .dragonfly => 0x008,
    else => 0o2000,
};
pub const O_EXCL: c_int = switch (builtin.os.tag) {
    .linux => 0o200,
    .macos, .ios, .tvos, .watchos, .freebsd, .netbsd, .openbsd, .dragonfly => 0x800,
    else => 0o200,
};
pub const O_NONBLOCK: c_int = switch (builtin.os.tag) {
    .linux => 0o4000,
    .macos, .ios, .tvos, .watchos, .freebsd, .netbsd, .openbsd, .dragonfly => 0x004,
    else => 0o4000,
};
pub const O_CLOEXEC: c_int = switch (builtin.os.tag) {
    .linux => 0o2000000,
    .macos, .ios, .tvos, .watchos => 0x01000000,
    .freebsd, .netbsd, .openbsd, .dragonfly => 0x00100000,
    else => 0,
};
