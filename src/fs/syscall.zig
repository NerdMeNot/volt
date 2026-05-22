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

const is_windows = builtin.os.tag == .windows;
const is_darwin = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => true,
    else => false,
};

// On Darwin x86_64 the canonical `stat` symbol is `stat$INODE64`;
// on Darwin arm64 it's the plain `stat`. Same shape for fstat /
// lstat. std.c picks the right symbol for fstat but not for stat /
// lstat in 0.16, so we wire all three ourselves.

const c_stat_default = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8, *std.c.Stat) callconv(.c) c_int,
    .{ .name = "stat" },
);
const c_fstat_default = if (is_windows) {} else @extern(
    *const fn (c_int, *std.c.Stat) callconv(.c) c_int,
    .{ .name = "fstat" },
);
const c_lstat_default = if (is_windows) {} else @extern(
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

pub fn stat(path: [*:0]const u8, buf: *std.c.Stat) c_int {
    if (is_windows) @compileError("stat not available on Windows path");
    if (comptime is_darwin and builtin.cpu.arch == .x86_64) return c_stat_darwin_x86_64(path, buf);
    return c_stat_default(path, buf);
}

pub fn fstat(fd: c_int, buf: *std.c.Stat) c_int {
    if (is_windows) @compileError("fstat not available on Windows path");
    if (comptime is_darwin and builtin.cpu.arch == .x86_64) return c_fstat_darwin_x86_64(fd, buf);
    return c_fstat_default(fd, buf);
}

pub fn lstat(path: [*:0]const u8, buf: *std.c.Stat) c_int {
    if (is_windows) @compileError("lstat not available on Windows");
    if (comptime is_darwin and builtin.cpu.arch == .x86_64) return c_lstat_darwin_x86_64(path, buf);
    return c_lstat_default(path, buf);
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
pub const c_open = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8, c_int, std.c.mode_t) callconv(.c) c_int,
    .{ .name = "open" },
);
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
