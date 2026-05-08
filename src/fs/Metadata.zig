//! `volt.fs.Metadata` — typed result of `stat` / `lstat` / `fstat`.
//!
//! A flat struct over the bits Volt users actually want, with sane
//! cross-platform names. Posix `Stat` is the underlying source.

const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");

const Instant = @import("../time.zig").Instant;
const syscall = @import("../internal/syscall.zig");

pub const Kind = enum {
    file,
    directory,
    symlink,
    block_device,
    character_device,
    named_pipe,
    unix_domain_socket,
    unknown,
};

pub const Metadata = struct {
    /// Size in bytes (0 for directories on most filesystems).
    size: u64,
    /// Mode bits (rwx + setuid/setgid/sticky).
    mode: posix.mode_t,
    /// File kind — distinguishes regular files from directories,
    /// symlinks, devices, etc. Kept out of `mode` for clarity.
    kind: Kind,
    /// Last access time.
    atime: Instant,
    /// Last modification time.
    mtime: Instant,
    /// Last status change time (chmod/chown).
    ctime: Instant,
    /// Birth (creation) time. `null` on systems that don't expose
    /// it cheaply — Linux requires `statx` (kernel 4.11+); Darwin
    /// has it on Stat directly. Volt v1.1 sets this only when the
    /// platform path makes it free; otherwise null. Full `statx`
    /// integration is a v1.2 follow-up.
    btime: ?Instant = null,

    /// Build a Metadata from Volt's portable Stat. The Stat type
    /// abstracts over Darwin/BSD's `system.Stat` and Linux's `Statx`
    /// (since Zig 0.16 dropped `posix.Stat` on Linux).
    pub fn fromStat(stat: syscall.Stat) Metadata {
        const kind = kindFromMode(stat.mode);
        return .{
            .size = stat.size,
            .mode = @intCast(stat.mode),
            .kind = kind,
            .atime = .{ .timestamp = stat.atime_ns },
            .mtime = .{ .timestamp = stat.mtime_ns },
            .ctime = .{ .timestamp = stat.ctime_ns },
            .btime = if (stat.btime_ns) |ns| Instant{ .timestamp = ns } else null,
        };
    }
};

fn kindFromMode(mode: u32) Kind {
    const fmt = mode & posix.S.IFMT;
    return switch (fmt) {
        posix.S.IFREG => .file,
        posix.S.IFDIR => .directory,
        posix.S.IFLNK => .symlink,
        posix.S.IFBLK => .block_device,
        posix.S.IFCHR => .character_device,
        posix.S.IFIFO => .named_pipe,
        posix.S.IFSOCK => .unix_domain_socket,
        else => .unknown,
    };
}

test "Metadata.kindFromMode: regular file" {
    try std.testing.expectEqual(Kind.file, kindFromMode(posix.S.IFREG | 0o644));
}

test "Metadata.kindFromMode: directory" {
    try std.testing.expectEqual(Kind.directory, kindFromMode(posix.S.IFDIR | 0o755));
}
