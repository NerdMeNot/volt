//! `volt.net.UnixAddress` — `sockaddr_un` wrapper.
//!
//! Kept separate from `Address` (IPv4/IPv6) because `sockaddr_un`'s
//! 110-byte path field would inflate the IP address type for users
//! who never touch Unix sockets. Tokio does the same split.
//!
//! ## Layout
//!
//! Darwin's `sockaddr_un` carries an `sun_len` byte before
//! `sun_family`; Linux doesn't. Comptime-keyed so each platform gets
//! the right struct.
//!
//! ## Path length
//!
//! Both Darwin (104) and Linux (108) cap the path; we use a 108-byte
//! buffer everywhere (the Darwin kernel ignores the trailing 4 bytes
//! when sun_len + sun_family + path doesn't reach them, but the
//! struct alignment is fine). Paths longer than the platform's
//! limit return `error.PathTooLong`.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

/// Platform-specific kernel ABI struct.
pub const SockAddrUn = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .visionos, .driverkit, .freebsd, .netbsd, .openbsd, .dragonfly => extern struct {
        sun_len: u8 = 0,
        family: u8 = posix.AF.UNIX,
        path: [104]u8 = .{0} ** 104,
    },
    .linux => extern struct {
        family: u16 = posix.AF.UNIX,
        path: [108]u8 = .{0} ** 108,
    },
    else => extern struct {
        family: u16 = posix.AF.UNIX,
        path: [108]u8 = .{0} ** 108,
    },
};

const PATH_CAP: usize = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .visionos, .driverkit, .freebsd, .netbsd, .openbsd, .dragonfly => 104,
    else => 108,
};

pub const UnixAddress = struct {
    inner: SockAddrUn,
    /// Length of the path written into `inner.path` (excluding any
    /// trailing null). Used to compute the socklen passed to syscalls.
    path_len: usize,

    /// Construct from a filesystem path. Returns `error.PathTooLong`
    /// if the path doesn't fit in the platform's `sun_path` buffer
    /// (104 bytes on Darwin/BSD, 108 on Linux).
    pub fn fromPath(p: []const u8) error{PathTooLong}!UnixAddress {
        if (p.len > PATH_CAP) return error.PathTooLong;
        var a = UnixAddress{ .inner = .{}, .path_len = p.len };
        @memcpy(a.inner.path[0..p.len], p);
        return a;
    }

    /// Length to pass to bind/connect — `offsetof(sun_path) + path_len`.
    pub fn osSockLen(self: *const UnixAddress) posix.socklen_t {
        // Darwin: sun_len(1) + family(1) + path = 2 + path_len
        // Linux: family(2) + path = 2 + path_len
        return @intCast(2 + self.path_len);
    }

    /// Cast to `*const posix.sockaddr` for passing to syscalls.
    pub fn sockaddrPtr(self: *const UnixAddress) *const posix.sockaddr {
        return @ptrCast(&self.inner);
    }

    pub fn sockaddrPtrMut(self: *UnixAddress) *posix.sockaddr {
        return @ptrCast(&self.inner);
    }

    /// Path slice (without trailing null).
    pub fn path(self: *const UnixAddress) []const u8 {
        return self.inner.path[0..self.path_len];
    }
};

test "UnixAddress.fromPath: round-trip" {
    const a = try UnixAddress.fromPath("/tmp/volt.sock");
    try std.testing.expectEqualStrings("/tmp/volt.sock", a.path());
}

test "UnixAddress.fromPath: rejects too-long path" {
    const long = "x" ** 200;
    try std.testing.expectError(error.PathTooLong, UnixAddress.fromPath(long));
}
