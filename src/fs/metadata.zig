//! File Metadata - Information about files and directories
//!
//! Provides types for querying file metadata including size, timestamps,
//! permissions, and file type.
//!
//! ## SystemTime
//!
//! Timestamps are returned as `SystemTime` which provides ergonomic methods:
//!
//! ```zig
//! const meta = try fs.metadata("file.txt");
//! const mtime = meta.modifiedTime();
//!
//! // Check if modified within last hour
//! if (mtime.elapsed().asHours() < 1) {
//!     // Recently modified
//! }
//!
//! // Compare timestamps
//! const atime = meta.accessedTime();
//! const diff = mtime.durationSince(atime);
//! ```

const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");
const Duration = @import("../time.zig").Duration;

/// Metadata about a file or directory.
pub const Metadata = struct {
    inner: posix.Stat,

    /// Create Metadata from a posix Stat.
    pub fn fromStat(stat: posix.Stat) Metadata {
        return .{ .inner = stat };
    }

    /// Get the file type.
    pub fn fileType(self: Metadata) FileType {
        return FileType.fromMode(self.inner.mode);
    }

    /// Returns true if this is a regular file.
    pub fn isFile(self: Metadata) bool {
        return self.fileType() == .file;
    }

    /// Returns true if this is a directory.
    pub fn isDir(self: Metadata) bool {
        return self.fileType() == .directory;
    }

    /// Returns true if this is a symbolic link.
    pub fn isSymlink(self: Metadata) bool {
        return self.fileType() == .sym_link;
    }

    /// Get the size of the file in bytes.
    pub fn size(self: Metadata) u64 {
        return @intCast(self.inner.size);
    }

    /// Get the permissions.
    pub fn permissions(self: Metadata) Permissions {
        return Permissions.fromMode(self.inner.mode);
    }

    /// Get the last modification time as seconds since Unix epoch.
    /// For a more ergonomic API, use `modifiedTime()` which returns `SystemTime`.
    pub fn modified(self: Metadata) i64 {
        return self.inner.mtime().tv_sec;
    }

    /// Get the last access time as seconds since Unix epoch.
    /// For a more ergonomic API, use `accessedTime()` which returns `SystemTime`.
    pub fn accessed(self: Metadata) i64 {
        return self.inner.atime().tv_sec;
    }

    /// Get the creation time as seconds since Unix epoch (if available).
    /// Returns null on platforms that don't support creation time.
    /// For a more ergonomic API, use `createdTime()` which returns `?SystemTime`.
    pub fn created(self: Metadata) ?i64 {
        if (comptime builtin.os.tag == .linux) {
            // Linux doesn't have creation time in stat
            return null;
        } else if (comptime builtin.os.tag == .macos or builtin.os.tag == .freebsd) {
            return self.inner.birthtime().tv_sec;
        } else {
            return null;
        }
    }

    /// Get the last modification time as SystemTime.
    pub fn modifiedTime(self: Metadata) SystemTime {
        const ts = self.inner.mtime();
        return SystemTime.fromTimespec(ts);
    }

    /// Get the last access time as SystemTime.
    pub fn accessedTime(self: Metadata) SystemTime {
        const ts = self.inner.atime();
        return SystemTime.fromTimespec(ts);
    }

    /// Get the creation time as SystemTime (if available).
    /// Returns null on platforms that don't support creation time (Linux).
    pub fn createdTime(self: Metadata) ?SystemTime {
        if (comptime builtin.os.tag == .linux) {
            return null;
        } else if (comptime builtin.os.tag == .macos or builtin.os.tag == .freebsd) {
            const ts = self.inner.birthtime();
            return SystemTime.fromTimespec(ts);
        } else {
            return null;
        }
    }

    /// Get the raw mode bits.
    pub fn mode(self: Metadata) u32 {
        return self.inner.mode;
    }

    /// Get the device ID (Unix).
    pub fn dev(self: Metadata) u64 {
        return self.inner.dev;
    }

    /// Get the inode number (Unix).
    pub fn ino(self: Metadata) u64 {
        return self.inner.ino;
    }

    /// Get the number of hard links.
    pub fn nlink(self: Metadata) u64 {
        return self.inner.nlink;
    }

    /// Get the user ID of the owner (Unix).
    pub fn uid(self: Metadata) u32 {
        return self.inner.uid;
    }

    /// Get the group ID of the owner (Unix).
    pub fn gid(self: Metadata) u32 {
        return self.inner.gid;
    }

    /// Get the block size for filesystem I/O.
    pub fn blksize(self: Metadata) u64 {
        return @intCast(self.inner.blksize);
    }

    /// Get the number of 512-byte blocks allocated.
    pub fn blocks(self: Metadata) u64 {
        return @intCast(self.inner.blocks);
    }
};

/// Type of a file system entry.
pub const FileType = enum {
    file,
    directory,
    sym_link,
    block_device,
    char_device,
    fifo,
    socket,
    unknown,

    /// Extract file type from mode bits.
    pub fn fromMode(mode: u32) FileType {
        const kind = mode & posix.S.IFMT;
        return switch (kind) {
            posix.S.IFREG => .file,
            posix.S.IFDIR => .directory,
            posix.S.IFLNK => .sym_link,
            posix.S.IFBLK => .block_device,
            posix.S.IFCHR => .char_device,
            posix.S.IFIFO => .fifo,
            posix.S.IFSOCK => .socket,
            else => .unknown,
        };
    }

    pub fn isFile(self: FileType) bool {
        return self == .file;
    }

    pub fn isDir(self: FileType) bool {
        return self == .directory;
    }

    pub fn isSymlink(self: FileType) bool {
        return self == .sym_link;
    }
};

/// File permissions.
pub const Permissions = struct {
    mode: u32,

    /// Create Permissions from mode bits.
    pub fn fromMode(mode: u32) Permissions {
        return .{ .mode = mode & 0o7777 };
    }

    /// Create Permissions from an octal mode (e.g., 0o755).
    pub fn fromOctal(octal: u32) Permissions {
        return .{ .mode = octal & 0o7777 };
    }

    /// Check if the file is readonly (no write permission for anyone).
    pub fn readonly(self: Permissions) bool {
        return (self.mode & 0o222) == 0;
    }

    /// Set readonly status.
    pub fn setReadonly(self: *Permissions, value: bool) void {
        if (value) {
            self.mode &= ~@as(u32, 0o222);
        } else {
            self.mode |= 0o200; // At least owner write
        }
    }

    /// Get the raw mode bits (e.g., 0o755).
    pub fn getMode(self: Permissions) u32 {
        return self.mode;
    }

    /// Check owner read permission.
    pub fn ownerRead(self: Permissions) bool {
        return (self.mode & posix.S.IRUSR) != 0;
    }

    /// Check owner write permission.
    pub fn ownerWrite(self: Permissions) bool {
        return (self.mode & posix.S.IWUSR) != 0;
    }

    /// Check owner execute permission.
    pub fn ownerExecute(self: Permissions) bool {
        return (self.mode & posix.S.IXUSR) != 0;
    }

    /// Check group read permission.
    pub fn groupRead(self: Permissions) bool {
        return (self.mode & posix.S.IRGRP) != 0;
    }

    /// Check group write permission.
    pub fn groupWrite(self: Permissions) bool {
        return (self.mode & posix.S.IWGRP) != 0;
    }

    /// Check group execute permission.
    pub fn groupExecute(self: Permissions) bool {
        return (self.mode & posix.S.IXGRP) != 0;
    }

    /// Check other read permission.
    pub fn otherRead(self: Permissions) bool {
        return (self.mode & posix.S.IROTH) != 0;
    }

    /// Check other write permission.
    pub fn otherWrite(self: Permissions) bool {
        return (self.mode & posix.S.IWOTH) != 0;
    }

    /// Check other execute permission.
    pub fn otherExecute(self: Permissions) bool {
        return (self.mode & posix.S.IXOTH) != 0;
    }
};

/// A point in time from the filesystem.
///
/// Provides ergonomic methods for comparing and measuring file timestamps.
///
/// ## Example
///
/// ```zig
/// const meta = try fs.metadata("file.txt");
/// const mtime = meta.modifiedTime();
///
/// // Check how long ago the file was modified
/// const age = mtime.elapsed();
/// if (age.asHours() < 1) {
///     // Modified within the last hour
/// }
///
/// // Compare two timestamps
/// const atime = meta.accessedTime();
/// if (mtime.isAfter(atime)) {
///     // File was modified after it was last accessed (unusual)
/// }
/// ```
pub const SystemTime = struct {
    /// Seconds since Unix epoch (1970-01-01 00:00:00 UTC).
    secs: i64,
    /// Nanoseconds within the second (0 to 999,999,999).
    nsecs: u32,

    /// Unix epoch (1970-01-01 00:00:00 UTC).
    pub const UNIX_EPOCH = SystemTime{ .secs = 0, .nsecs = 0 };

    /// Create from seconds since Unix epoch.
    pub fn fromSecs(secs: i64) SystemTime {
        return .{ .secs = secs, .nsecs = 0 };
    }

    /// Create from a timespec.
    pub fn fromTimespec(ts: posix.timespec) SystemTime {
        return .{
            .secs = ts.tv_sec,
            .nsecs = if (ts.tv_nsec >= 0) @intCast(ts.tv_nsec) else 0,
        };
    }

    /// Get the current system time.
    pub fn now() SystemTime {
        const ts = std.time.nanoTimestamp();
        const secs = @divFloor(ts, std.time.ns_per_s);
        const nsecs = @mod(ts, std.time.ns_per_s);
        return .{
            .secs = @intCast(secs),
            .nsecs = @intCast(if (nsecs < 0) 0 else nsecs),
        };
    }

    /// Duration elapsed since this time (from now).
    /// Returns Duration.ZERO if this time is in the future.
    pub fn elapsed(self: SystemTime) Duration {
        const current = now();
        return current.durationSince(self);
    }

    /// Duration between this time and an earlier time.
    /// Returns Duration.ZERO if `earlier` is actually later than self.
    pub fn durationSince(self: SystemTime, earlier: SystemTime) Duration {
        const self_nanos = self.toNanos();
        const earlier_nanos = earlier.toNanos();

        if (self_nanos <= earlier_nanos) {
            return Duration.ZERO;
        }

        return Duration.fromNanos(@intCast(self_nanos - earlier_nanos));
    }

    /// Check if this time is before another.
    pub fn isBefore(self: SystemTime, other: SystemTime) bool {
        if (self.secs != other.secs) {
            return self.secs < other.secs;
        }
        return self.nsecs < other.nsecs;
    }

    /// Check if this time is after another.
    pub fn isAfter(self: SystemTime, other: SystemTime) bool {
        if (self.secs != other.secs) {
            return self.secs > other.secs;
        }
        return self.nsecs > other.nsecs;
    }

    /// Add a duration to this time.
    pub fn add(self: SystemTime, duration: Duration) SystemTime {
        const total_nanos = self.toNanos() + @as(i128, duration.asNanos());
        return fromNanosI128(total_nanos);
    }

    /// Subtract a duration from this time.
    pub fn sub(self: SystemTime, duration: Duration) SystemTime {
        const total_nanos = self.toNanos() - @as(i128, duration.asNanos());
        return fromNanosI128(total_nanos);
    }

    /// Get seconds since Unix epoch.
    pub fn asSecs(self: SystemTime) i64 {
        return self.secs;
    }

    /// Get total nanoseconds since Unix epoch.
    fn toNanos(self: SystemTime) i128 {
        return @as(i128, self.secs) * std.time.ns_per_s + self.nsecs;
    }

    fn fromNanosI128(nanos: i128) SystemTime {
        const secs = @divFloor(nanos, std.time.ns_per_s);
        const nsecs = @mod(nanos, std.time.ns_per_s);
        return .{
            .secs = @intCast(secs),
            .nsecs = @intCast(if (nsecs < 0) 0 else nsecs),
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Permissions - basic operations" {
    var perm = Permissions.fromOctal(0o755);

    try std.testing.expect(perm.ownerRead());
    try std.testing.expect(perm.ownerWrite());
    try std.testing.expect(perm.ownerExecute());
    try std.testing.expect(perm.groupRead());
    try std.testing.expect(!perm.groupWrite());
    try std.testing.expect(perm.groupExecute());
    try std.testing.expect(!perm.readonly());

    perm.setReadonly(true);
    try std.testing.expect(perm.readonly());
}

test "FileType - from mode" {
    try std.testing.expectEqual(FileType.file, FileType.fromMode(posix.S.IFREG));
    try std.testing.expectEqual(FileType.directory, FileType.fromMode(posix.S.IFDIR));
    try std.testing.expectEqual(FileType.sym_link, FileType.fromMode(posix.S.IFLNK));
}

test "SystemTime - from seconds" {
    const t = SystemTime.fromSecs(1000);
    try std.testing.expectEqual(@as(i64, 1000), t.secs);
    try std.testing.expectEqual(@as(u32, 0), t.nsecs);
}

test "SystemTime - comparisons" {
    const t1 = SystemTime.fromSecs(100);
    const t2 = SystemTime.fromSecs(200);

    try std.testing.expect(t1.isBefore(t2));
    try std.testing.expect(t2.isAfter(t1));
    try std.testing.expect(!t1.isAfter(t2));
    try std.testing.expect(!t2.isBefore(t1));
}

test "SystemTime - duration arithmetic" {
    const t1 = SystemTime.fromSecs(100);
    const t2 = SystemTime.fromSecs(150);

    const diff = t2.durationSince(t1);
    try std.testing.expectEqual(@as(u64, 50), diff.asSecs());

    // Duration since later time should be zero
    const zero_diff = t1.durationSince(t2);
    try std.testing.expectEqual(@as(u64, 0), zero_diff.asSecs());
}

test "SystemTime - add and sub" {
    const t = SystemTime.fromSecs(100);
    const d = Duration.fromSecs(50);

    const t_plus = t.add(d);
    try std.testing.expectEqual(@as(i64, 150), t_plus.secs);

    const t_minus = t.sub(d);
    try std.testing.expectEqual(@as(i64, 50), t_minus.secs);
}

test "SystemTime - UNIX_EPOCH" {
    try std.testing.expectEqual(@as(i64, 0), SystemTime.UNIX_EPOCH.secs);
    try std.testing.expectEqual(@as(u32, 0), SystemTime.UNIX_EPOCH.nsecs);
}
