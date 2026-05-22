//! File metadata vocabulary — `Metadata`, `Permissions`, `FileType`,
//! `SystemTime`. Filled in by `volt.fs.stat / lstat / fstat`; also
//! produced by `File.metadata()` (Phase B.2) and `DirEntry.metadata()`
//! (Phase B.3).
//!
//! Underlying syscalls return `struct stat`; we wrap the platform-
//! specific layout (`std.c.Stat`) and expose typed accessors that
//! work uniformly across Linux / Darwin / BSD. Windows path lives in
//! its own switch — `FILE_ATTRIBUTE_*` bits drive `Permissions` and
//! a `BY_HANDLE_FILE_INFORMATION` carries timestamps + size.

const std = @import("std");
const builtin = @import("builtin");
const Duration = @import("../time.zig").Duration;

const is_windows = builtin.os.tag == .windows;

/// Kind of filesystem entry. Maps directly to the `S_IFMT` mask on
/// POSIX; on Windows we collapse to `.file` / `.directory` / `.sym_link`
/// since the file-attribute model doesn't distinguish further.
pub const FileType = enum {
    file,
    directory,
    sym_link,
    block_device,
    char_device,
    fifo,
    socket,
    unknown,

    /// Extract from POSIX `st_mode`. Caller passes the whole mode;
    /// we mask off everything but the type bits.
    pub fn fromMode(mode: u32) FileType {
        if (is_windows) return .unknown;
        const kind = mode & std.c.S.IFMT;
        return switch (kind) {
            std.c.S.IFREG => .file,
            std.c.S.IFDIR => .directory,
            std.c.S.IFLNK => .sym_link,
            std.c.S.IFBLK => .block_device,
            std.c.S.IFCHR => .char_device,
            std.c.S.IFIFO => .fifo,
            std.c.S.IFSOCK => .socket,
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

/// POSIX mode bits decoded into per-class read/write/execute flags
/// plus the setuid/setgid/sticky specials. The raw `mode` field is
/// preserved so callers can pass it straight back to `chmod`.
///
/// On Windows v1, only the read-only bit is meaningful — `mode` is
/// either `0o644` (RW) or `0o444` (RO). The per-class accessors
/// still return sensible defaults so cross-platform code compiles.
pub const Permissions = struct {
    mode: u32,

    pub fn fromMode(mode: u32) Permissions {
        return .{ .mode = mode & 0o7777 };
    }

    pub fn fromOctal(octal: u32) Permissions {
        return .{ .mode = octal & 0o7777 };
    }

    /// `true` if no class has write permission. Honoured on Windows
    /// via `FILE_ATTRIBUTE_READONLY`.
    pub fn readonly(self: Permissions) bool {
        return (self.mode & 0o222) == 0;
    }

    /// Toggle the read-only bit. Setting it strips every write
    /// permission; clearing grants owner write only — the rest
    /// stays where the caller left it.
    pub fn setReadonly(self: *Permissions, value: bool) void {
        if (value) {
            self.mode &= ~@as(u32, 0o222);
        } else {
            self.mode |= 0o200;
        }
    }

    pub fn getMode(self: Permissions) u32 {
        return self.mode;
    }

    pub fn ownerRead(self: Permissions) bool {
        return (self.mode & 0o400) != 0;
    }
    pub fn ownerWrite(self: Permissions) bool {
        return (self.mode & 0o200) != 0;
    }
    pub fn ownerExecute(self: Permissions) bool {
        return (self.mode & 0o100) != 0;
    }
    pub fn groupRead(self: Permissions) bool {
        return (self.mode & 0o040) != 0;
    }
    pub fn groupWrite(self: Permissions) bool {
        return (self.mode & 0o020) != 0;
    }
    pub fn groupExecute(self: Permissions) bool {
        return (self.mode & 0o010) != 0;
    }
    pub fn otherRead(self: Permissions) bool {
        return (self.mode & 0o004) != 0;
    }
    pub fn otherWrite(self: Permissions) bool {
        return (self.mode & 0o002) != 0;
    }
    pub fn otherExecute(self: Permissions) bool {
        return (self.mode & 0o001) != 0;
    }
    pub fn setuid(self: Permissions) bool {
        return (self.mode & 0o4000) != 0;
    }
    pub fn setgid(self: Permissions) bool {
        return (self.mode & 0o2000) != 0;
    }
    pub fn sticky(self: Permissions) bool {
        return (self.mode & 0o1000) != 0;
    }
};

/// Wall-clock timestamp. Filesystem timestamps come back as
/// `(secs, nsecs)` pairs; we keep the same shape so library authors
/// can compare two of them or compute a Duration without losing
/// sub-second precision.
///
/// `SystemTime` is wall clock, distinct from `Instant` (monotonic).
/// File mtimes shift if the user adjusts their system clock — that
/// shift is observable here.
pub const SystemTime = struct {
    /// Seconds since Unix epoch (1970-01-01 00:00:00 UTC).
    secs: i64,
    /// Sub-second nanoseconds, `[0, 999_999_999]`.
    nsecs: u32,

    pub const UNIX_EPOCH = SystemTime{ .secs = 0, .nsecs = 0 };

    pub fn fromSecs(secs: i64) SystemTime {
        return .{ .secs = secs, .nsecs = 0 };
    }

    pub fn fromTimespec(ts: std.c.timespec) SystemTime {
        return .{
            .secs = @intCast(ts.sec),
            .nsecs = if (ts.nsec >= 0) @intCast(ts.nsec) else 0,
        };
    }

    pub fn now() SystemTime {
        const ts = std.time.nanoTimestamp();
        const secs = @divFloor(ts, std.time.ns_per_s);
        const nsecs = @mod(ts, std.time.ns_per_s);
        return .{
            .secs = @intCast(secs),
            .nsecs = @intCast(if (nsecs < 0) 0 else nsecs),
        };
    }

    /// Convert to a POSIX `struct timespec` for passing back to
    /// `utimensat` / `futimens`.
    pub fn toTimespec(self: SystemTime) std.c.timespec {
        return .{ .sec = @intCast(self.secs), .nsec = @intCast(self.nsecs) };
    }

    /// Duration from this timestamp until `now()`. Saturates at
    /// zero if `self` is in the future (clock skew, mtime
    /// hand-edited to future).
    pub fn elapsed(self: SystemTime) Duration {
        const current = SystemTime.now();
        return current.durationSince(self);
    }

    /// Duration from `earlier` to `self`. Saturates at zero if
    /// `earlier` is actually later (caller mis-ordered the args).
    pub fn durationSince(self: SystemTime, earlier: SystemTime) Duration {
        const a = self.toNanos();
        const b = earlier.toNanos();
        if (a <= b) return Duration.fromNanos(0);
        return Duration.fromNanos(@intCast(a - b));
    }

    pub fn isBefore(self: SystemTime, other: SystemTime) bool {
        if (self.secs != other.secs) return self.secs < other.secs;
        return self.nsecs < other.nsecs;
    }

    pub fn isAfter(self: SystemTime, other: SystemTime) bool {
        if (self.secs != other.secs) return self.secs > other.secs;
        return self.nsecs > other.nsecs;
    }

    /// Total nanoseconds since Unix epoch. `i128` to avoid overflow
    /// — the year-2554 problem otherwise hits us at i64 nanos.
    fn toNanos(self: SystemTime) i128 {
        return @as(i128, self.secs) * std.time.ns_per_s + self.nsecs;
    }
};

/// Information about a file or directory. Produced by
/// `volt.fs.stat` / `lstat` / `fstat` / `File.metadata()`.
pub const Metadata = struct {
    /// Raw kernel stat buffer — kept private so we can swap to
    /// statx on Linux later without breaking the public surface.
    inner: PlatformStat,

    /// Cached file type — pre-decoded so `isFile()` etc. are
    /// branch-free after the initial stat.
    kind: FileType,

    /// Cached permissions — same rationale.
    perms: Permissions,

    pub fn fromStat(stat: PlatformStat) Metadata {
        if (is_windows) {
            return .{
                .inner = stat,
                .kind = if ((stat.file_attributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0)
                    .directory
                else if ((stat.file_attributes & windows.FILE_ATTRIBUTE_REPARSE_POINT) != 0)
                    .sym_link
                else
                    .file,
                .perms = .{ .mode = if ((stat.file_attributes & windows.FILE_ATTRIBUTE_READONLY) != 0) 0o444 else 0o644 },
            };
        }
        const mode_u32: u32 = @intCast(stat.mode);
        return .{
            .inner = stat,
            .kind = FileType.fromMode(mode_u32),
            .perms = Permissions.fromMode(mode_u32),
        };
    }

    pub fn fileType(self: Metadata) FileType {
        return self.kind;
    }

    pub fn isFile(self: Metadata) bool {
        return self.kind == .file;
    }

    pub fn isDir(self: Metadata) bool {
        return self.kind == .directory;
    }

    pub fn isSymlink(self: Metadata) bool {
        return self.kind == .sym_link;
    }

    /// File size in bytes. For directories the value is filesystem-
    /// dependent — usually a small constant — so don't read into it.
    pub fn size(self: Metadata) u64 {
        if (is_windows) return self.inner.file_size;
        return @intCast(self.inner.size);
    }

    pub fn permissions(self: Metadata) Permissions {
        return self.perms;
    }

    /// Last-modification time. The closest thing to "when did the
    /// file's content change."
    pub fn modified(self: Metadata) SystemTime {
        if (is_windows) return SystemTime{ .secs = self.inner.mtime_secs, .nsecs = self.inner.mtime_nsecs };
        return SystemTime.fromTimespec(self.inner.mtime());
    }

    /// Last-access time. Filesystems often defer or disable atime
    /// updates (`relatime` / `noatime`) for performance; don't trust
    /// fine-grained ordering across two atimes.
    pub fn accessed(self: Metadata) SystemTime {
        if (is_windows) return SystemTime{ .secs = self.inner.atime_secs, .nsecs = self.inner.atime_nsecs };
        return SystemTime.fromTimespec(self.inner.atime());
    }

    /// Creation time. Returns `null` on Linux — ext4 / xfs only
    /// surface birthtime via `statx`, and we use plain `stat` here.
    pub fn created(self: Metadata) ?SystemTime {
        if (is_windows) return SystemTime{ .secs = self.inner.ctime_secs, .nsecs = self.inner.ctime_nsecs };
        return switch (comptime builtin.os.tag) {
            .linux => null,
            .macos, .ios, .tvos, .watchos, .freebsd, .dragonfly => SystemTime.fromTimespec(self.inner.birthtime()),
            else => null,
        };
    }

    /// POSIX-only: device + inode. Useful for cycle detection in
    /// `walk` with `follow_symlinks = true`.
    pub fn device(self: Metadata) u64 {
        if (is_windows) return 0;
        return @intCast(self.inner.dev);
    }

    pub fn inode(self: Metadata) u64 {
        if (is_windows) return 0;
        return @intCast(self.inner.ino);
    }

    pub fn links(self: Metadata) u64 {
        if (is_windows) return self.inner.nlink;
        return @intCast(self.inner.nlink);
    }

    pub fn uid(self: Metadata) u32 {
        if (is_windows) return 0;
        return @intCast(self.inner.uid);
    }

    pub fn gid(self: Metadata) u32 {
        if (is_windows) return 0;
        return @intCast(self.inner.gid);
    }

    pub fn blksize(self: Metadata) u64 {
        if (is_windows) return 0;
        return @intCast(self.inner.blksize);
    }

    pub fn blocks(self: Metadata) u64 {
        if (is_windows) return 0;
        return @intCast(self.inner.blocks);
    }
};

// ─── Platform stat backing store ─────────────────────────────────

/// POSIX path uses `std.c.Stat`; Windows path uses a synthetic
/// struct populated from `BY_HANDLE_FILE_INFORMATION` +
/// `GetFileSizeEx`. The public `Metadata` API hides which one is in
/// play.
pub const PlatformStat = if (is_windows) WindowsStat else std.c.Stat;

const WindowsStat = extern struct {
    file_attributes: u32,
    file_size: u64,
    nlink: u32,
    mtime_secs: i64,
    mtime_nsecs: u32,
    atime_secs: i64,
    atime_nsecs: u32,
    ctime_secs: i64,
    ctime_nsecs: u32,
};

const windows = struct {
    pub const FILE_ATTRIBUTE_READONLY: u32 = 0x0001;
    pub const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x0010;
    pub const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
};

// ─── Tests ───────────────────────────────────────────────────────

const testing = std.testing;

test "Permissions: octal decode" {
    var perm = Permissions.fromOctal(0o755);
    try testing.expect(perm.ownerRead());
    try testing.expect(perm.ownerWrite());
    try testing.expect(perm.ownerExecute());
    try testing.expect(perm.groupRead());
    try testing.expect(!perm.groupWrite());
    try testing.expect(perm.groupExecute());
    try testing.expect(perm.otherRead());
    try testing.expect(!perm.otherWrite());
    try testing.expect(perm.otherExecute());
    try testing.expect(!perm.readonly());

    perm.setReadonly(true);
    try testing.expect(perm.readonly());
    try testing.expect(!perm.ownerWrite());
}

test "Permissions: setuid / setgid / sticky" {
    const perm = Permissions.fromOctal(0o7755);
    try testing.expect(perm.setuid());
    try testing.expect(perm.setgid());
    try testing.expect(perm.sticky());
}

test "FileType: decode from mode bits" {
    if (is_windows) return error.SkipZigTest;
    try testing.expectEqual(FileType.file, FileType.fromMode(std.c.S.IFREG));
    try testing.expectEqual(FileType.directory, FileType.fromMode(std.c.S.IFDIR));
    try testing.expectEqual(FileType.sym_link, FileType.fromMode(std.c.S.IFLNK));
    try testing.expectEqual(FileType.fifo, FileType.fromMode(std.c.S.IFIFO));
    try testing.expectEqual(FileType.socket, FileType.fromMode(std.c.S.IFSOCK));
}

test "SystemTime: ordering + arithmetic" {
    const a = SystemTime.fromSecs(100);
    const b = SystemTime.fromSecs(200);
    try testing.expect(a.isBefore(b));
    try testing.expect(b.isAfter(a));
    try testing.expect(!a.isAfter(b));

    const d = b.durationSince(a);
    try testing.expectEqual(@as(u64, 100), d.toSecs());

    const zero = a.durationSince(b);
    try testing.expectEqual(@as(u64, 0), zero.toNanos());
}

test "SystemTime: timespec round-trip preserves sub-second" {
    if (is_windows) return error.SkipZigTest;
    const ts = std.c.timespec{ .sec = 1234, .nsec = 567_000_000 };
    const st = SystemTime.fromTimespec(ts);
    try testing.expectEqual(@as(i64, 1234), st.secs);
    try testing.expectEqual(@as(u32, 567_000_000), st.nsecs);
    const back = st.toTimespec();
    try testing.expectEqual(@as(@TypeOf(ts.sec), 1234), back.sec);
    try testing.expectEqual(@as(@TypeOf(ts.nsec), 567_000_000), back.nsec);
}
