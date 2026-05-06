//! `volt.fs.OpenOptions` — builder for opening files.
//!
//! Initialise with the access flags you want, call `.open(path)`.
//! Maps to POSIX `O_*` flags via comptime; the same struct works
//! on Linux + Darwin without code changes.

const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");

const syscall = @import("../internal/syscall.zig");
const spawnBlocking = @import("../api/spawn_blocking.zig").spawnBlocking;
const File = @import("File.zig").File;

pub const OpenOptions = struct {
    read: bool = false,
    write: bool = false,
    /// Append mode — every `write` is atomic-relative-to-EOF.
    /// Implies `write = true`.
    append: bool = false,
    /// Create the file if it doesn't exist.
    create: bool = false,
    /// Fail if the file already exists. Implies `create = true`.
    /// Useful for atomic-create patterns.
    exclusive: bool = false,
    /// Truncate to length 0 if the file exists.
    truncate: bool = false,
    /// Mode bits used when creating (umask still applies). 0o644 is
    /// the conventional default for user-writable files.
    mode: posix.mode_t = 0o644,

    /// Open `path` with the configured options. Routes through the
    /// blocking pool so the calling coroutine doesn't stall its
    /// worker on a slow disk.
    pub fn open(self: OpenOptions, path: []const u8) !File {
        // The file's owner pays the path-z conversion. 4 KiB on the
        // calling coroutine's stack is plenty for any realistic path.
        var path_buf: [4096]u8 = undefined;
        if (path.len >= path_buf.len) return error.NameTooLong;
        const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

        const flags = self.toPosix();
        const mode = self.mode;

        const Args = struct {
            path_z: [:0]const u8,
            flags: posix.O,
            mode: posix.mode_t,
        };
        var args = Args{ .path_z = path_z, .flags = flags, .mode = mode };

        const fd = try spawnBlocking(struct {
            fn run(a: *Args) !posix.fd_t {
                return posix.openatZ(posix.AT.FDCWD, a.path_z.ptr, a.flags, a.mode);
            }
        }.run, .{&args});

        return File{ .fd = fd };
    }

    fn toPosix(self: OpenOptions) posix.O {
        // Access mode is a triple: RDONLY, WRONLY, RDWR.
        const accmode: posix.ACCMODE = if (self.read and self.write)
            .RDWR
        else if (self.write or self.append)
            .WRONLY
        else
            .RDONLY;

        return .{
            .ACCMODE = accmode,
            .APPEND = self.append,
            .CREAT = self.create or self.exclusive,
            .EXCL = self.exclusive,
            .TRUNC = self.truncate,
            .CLOEXEC = true,
        };
    }
};

test "OpenOptions.toPosix: read-only sets ACCMODE=RDONLY" {
    const opts = OpenOptions{ .read = true };
    const flags = opts.toPosix();
    try std.testing.expectEqual(posix.ACCMODE.RDONLY, flags.ACCMODE);
    try std.testing.expect(!flags.CREAT);
    try std.testing.expect(flags.CLOEXEC);
}

test "OpenOptions.toPosix: write+truncate+create" {
    const opts = OpenOptions{ .write = true, .create = true, .truncate = true };
    const flags = opts.toPosix();
    try std.testing.expectEqual(posix.ACCMODE.WRONLY, flags.ACCMODE);
    try std.testing.expect(flags.CREAT);
    try std.testing.expect(flags.TRUNC);
}

test "OpenOptions.toPosix: read+write" {
    const opts = OpenOptions{ .read = true, .write = true };
    const flags = opts.toPosix();
    try std.testing.expectEqual(posix.ACCMODE.RDWR, flags.ACCMODE);
}

test "OpenOptions.toPosix: append implies write-mode" {
    const opts = OpenOptions{ .append = true };
    const flags = opts.toPosix();
    try std.testing.expectEqual(posix.ACCMODE.WRONLY, flags.ACCMODE);
    try std.testing.expect(flags.APPEND);
}

test "OpenOptions.toPosix: exclusive implies CREAT" {
    const opts = OpenOptions{ .write = true, .exclusive = true };
    const flags = opts.toPosix();
    try std.testing.expect(flags.CREAT);
    try std.testing.expect(flags.EXCL);
}
