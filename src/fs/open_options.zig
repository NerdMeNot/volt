//! OpenOptions - Builder for File Opening
//!
//! Configure how a file should be opened with fine-grained control over
//! read/write access, creation behavior, and platform-specific options.
//!
//! ## Example
//!
//! ```zig
//! // Open for reading
//! const file = try OpenOptions.new().read(true).open("data.txt");
//!
//! // Create or truncate for writing
//! const file = try OpenOptions.new()
//!     .write(true)
//!     .create(true)
//!     .truncate(true)
//!     .open("output.txt");
//!
//! // Append to existing file
//! const file = try OpenOptions.new()
//!     .write(true)
//!     .append(true)
//!     .open("log.txt");
//! ```

const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");

/// Builder for configuring how a file is opened.
pub const OpenOptions = struct {
    read: bool = false,
    write: bool = false,
    append: bool = false,
    truncate: bool = false,
    create: bool = false,
    create_new: bool = false,
    /// Unix: file mode for new files (default 0o666, modified by umask)
    mode: Mode = 0o666,
    /// Custom open flags (platform-specific)
    custom_flags: u32 = 0,

    pub const Mode = posix.mode_t;

    /// Create a new OpenOptions with all options set to false.
    pub fn new() OpenOptions {
        return .{};
    }

    /// Sets the option for read access.
    pub fn setRead(self: OpenOptions, value: bool) OpenOptions {
        var opts = self;
        opts.read = value;
        return opts;
    }

    /// Sets the option for write access.
    pub fn setWrite(self: OpenOptions, value: bool) OpenOptions {
        var opts = self;
        opts.write = value;
        return opts;
    }

    /// Sets the option for append mode.
    /// When true, writes will append to the file instead of overwriting.
    pub fn setAppend(self: OpenOptions, value: bool) OpenOptions {
        var opts = self;
        opts.append = value;
        return opts;
    }

    /// Sets the option for truncating the file.
    /// When true, the file will be truncated to 0 length when opened.
    pub fn setTruncate(self: OpenOptions, value: bool) OpenOptions {
        var opts = self;
        opts.truncate = value;
        return opts;
    }

    /// Sets the option for creating a new file if it doesn't exist.
    pub fn setCreate(self: OpenOptions, value: bool) OpenOptions {
        var opts = self;
        opts.create = value;
        return opts;
    }

    /// Sets the option for creating a new file, failing if it already exists.
    /// This is atomic: no TOCTOU race between checking and creating.
    pub fn setCreateNew(self: OpenOptions, value: bool) OpenOptions {
        var opts = self;
        opts.create_new = value;
        return opts;
    }

    /// Sets the file mode (Unix only).
    /// The actual mode will be modified by the process umask.
    pub fn setMode(self: OpenOptions, file_mode: Mode) OpenOptions {
        var opts = self;
        opts.mode = file_mode;
        return opts;
    }

    /// Sets custom platform-specific flags.
    pub fn setCustomFlags(self: OpenOptions, flags: u32) OpenOptions {
        var opts = self;
        opts.custom_flags = flags;
        return opts;
    }

    /// Open the file at the given path with the configured options.
    pub fn open(self: OpenOptions, path: []const u8) !std.fs.File {
        // Build flags
        var flags: posix.O = .{};

        // Access mode
        if (self.read and self.write) {
            flags.ACCMODE = .RDWR;
        } else if (self.write) {
            flags.ACCMODE = .WRONLY;
        } else {
            flags.ACCMODE = .RDONLY;
        }

        // Creation flags
        if (self.create_new) {
            flags.CREAT = true;
            flags.EXCL = true;
        } else if (self.create) {
            flags.CREAT = true;
        }

        if (self.truncate) {
            flags.TRUNC = true;
        }

        if (self.append) {
            flags.APPEND = true;
        }

        // Always use CLOEXEC
        flags.CLOEXEC = true;

        // Open the file
        const path_z = try posix.toPosixPath(path);
        const fd = try posix.openZ(&path_z, flags, self.mode);

        return std.fs.File{ .handle = fd };
    }

    /// Open the file at the given path (null-terminated).
    pub fn openZ(self: OpenOptions, path: [*:0]const u8) !std.fs.File {
        var flags: posix.O = .{};

        if (self.read and self.write) {
            flags.ACCMODE = .RDWR;
        } else if (self.write) {
            flags.ACCMODE = .WRONLY;
        } else {
            flags.ACCMODE = .RDONLY;
        }

        if (self.create_new) {
            flags.CREAT = true;
            flags.EXCL = true;
        } else if (self.create) {
            flags.CREAT = true;
        }

        if (self.truncate) {
            flags.TRUNC = true;
        }

        if (self.append) {
            flags.APPEND = true;
        }

        flags.CLOEXEC = true;

        const fd = try posix.openZ(path, flags, self.mode);
        return std.fs.File{ .handle = fd };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "OpenOptions - builder pattern" {
    const opts = OpenOptions.new()
        .setRead(true)
        .setWrite(true)
        .setCreate(true)
        .setTruncate(true)
        .setMode(0o644);

    try std.testing.expect(opts.read);
    try std.testing.expect(opts.write);
    try std.testing.expect(opts.create);
    try std.testing.expect(opts.truncate);
    try std.testing.expectEqual(@as(OpenOptions.Mode, 0o644), opts.mode);
}
