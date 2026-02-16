//! Directory Operations
//!
//! Functions for creating, reading, and removing directories.
//!
//! ## Example
//!
//! ```zig
//! // Create a directory
//! try createDir("new_folder");
//!
//! // Create nested directories
//! try createDirAll("path/to/nested/folder");
//!
//! // List directory contents
//! var iter = try readDir(".");
//! defer iter.close();
//!
//! while (try iter.next()) |entry| {
//!     std.debug.print("{s}\n", .{entry.name});
//! }
//! ```

const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");
const mem = std.mem;

const Metadata = @import("metadata.zig").Metadata;
const FileType = @import("metadata.zig").FileType;

/// Create a new directory.
pub fn createDir(path: []const u8) !void {
    const path_z = try posix.toPosixPath(path);
    try posix.mkdiratZ(posix.AT.FDCWD, &path_z, 0o755);
}

/// Create a new directory with specific permissions.
pub fn createDirMode(path: []const u8, mode: u32) !void {
    const path_z = try posix.toPosixPath(path);
    try posix.mkdiratZ(posix.AT.FDCWD, &path_z, @intCast(mode));
}

/// Create a directory and all parent directories as needed.
pub fn createDirAll(path: []const u8) !void {
    // Find each path component and create if missing
    var end: usize = 0;

    while (end < path.len) {
        // Find next separator
        while (end < path.len and path[end] != '/') : (end += 1) {}

        if (end > 0) {
            const subpath = path[0..end];
            createDir(subpath) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };
        }

        end += 1;
    }

    // Create the final directory
    if (path.len > 0) {
        createDir(path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
    }
}

/// Remove an empty directory.
pub fn removeDir(path: []const u8) !void {
    const path_z = try posix.toPosixPath(path);
    try posix.unlinkatZ(posix.AT.FDCWD, &path_z, posix.AT.REMOVEDIR);
}

/// Remove a directory and all its contents recursively.
pub fn removeDirAll(allocator: mem.Allocator, path: []const u8) !void {
    // Open the directory
    var dir = std.fs.cwd().openDir(path, .{ .iterate = true }) catch |err| switch (err) {
        error.NotDir => {
            // It's a file, just remove it
            return removeFile(path);
        },
        else => return err,
    };
    defer dir.close();

    // Iterate and remove contents
    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        const full_path = try std.fs.path.join(allocator, &.{ path, entry.name });
        defer allocator.free(full_path);

        switch (entry.kind) {
            .directory => try removeDirAll(allocator, full_path),
            else => try removeFile(full_path),
        }
    }

    // Now remove the empty directory
    try removeDir(path);
}

/// Remove a file.
pub fn removeFile(path: []const u8) !void {
    const path_z = try posix.toPosixPath(path);
    try posix.unlinkatZ(posix.AT.FDCWD, &path_z, 0);
}

/// Directory entry from readDir.
pub const DirEntry = struct {
    name: []const u8,
    kind: FileType,

    /// Check if this entry is a file.
    pub fn isFile(self: DirEntry) bool {
        return self.kind == .file;
    }

    /// Check if this entry is a directory.
    pub fn isDir(self: DirEntry) bool {
        return self.kind == .directory;
    }

    /// Check if this entry is a symbolic link.
    pub fn isSymlink(self: DirEntry) bool {
        return self.kind == .sym_link;
    }
};

/// Iterator over directory entries.
pub const ReadDir = struct {
    inner: std.fs.Dir.Iterator,
    dir: std.fs.Dir,

    /// Get the next entry, or null if done.
    pub fn next(self: *ReadDir) !?DirEntry {
        if (try self.inner.next()) |entry| {
            return DirEntry{
                .name = entry.name,
                .kind = fromStdKind(entry.kind),
            };
        }
        return null;
    }

    /// Close the directory iterator.
    pub fn close(self: *ReadDir) void {
        self.dir.close();
    }

    fn fromStdKind(kind: std.fs.Dir.Entry.Kind) FileType {
        return switch (kind) {
            .file => .file,
            .directory => .directory,
            .sym_link => .sym_link,
            .block_device => .block_device,
            .character_device => .char_device,
            .named_pipe => .fifo,
            .unix_domain_socket => .socket,
            else => .unknown,
        };
    }
};

/// Open a directory for iteration.
pub fn readDir(path: []const u8) !ReadDir {
    const dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
    return ReadDir{
        .inner = dir.iterate(),
        .dir = dir,
    };
}

/// Directory builder with configurable options.
///
/// ## Example
///
/// ```zig
/// // Create nested directories with custom mode
/// try DirBuilder.new()
///     .setRecursive(true)
///     .setMode(0o700)
///     .create("path/to/private/dir");
///
/// // Set mode on all created directories (not just the final one)
/// try DirBuilder.new()
///     .setRecursive(true)
///     .setMode(0o700)
///     .setModeForAll(true)
///     .create("a/b/c");  // All three get 0o700
/// ```
pub const DirBuilder = struct {
    recursive: bool = false,
    mode: u32 = 0o755,
    mode_for_all: bool = false,

    pub fn new() DirBuilder {
        return .{};
    }

    /// Create parent directories as needed.
    pub fn setRecursive(self: DirBuilder, value: bool) DirBuilder {
        var builder = self;
        builder.recursive = value;
        return builder;
    }

    /// Set the directory permissions.
    pub fn setMode(self: DirBuilder, mode_val: u32) DirBuilder {
        var builder = self;
        builder.mode = mode_val;
        return builder;
    }

    /// When true, apply mode to all created directories (including parents).
    /// When false (default), only the final directory gets the custom mode.
    /// This matches the difference between `mkdir -p` (false) and a custom
    /// implementation that sets mode on each created directory (true).
    pub fn setModeForAll(self: DirBuilder, value: bool) DirBuilder {
        var builder = self;
        builder.mode_for_all = value;
        return builder;
    }

    /// Create the directory.
    pub fn create(self: DirBuilder, path: []const u8) !void {
        if (self.recursive) {
            if (self.mode_for_all) {
                try self.createDirAllWithMode(path);
            } else {
                try createDirAll(path);
                // Set final mode only
                try posix.fchmodat(posix.AT.FDCWD, path, @intCast(self.mode), 0);
            }
        } else {
            try createDirMode(path, self.mode);
        }
    }

    /// Create all directories with the configured mode.
    fn createDirAllWithMode(self: DirBuilder, path: []const u8) !void {
        var end: usize = 0;

        while (end < path.len) {
            while (end < path.len and path[end] != '/') : (end += 1) {}

            if (end > 0) {
                const subpath = path[0..end];
                createDirMode(subpath, self.mode) catch |err| switch (err) {
                    error.PathAlreadyExists => {},
                    else => return err,
                };
            }

            end += 1;
        }

        // Create the final directory
        if (path.len > 0) {
            createDirMode(path, self.mode) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "createDir and removeDir" {
    const path = "/tmp/blitz_io_test_dir";

    try createDir(path);
    defer removeDir(path) catch {};

    // Verify it exists
    const path_z = try posix.toPosixPath(path);
    const stat = try posix.fstatatZ(posix.AT.FDCWD, &path_z, 0);
    try std.testing.expect((stat.mode & posix.S.IFMT) == posix.S.IFDIR);
}

test "createDirAll" {
    const path = "/tmp/blitz_io_test_nested/a/b/c";

    try createDirAll(path);
    defer removeDirAll(std.testing.allocator, "/tmp/blitz_io_test_nested") catch {};

    // Verify deepest dir exists
    const path_z = try posix.toPosixPath(path);
    const stat = try posix.fstatatZ(posix.AT.FDCWD, &path_z, 0);
    try std.testing.expect((stat.mode & posix.S.IFMT) == posix.S.IFDIR);
}

test "readDir" {
    const path = "/tmp/blitz_io_test_readdir";

    try createDir(path);
    defer removeDirAll(std.testing.allocator, path) catch {};

    // Create some files
    {
        var f = try std.fs.cwd().createFile("/tmp/blitz_io_test_readdir/file1.txt", .{});
        f.close();
    }
    {
        var f = try std.fs.cwd().createFile("/tmp/blitz_io_test_readdir/file2.txt", .{});
        f.close();
    }

    // Read directory
    var iter = try readDir(path);
    defer iter.close();

    var count: usize = 0;
    while (try iter.next()) |entry| {
        try std.testing.expect(entry.isFile());
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 2), count);
}

test "DirBuilder" {
    const builder = DirBuilder.new()
        .setRecursive(true)
        .setMode(0o700);

    try std.testing.expect(builder.recursive);
    try std.testing.expectEqual(@as(u32, 0o700), builder.mode);
}

test "DirBuilder - modeForAll" {
    const path = "/tmp/blitz_io_dirbuilder_all/a/b/c";
    const base = "/tmp/blitz_io_dirbuilder_all";

    // Create with mode for all directories
    try DirBuilder.new()
        .setRecursive(true)
        .setMode(0o700)
        .setModeForAll(true)
        .create(path);

    defer removeDirAll(std.testing.allocator, base) catch {};

    // Check that intermediate directories also have the mode
    const stat_a = try posix.fstatatZ(posix.AT.FDCWD, &(try posix.toPosixPath("/tmp/blitz_io_dirbuilder_all/a")), 0);
    const stat_b = try posix.fstatatZ(posix.AT.FDCWD, &(try posix.toPosixPath("/tmp/blitz_io_dirbuilder_all/a/b")), 0);
    const stat_c = try posix.fstatatZ(posix.AT.FDCWD, &(try posix.toPosixPath(path)), 0);

    // Mode should be 0o700 for all (with directory bit)
    const expected_mode = 0o700;
    try std.testing.expectEqual(expected_mode, stat_a.mode & 0o777);
    try std.testing.expectEqual(expected_mode, stat_b.mode & 0o777);
    try std.testing.expectEqual(expected_mode, stat_c.mode & 0o777);
}
