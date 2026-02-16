//! Filesystem Operations
//!
//! Convenience functions for common filesystem operations.
//! These are simpler than using File directly and handle the full operation
//! in one call.
//!
//! ## Example
//!
//! ```zig
//! // Read entire file
//! const data = try fs.readFile(allocator, "data.txt");
//! defer allocator.free(data);
//!
//! // Write entire file
//! try fs.writeFile("output.txt", "Hello, world!");
//!
//! // Copy a file
//! try fs.copy("source.txt", "dest.txt");
//!
//! // Check if file exists
//! if (try fs.exists("config.toml")) {
//!     // ...
//! }
//! ```

const std = @import("std");
const posix = std.posix;
const mem = std.mem;
const builtin = @import("builtin");

const File = @import("file.zig").File;
const Metadata = @import("metadata.zig").Metadata;
const Permissions = @import("metadata.zig").Permissions;

// ═══════════════════════════════════════════════════════════════════════════════
// Read Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Read the entire contents of a file into a newly allocated buffer.
pub fn readFile(allocator: mem.Allocator, path: []const u8) ![]u8 {
    var file = try File.open(path);
    defer file.close();
    return file.readToEnd(allocator);
}

/// Read the entire contents of a file as a validated UTF-8 string.
/// Returns error.InvalidUtf8 if the file contains invalid UTF-8 sequences.
pub fn readFileString(allocator: mem.Allocator, path: []const u8) ![]u8 {
    const data = try readFile(allocator, path);
    errdefer allocator.free(data);

    if (!std.unicode.utf8ValidateSlice(data)) {
        return error.InvalidUtf8;
    }

    return data;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Write Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Write data to a file, creating it if it doesn't exist, truncating if it does.
pub fn writeFile(path: []const u8, data: []const u8) !void {
    var file = try File.create(path);
    defer file.close();
    try file.writeAll(data);
}

/// Append data to a file, creating it if it doesn't exist.
pub fn appendFile(path: []const u8, data: []const u8) !void {
    const std_file = try File.getOptions()
        .setWrite(true)
        .setCreate(true)
        .setAppend(true)
        .open(path);

    var file = File.fromStd(std_file);
    defer file.close();

    try file.writeAll(data);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Copy and Move Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Copy a file from source to destination.
pub fn copy(src: []const u8, dst: []const u8) !u64 {
    var src_file = try File.open(src);
    defer src_file.close();

    var dst_file = try File.create(dst);
    defer dst_file.close();

    // Copy in chunks
    var buf: [64 * 1024]u8 = undefined;
    var total: u64 = 0;

    while (true) {
        const n = try src_file.read(&buf);
        if (n == 0) break;

        try dst_file.writeAll(buf[0..n]);
        total += n;
    }

    // Copy permissions
    const meta = try src_file.metadata();
    try dst_file.setPermissions(meta.permissions());

    return total;
}

/// Rename or move a file or directory.
pub fn rename(old_path: []const u8, new_path: []const u8) !void {
    const old_z = try posix.toPosixPath(old_path);
    const new_z = try posix.toPosixPath(new_path);
    try posix.renameZ(&old_z, &new_z);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Link Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Create a hard link.
pub fn hardLink(src: []const u8, dst: []const u8) !void {
    const src_z = try posix.toPosixPath(src);
    const dst_z = try posix.toPosixPath(dst);
    try posix.linkatZ(posix.AT.FDCWD, &src_z, posix.AT.FDCWD, &dst_z, 0);
}

/// Create a symbolic link.
pub fn symlink(target: []const u8, link_path: []const u8) !void {
    const target_z = try posix.toPosixPath(target);
    const link_z = try posix.toPosixPath(link_path);
    try posix.symlinkatZ(&target_z, posix.AT.FDCWD, &link_z);
}

/// Read the target of a symbolic link.
pub fn readLink(allocator: mem.Allocator, path: []const u8) ![]u8 {
    const path_z = try posix.toPosixPath(path);
    var buf: [posix.PATH_MAX]u8 = undefined;
    const link_target = try posix.readlinkatZ(posix.AT.FDCWD, &path_z, &buf);
    const result = try allocator.alloc(u8, link_target.len);
    @memcpy(result, link_target);
    return result;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Metadata Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Get metadata for a path.
pub fn metadata(path: []const u8) !Metadata {
    const path_z = try posix.toPosixPath(path);
    const stat = try posix.fstatatZ(posix.AT.FDCWD, &path_z, 0);
    return Metadata.fromStat(stat);
}

/// Get metadata for a symbolic link (doesn't follow the link).
pub fn symlinkMetadata(path: []const u8) !Metadata {
    const path_z = try posix.toPosixPath(path);
    const stat = try posix.fstatatZ(posix.AT.FDCWD, &path_z, posix.AT.SYMLINK_NOFOLLOW);
    return Metadata.fromStat(stat);
}

/// Set permissions on a path.
pub fn setPermissions(path: []const u8, perm: Permissions) !void {
    const path_z = try posix.toPosixPath(path);
    try posix.chmodZ(&path_z, @intCast(perm.mode));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Path Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Resolve a path to its canonical, absolute form.
pub fn canonicalize(allocator: mem.Allocator, path: []const u8) ![]u8 {
    const path_z = try posix.toPosixPath(path);
    var out_buf: [posix.PATH_MAX]u8 = undefined;
    const result = try posix.realpathZ(&path_z, &out_buf);
    const buf = try allocator.alloc(u8, result.len);
    @memcpy(buf, result);
    return buf;
}

/// Check if a path exists.
pub fn exists(path: []const u8) !bool {
    const path_z = try posix.toPosixPath(path);
    _ = posix.fstatatZ(posix.AT.FDCWD, &path_z, 0) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

/// Check if a path exists (returns false on any error).
pub fn tryExists(path: []const u8) bool {
    return exists(path) catch false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "readFile and writeFile" {
    const path = "/tmp/blitz_io_ops_test.txt";
    const content = "Hello from ops!";

    try writeFile(path, content);
    defer std.fs.deleteFileAbsolute(path) catch {};

    const data = try readFile(std.testing.allocator, path);
    defer std.testing.allocator.free(data);

    try std.testing.expectEqualStrings(content, data);
}

test "readFileString - valid UTF-8" {
    const path = "/tmp/blitz_io_utf8_valid.txt";
    const content = "Hello, 世界! 🎉";

    try writeFile(path, content);
    defer std.fs.deleteFileAbsolute(path) catch {};

    const data = try readFileString(std.testing.allocator, path);
    defer std.testing.allocator.free(data);

    try std.testing.expectEqualStrings(content, data);
}

test "readFileString - invalid UTF-8" {
    const path = "/tmp/blitz_io_utf8_invalid.bin";

    // Write invalid UTF-8 sequence
    {
        var file = try File.create(path);
        defer file.close();
        // 0xFF 0xFE is invalid UTF-8
        try file.writeAll(&[_]u8{ 0xFF, 0xFE, 0x00 });
    }

    defer std.fs.deleteFileAbsolute(path) catch {};

    const result = readFileString(std.testing.allocator, path);
    try std.testing.expectError(error.InvalidUtf8, result);
}

test "appendFile" {
    const path = "/tmp/blitz_io_append_test.txt";

    try writeFile(path, "line1\n");
    defer std.fs.deleteFileAbsolute(path) catch {};

    try appendFile(path, "line2\n");

    const data = try readFile(std.testing.allocator, path);
    defer std.testing.allocator.free(data);

    try std.testing.expectEqualStrings("line1\nline2\n", data);
}

test "copy" {
    const src = "/tmp/blitz_io_copy_src.txt";
    const dst = "/tmp/blitz_io_copy_dst.txt";

    try writeFile(src, "copy me!");
    defer std.fs.deleteFileAbsolute(src) catch {};
    defer std.fs.deleteFileAbsolute(dst) catch {};

    const bytes = try copy(src, dst);
    try std.testing.expectEqual(@as(u64, 8), bytes);

    const data = try readFile(std.testing.allocator, dst);
    defer std.testing.allocator.free(data);

    try std.testing.expectEqualStrings("copy me!", data);
}

test "exists" {
    const path = "/tmp/blitz_io_exists_test.txt";

    try std.testing.expect(!try exists(path));

    try writeFile(path, "");
    defer std.fs.deleteFileAbsolute(path) catch {};

    try std.testing.expect(try exists(path));
}

test "rename" {
    const old = "/tmp/blitz_io_rename_old.txt";
    const new = "/tmp/blitz_io_rename_new.txt";

    try writeFile(old, "rename me");
    defer std.fs.deleteFileAbsolute(new) catch {};

    try rename(old, new);

    try std.testing.expect(!try exists(old));
    try std.testing.expect(try exists(new));
}

test "symlink and readLink" {
    const target = "/tmp/blitz_io_symlink_target.txt";
    const link = "/tmp/blitz_io_symlink_link";

    try writeFile(target, "target");
    defer std.fs.deleteFileAbsolute(target) catch {};
    defer std.fs.deleteFileAbsolute(link) catch {};

    try symlink(target, link);

    const read_target = try readLink(std.testing.allocator, link);
    defer std.testing.allocator.free(read_target);

    try std.testing.expectEqualStrings(target, read_target);
}

test "metadata" {
    const path = "/tmp/blitz_io_meta_ops.txt";

    try writeFile(path, "metadata test");
    defer std.fs.deleteFileAbsolute(path) catch {};

    const meta = try metadata(path);
    try std.testing.expect(meta.isFile());
    try std.testing.expectEqual(@as(u64, 13), meta.size());
}
