//! # Filesystem Module
//!
//! Provides file and directory operations for volt.
//!
//! ## Overview
//!
//! | Type | Description |
//! |------|-------------|
//! | `File` | File handle for reading/writing |
//! | `AsyncFile` | Async file handle using runtime I/O backend |
//! | `ReadDir` | Directory entry iterator |
//! | `DirEntry` | Entry returned from directory iteration |
//! | `Metadata` | File metadata (size, timestamps, permissions) |
//! | `OpenOptions` | Options for opening files |
//!
//! | Function | Description |
//! |----------|-------------|
//! | `readFile` | Read entire file contents |
//! | `writeFile` | Write data to file (creates/truncates) |
//! | `appendFile` | Append data to file |
//! | `createDir` | Create a directory |
//! | `createDirAll` | Create directory with parents |
//! | `removeDir` | Remove empty directory |
//! | `removeDirAll` | Remove directory recursively |
//! | `readDir` | Iterate over directory entries |
//! | `copy` | Copy a file |
//! | `rename` | Rename/move file or directory |
//! | `exists` | Check if path exists |
//!
//! ## Quick Start
//!
//! ```zig
//! const fs = @import("volt").fs;
//!
//! // Read entire file
//! const data = try fs.readFile(allocator, "input.txt");
//! defer allocator.free(data);
//!
//! // Write entire file
//! try fs.writeFile("output.txt", "Hello!");
//!
//! // Work with File handle
//! var file = try fs.File.open("data.bin");
//! defer file.close();
//!
//! var buf: [1024]u8 = undefined;
//! const n = try file.read(&buf);
//! ```
//!
//! ## File Handle
//!
//! For more control, use the `File` type directly:
//!
//! ```zig
//! var file = try fs.File.create("output.bin");
//! defer file.close();
//!
//! try file.writeAll(data);
//! try file.sync();
//!
//! // Seek and read
//! _ = try file.seek(0, .start);
//! const n = try file.read(&buf);
//! ```
//!
//! ## Directory Operations
//!
//! ```zig
//! // Create directories
//! try fs.createDir("new_folder");
//! try fs.createDirAll("path/to/nested");
//!
//! // List directory
//! var iter = try fs.readDir(".");
//! defer iter.close();
//!
//! while (try iter.next()) |entry| {
//!     std.debug.print("{s} ({s})\n", .{
//!         entry.name,
//!         if (entry.isDir()) "dir" else "file",
//!     });
//! }
//! ```
//!
//! ## Reader/Writer Interfaces
//!
//! Files support the polymorphic Reader/Writer interfaces:
//!
//! ```zig
//! var file = try fs.File.open("data.txt");
//! var r = file.reader();
//!
//! // Can wrap with buffering, decompression, etc.
//! ```

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// Core Types
// ═══════════════════════════════════════════════════════════════════════════════

/// File handle for reading and writing.
pub const File = @import("fs/file.zig").File;

/// Options for opening files.
pub const OpenOptions = @import("fs/open_options.zig").OpenOptions;

/// File metadata (size, timestamps, permissions).
pub const Metadata = @import("fs/metadata.zig").Metadata;

/// File permissions.
pub const Permissions = @import("fs/metadata.zig").Permissions;

/// File type (file, directory, symlink, etc.).
pub const FileType = @import("fs/metadata.zig").FileType;

/// A point in time from the filesystem (for timestamps).
pub const SystemTime = @import("fs/metadata.zig").SystemTime;

// ═══════════════════════════════════════════════════════════════════════════════
// Async File I/O (requires Runtime)
// ═══════════════════════════════════════════════════════════════════════════════

const async_file_mod = @import("fs/async_file.zig");

/// Async file handle that uses the runtime's I/O backend.
/// - Linux with io_uring: True async I/O
/// - Other platforms: Uses blocking pool
pub const AsyncFile = async_file_mod.AsyncFile;

/// Read entire file using async I/O.
pub const readFileAsync = async_file_mod.readFileAsync;

/// Write entire file using async I/O.
pub const writeFileAsync = async_file_mod.writeFileAsync;

/// Append to file using async I/O.
pub const appendFileAsync = async_file_mod.appendFileAsync;

// ═══════════════════════════════════════════════════════════════════════════════
// Directory Types
// ═══════════════════════════════════════════════════════════════════════════════

const dir_mod = @import("fs/dir.zig");

/// Directory entry from readDir.
pub const DirEntry = dir_mod.DirEntry;

/// Iterator over directory entries.
pub const ReadDir = dir_mod.ReadDir;

/// Builder for creating directories.
pub const DirBuilder = dir_mod.DirBuilder;

// ═══════════════════════════════════════════════════════════════════════════════
// File Operations (Convenience Functions)
// ═══════════════════════════════════════════════════════════════════════════════

const ops = @import("fs/ops.zig");

/// Read the entire contents of a file.
pub const readFile = ops.readFile;

/// Read the entire contents of a file as a string.
pub const readFileString = ops.readFileString;

/// Write data to a file (creates or truncates).
pub const writeFile = ops.writeFile;

/// Append data to a file (creates if needed).
pub const appendFile = ops.appendFile;

/// Copy a file.
pub const copy = ops.copy;

/// Rename or move a file/directory.
pub const rename = ops.rename;

// ═══════════════════════════════════════════════════════════════════════════════
// Directory Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Create a directory.
pub const createDir = dir_mod.createDir;

/// Create a directory with specific permissions.
pub const createDirMode = dir_mod.createDirMode;

/// Create a directory and all parent directories.
pub const createDirAll = dir_mod.createDirAll;

/// Remove an empty directory.
pub const removeDir = dir_mod.removeDir;

/// Remove a directory and all its contents.
pub const removeDirAll = dir_mod.removeDirAll;

/// Remove a file.
pub const removeFile = dir_mod.removeFile;

/// Open a directory for iteration.
pub const readDir = dir_mod.readDir;

// ═══════════════════════════════════════════════════════════════════════════════
// Link Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Create a hard link.
pub const hardLink = ops.hardLink;

/// Create a symbolic link.
pub const symlink = ops.symlink;

/// Read the target of a symbolic link.
pub const readLink = ops.readLink;

// ═══════════════════════════════════════════════════════════════════════════════
// Metadata Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Get metadata for a path.
pub const metadata = ops.metadata;

/// Get metadata for a symbolic link (doesn't follow).
pub const symlinkMetadata = ops.symlinkMetadata;

/// Set permissions on a path.
pub const setPermissions = ops.setPermissions;

// ═══════════════════════════════════════════════════════════════════════════════
// Path Operations
// ═══════════════════════════════════════════════════════════════════════════════

/// Resolve a path to its canonical form.
pub const canonicalize = ops.canonicalize;

/// Check if a path exists.
pub const exists = ops.exists;

/// Check if a path exists (returns false on error).
pub const tryExists = ops.tryExists;

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test {
    _ = @import("fs/file.zig");
    _ = @import("fs/open_options.zig");
    _ = @import("fs/metadata.zig");
    _ = @import("fs/dir.zig");
    _ = @import("fs/ops.zig");
    _ = @import("fs/async_file.zig");
}
