//! `volt.fs.path` — path manipulation utilities.
//!
//! Thin wrappers around `std.fs.path` — re-exported so users don't
//! have to reach across two namespaces. All pure-Zig string ops; no
//! allocator unless explicitly noted, no syscalls, no platform
//! dispatch beyond what std already does.

const std = @import("std");

/// Posix-style path join: `path.join(alloc, &.{ "a", "b" })` →
/// `"a/b"`. Caller frees.
pub const join = std.fs.path.join;

/// Like `join` but null-terminated.
pub const joinZ = std.fs.path.joinZ;

/// Return the directory portion: `dirname("a/b/c.txt") == "a/b"`.
/// Returns null for paths without a separator.
pub const dirname = std.fs.path.dirname;

/// Return the base name: `basename("a/b/c.txt") == "c.txt"`.
pub const basename = std.fs.path.basename;

/// Return the extension including the dot: `extension("foo.txt") == ".txt"`.
pub const extension = std.fs.path.extension;

/// True if the path is absolute (starts with `/` on POSIX).
pub const isAbsolute = std.fs.path.isAbsolute;
