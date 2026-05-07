//! `volt.fs.DirEntry` — single entry yielded by a directory iterator.
//!
//! Lifetime: the `name` slice borrows from the iterator's internal
//! buffer and becomes invalid at the next `iter.next()` call. Copy
//! the string if you need it past iteration.

const std = @import("std");

pub const Kind = @import("Metadata.zig").Kind;

pub const DirEntry = struct {
    /// Entry name (without path prefix). Borrowed from the
    /// iterator's internal buffer — copy before the next `next()`
    /// call if needed past iteration.
    name: []const u8,
    /// Kind from the dirent's `d_type` field. May be `.unknown` on
    /// filesystems that don't populate it (some network mounts);
    /// callers can stat the entry directly to disambiguate.
    kind: Kind,
    /// Inode number (for de-dup, hard-link detection).
    inode: u64,
};
