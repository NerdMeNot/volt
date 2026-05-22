//! Path utilities — thin re-exports over `std.fs.path` plus a few
//! Volt-flavored conveniences. No I/O, no allocator state held here;
//! every allocating function takes an explicit allocator.
//!
//! `std.fs.path` already covers the heavy lifting (join, dirname,
//! basename, extension, stem, isAbsolute, relative, resolve, sep,
//! isSep). This module re-exports them under `volt.fs.path` so users
//! find them where they look for other Volt fs ops, plus adds:
//!
//!   * `normalize` — clean a single path (collapse `//`, resolve
//!     `./` and `../`). Zig std exposes this only via `resolve` on
//!     a paths-list; `normalize` is the single-path case.
//!   * `withExtension` — Node-style "swap the extension."
//!   * `Components` + `parse` — split a path into its parts for
//!     pattern-matching use cases.

const std = @import("std");
const path = std.fs.path;

/// Platform separator: `/` on POSIX, `\` on Windows.
pub const sep = path.sep;
pub const sep_str = path.sep_str;
pub const sep_posix = path.sep_posix;
pub const sep_windows = path.sep_windows;

/// Returns true if `byte` is a valid path separator on the current
/// platform.
pub const isSep = path.isSep;

/// Join `paths` with the platform separator. Allocates.
pub const join = path.join;
pub const joinZ = path.joinZ;

/// Returns the directory portion (everything up to the final
/// separator), or null if there's no separator.
pub const dirname = path.dirname;

/// Returns the file name (everything after the final separator).
/// Always returns a slice; empty if the path ends in a separator.
pub const basename = path.basename;

/// Returns the file extension including the leading `.`, or empty
/// string if there's no extension.
pub const extension = path.extension;

/// Returns the file name without its extension.
pub const stem = path.stem;

/// Returns true if `path` is absolute on the current platform.
pub const isAbsolute = path.isAbsolute;

/// Compute a relative path from `from` to `to`. Both must be
/// absolute paths. Allocates.
pub const relative = path.relative;

/// Resolve a list of paths into a single absolute path. Each
/// successive entry may be absolute (resets) or relative (appends).
pub const resolve = path.resolve;

/// Normalise a single path: collapse `//`, resolve `./` and `../`,
/// produce a canonical form. Convenience wrapper around `resolve`
/// for the common single-path case. Allocates.
///
/// Example: `normalize(alloc, "/a/b/../c//d")` → `/a/c/d`
pub fn normalize(allocator: std.mem.Allocator, p: []const u8) std.mem.Allocator.Error![]u8 {
    return resolve(allocator, &[_][]const u8{p});
}

/// Return a copy of `p` with its extension replaced by `new_ext`.
/// `new_ext` should include the leading `.` (or be empty to strip).
/// Allocates.
///
/// Examples:
///   `withExtension(alloc, "foo.txt", ".md")` → `foo.md`
///   `withExtension(alloc, "foo.tar.gz", ".bz2")` → `foo.tar.bz2`
///   `withExtension(alloc, "foo", ".txt")` → `foo.txt`
///   `withExtension(alloc, "foo.txt", "")` → `foo`
pub fn withExtension(
    allocator: std.mem.Allocator,
    p: []const u8,
    new_ext: []const u8,
) std.mem.Allocator.Error![]u8 {
    const ext = extension(p);
    const stem_len = p.len - ext.len;
    const out = try allocator.alloc(u8, stem_len + new_ext.len);
    @memcpy(out[0..stem_len], p[0..stem_len]);
    @memcpy(out[stem_len..], new_ext);
    return out;
}

/// Components of a parsed path. Useful for pattern-matching when the
/// caller wants more than basename/dirname/extension separately.
///
/// For `"/usr/local/bin/zig"` on POSIX:
///   `.dir = "/usr/local/bin"`, `.name = "zig"`, `.ext = ""`
/// For `"foo.tar.gz"`:
///   `.dir = ""`, `.name = "foo.tar"`, `.ext = ".gz"`
pub const Components = struct {
    dir: []const u8,
    name: []const u8,
    ext: []const u8,
};

/// Split `p` into its components without allocating. All returned
/// slices point into `p`.
pub fn parse(p: []const u8) Components {
    const d = dirname(p) orelse "";
    const ext = extension(p);
    const base = basename(p);
    const name = base[0 .. base.len - ext.len];
    return .{ .dir = d, .name = name, .ext = ext };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const test_alloc = @import("../testing.zig").allocator;

test "path.join + basename + dirname round-trip (POSIX)" {
    if (sep != '/') return error.SkipZigTest;
    const j = try join(test_alloc, &[_][]const u8{ "a", "b", "c" });
    defer test_alloc.free(j);
    try testing.expectEqualStrings("a/b/c", j);
    try testing.expectEqualStrings("c", basename(j));
    try testing.expectEqualStrings("a/b", dirname(j).?);
}

test "path.extension + stem + parse" {
    try testing.expectEqualStrings(".txt", extension("foo.txt"));
    try testing.expectEqualStrings(".gz", extension("foo.tar.gz"));
    try testing.expectEqualStrings("", extension("README"));
    try testing.expectEqualStrings("foo", stem("foo.txt"));
    try testing.expectEqualStrings("foo.tar", stem("foo.tar.gz"));

    const c = parse("/usr/local/bin/zig");
    if (sep == '/') {
        try testing.expectEqualStrings("/usr/local/bin", c.dir);
    }
    try testing.expectEqualStrings("zig", c.name);
    try testing.expectEqualStrings("", c.ext);
}

test "path.normalize collapses // and resolves ../" {
    if (sep != '/') return error.SkipZigTest;
    const n = try normalize(test_alloc, "/a/b/../c//d");
    defer test_alloc.free(n);
    try testing.expectEqualStrings("/a/c/d", n);
}

test "path.withExtension swaps extension" {
    {
        const r = try withExtension(test_alloc, "foo.txt", ".md");
        defer test_alloc.free(r);
        try testing.expectEqualStrings("foo.md", r);
    }
    {
        const r = try withExtension(test_alloc, "foo.tar.gz", ".bz2");
        defer test_alloc.free(r);
        try testing.expectEqualStrings("foo.tar.bz2", r);
    }
    {
        const r = try withExtension(test_alloc, "foo", ".txt");
        defer test_alloc.free(r);
        try testing.expectEqualStrings("foo.txt", r);
    }
    {
        const r = try withExtension(test_alloc, "foo.txt", "");
        defer test_alloc.free(r);
        try testing.expectEqualStrings("foo", r);
    }
}

test "path.isAbsolute + isSep" {
    if (sep == '/') {
        try testing.expect(isAbsolute("/a/b"));
        try testing.expect(!isAbsolute("a/b"));
        try testing.expect(isSep('/'));
        try testing.expect(!isSep('a'));
    }
}
