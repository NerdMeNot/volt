//! `volt.io.lineIterator` — iterate lines from a `Reader`.
//!
//! Thin convenience over `BufReader.readUntilAlloc('\n', …)`. Each
//! `next()` call returns a freshly-allocated, owned line (without
//! the trailing `\n`); `null` signals EOF. Caller frees each line.
//!
//! For a borrowed-from-internal-buffer iterator (no per-line alloc),
//! use `BufReader.readUntil` directly with a stack-allocated buffer.

const std = @import("std");
const traits = @import("../traits/traits.zig");
const Reader = traits.Reader;
const ReadError = traits.ReadError;
const BufReader = @import("BufReader.zig").BufReader;

pub const LineIterator = struct {
    br: *BufReader,
    allocator: std.mem.Allocator,
    max_line: usize,

    /// Yield the next line, allocated from `self.allocator`. Returns
    /// `null` on clean EOF. Returns `error.StreamTooLong` if a line
    /// exceeds `max_line` bytes (excluding the terminator).
    pub fn next(self: *LineIterator) (ReadError || std.mem.Allocator.Error)!?[]u8 {
        const line = self.br.readUntilAlloc(self.allocator, '\n', self.max_line) catch |err| switch (err) {
            error.EndOfStream => return null,
            else => |e| return e,
        };
        return line;
    }
};

/// Construct a line iterator over `br`. The iterator borrows `br`;
/// callers should `br.deinit()` after the iterator is exhausted.
pub fn lineIterator(br: *BufReader, allocator: std.mem.Allocator, max_line: usize) LineIterator {
    return .{ .br = br, .allocator = allocator, .max_line = max_line };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const SliceReader = traits.SliceReader;

test "lineIterator: yields each line, then null on EOF" {
    var sr = SliceReader{ .bytes = "alpha\nbeta\ngamma\n" };
    var br = try BufReader.init(std.testing.allocator, sr.reader(), 8);
    defer br.deinit();

    var it = lineIterator(&br, std.testing.allocator, 64);

    const a = (try it.next()).?;
    defer std.testing.allocator.free(a);
    try std.testing.expectEqualStrings("alpha", a);

    const b = (try it.next()).?;
    defer std.testing.allocator.free(b);
    try std.testing.expectEqualStrings("beta", b);

    const g = (try it.next()).?;
    defer std.testing.allocator.free(g);
    try std.testing.expectEqualStrings("gamma", g);

    try std.testing.expectEqual(@as(?[]u8, null), try it.next());
}

test "lineIterator: rejects oversized line" {
    var sr = SliceReader{ .bytes = "this-line-is-way-too-long\n" };
    var br = try BufReader.init(std.testing.allocator, sr.reader(), 64);
    defer br.deinit();

    var it = lineIterator(&br, std.testing.allocator, 8);
    try std.testing.expectError(error.StreamTooLong, it.next());
}
