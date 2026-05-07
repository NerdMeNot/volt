//! `volt.io.MultiReader` — concatenate N readers into one.
//!
//! On EOF of the current reader, advances to the next; returns 0
//! only when all readers are exhausted. Useful for HTTP chunked-
//! body assembly (header + body + trailer), log file rotation, and
//! anywhere you want to present a virtual concatenation to a
//! consumer that takes a single `Reader`.
//!
//! The MultiReader holds a borrowed slice of the input readers; it
//! does NOT take ownership and does NOT close them at exhaustion.
//! Caller manages lifetimes and closes.

const std = @import("std");
const traits = @import("../traits/traits.zig");
const Reader = traits.Reader;
const ReadError = traits.ReadError;

pub const MultiReader = struct {
    sources: []const Reader,
    /// Index of the source currently being read from. Advances on
    /// EOF until equal to `sources.len`.
    cursor: usize = 0,

    pub fn init(sources: []const Reader) MultiReader {
        return .{ .sources = sources };
    }

    pub fn reader(self: *MultiReader) Reader {
        return .{ .ctx = @ptrCast(self), .vtable = &vtable };
    }
};

const vtable: Reader.VTable = .{ .read = &vtableRead };

fn vtableRead(ctx: *anyopaque, buf: []u8) ReadError!usize {
    const self: *MultiReader = @ptrCast(@alignCast(ctx));
    while (self.cursor < self.sources.len) {
        const n = try self.sources[self.cursor].read(buf);
        if (n > 0) return n;
        // Current source exhausted — advance.
        self.cursor += 1;
    }
    return 0;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const SliceReader = traits.SliceReader;

test "MultiReader: yields each source in turn" {
    var s1 = SliceReader{ .bytes = "alpha" };
    var s2 = SliceReader{ .bytes = "bravo" };
    var s3 = SliceReader{ .bytes = "charlie" };

    var sources = [_]Reader{ s1.reader(), s2.reader(), s3.reader() };
    var mr = MultiReader.init(&sources);
    const r = mr.reader();

    var buf: [32]u8 = undefined;
    var total: usize = 0;
    while (true) {
        const n = try r.read(buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqualStrings("alphabravocharlie", buf[0..total]);
}

test "MultiReader: empty source list returns EOF immediately" {
    var sources: [0]Reader = .{};
    var mr = MultiReader.init(&sources);
    var buf: [16]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try mr.reader().read(&buf));
}

test "MultiReader: skips empty sources cleanly" {
    var s1 = SliceReader{ .bytes = "" };
    var s2 = SliceReader{ .bytes = "data" };
    var s3 = SliceReader{ .bytes = "" };

    var sources = [_]Reader{ s1.reader(), s2.reader(), s3.reader() };
    var mr = MultiReader.init(&sources);
    var buf: [16]u8 = undefined;
    var total: usize = 0;
    while (true) {
        const n = try mr.reader().read(buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqualStrings("data", buf[0..total]);
}
