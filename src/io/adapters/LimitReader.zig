//! `volt.io.LimitReader` — read at most N bytes from a source, then EOF.
//!
//! Used for length-prefixed framing (HTTP body with Content-Length,
//! gRPC frame, S3 object range, …): wrap the source in a LimitReader
//! sized to the frame length and hand the LimitReader to the body
//! parser. The parser sees a clean EOF at the frame boundary; the
//! underlying source remains usable for the next frame.

const std = @import("std");
const traits = @import("../traits/traits.zig");
const Reader = traits.Reader;
const ReadError = traits.ReadError;

pub const LimitReader = struct {
    source: Reader,
    remaining: u64,

    pub fn init(source: Reader, limit: u64) LimitReader {
        return .{ .source = source, .remaining = limit };
    }

    pub fn reader(self: *LimitReader) Reader {
        return .{ .ctx = @ptrCast(self), .vtable = &vtable };
    }

    /// Bytes still allowed before this LimitReader reports EOF.
    pub fn bytesLeft(self: *const LimitReader) u64 {
        return self.remaining;
    }
};

const vtable: Reader.VTable = .{ .read = &vtableRead };

fn vtableRead(ctx: *anyopaque, buf: []u8) ReadError!usize {
    const self: *LimitReader = @ptrCast(@alignCast(ctx));
    if (self.remaining == 0) return 0;
    const want: usize = if (buf.len < self.remaining) buf.len else @intCast(self.remaining);
    const got = try self.source.read(buf[0..want]);
    self.remaining -= got;
    return got;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const SliceReader = traits.SliceReader;

test "LimitReader: stops at limit even if source has more" {
    var sr = SliceReader{ .bytes = "abcdefghij" };
    var lr = LimitReader.init(sr.reader(), 4);
    var buf: [16]u8 = undefined;

    try std.testing.expectEqual(@as(usize, 4), try lr.reader().read(&buf));
    try std.testing.expectEqualStrings("abcd", buf[0..4]);
    try std.testing.expectEqual(@as(usize, 0), try lr.reader().read(&buf));
    try std.testing.expectEqual(@as(u64, 0), lr.bytesLeft());
}

test "LimitReader: source EOF before limit returns 0 cleanly" {
    var sr = SliceReader{ .bytes = "ab" };
    var lr = LimitReader.init(sr.reader(), 100);
    var buf: [16]u8 = undefined;

    try std.testing.expectEqual(@as(usize, 2), try lr.reader().read(&buf));
    try std.testing.expectEqual(@as(usize, 0), try lr.reader().read(&buf));
}
