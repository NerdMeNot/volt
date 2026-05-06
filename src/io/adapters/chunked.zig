//! `volt.io.chunked` — split a Reader into fixed-size byte chunks.
//!
//! Each `next()` call fills a caller-provided buffer with up to
//! `chunk_size` bytes (less only on the final chunk). Returns the
//! filled prefix length, or `null` on clean EOF. Useful for streaming
//! upload paths (S3 multipart, gRPC framed body), checksumming over
//! fixed boundaries, and any consumer that wants page-aligned reads
//! regardless of how the source delivers them.

const std = @import("std");
const traits = @import("../traits/traits.zig");
const Reader = traits.Reader;
const ReadError = traits.ReadError;

pub const ChunkIterator = struct {
    source: Reader,
    chunk_size: usize,

    /// Fill `dst` with up to `chunk_size` bytes (must hold at least
    /// `chunk_size`). Loops over the source so partial returns are
    /// hidden. Returns the filled byte count; `null` if the source
    /// was at EOF before any byte was read.
    pub fn next(self: *ChunkIterator, dst: []u8) ReadError!?usize {
        std.debug.assert(dst.len >= self.chunk_size);
        var got: usize = 0;
        while (got < self.chunk_size) {
            const n = try self.source.read(dst[got..self.chunk_size]);
            if (n == 0) {
                if (got == 0) return null;
                return got; // short final chunk
            }
            got += n;
        }
        return got;
    }
};

/// Construct a chunk iterator. The iterator does not own `source`.
pub fn chunked(source: Reader, chunk_size: usize) ChunkIterator {
    std.debug.assert(chunk_size > 0);
    return .{ .source = source, .chunk_size = chunk_size };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const SliceReader = traits.SliceReader;

test "chunked: yields full chunks then short tail" {
    var sr = SliceReader{ .bytes = "0123456789abcde" }; // 15 bytes
    var it = chunked(sr.reader(), 4);

    var buf: [4]u8 = undefined;
    try std.testing.expectEqual(@as(?usize, 4), try it.next(&buf));
    try std.testing.expectEqualStrings("0123", &buf);

    try std.testing.expectEqual(@as(?usize, 4), try it.next(&buf));
    try std.testing.expectEqualStrings("4567", &buf);

    try std.testing.expectEqual(@as(?usize, 4), try it.next(&buf));
    try std.testing.expectEqualStrings("89ab", &buf);

    // 15 % 4 = 3 — final short chunk.
    try std.testing.expectEqual(@as(?usize, 3), try it.next(&buf));
    try std.testing.expectEqualStrings("cde", buf[0..3]);

    try std.testing.expectEqual(@as(?usize, null), try it.next(&buf));
}

test "chunked: EOF before any byte returns null" {
    var sr = SliceReader{ .bytes = "" };
    var it = chunked(sr.reader(), 4);
    var buf: [4]u8 = undefined;
    try std.testing.expectEqual(@as(?usize, null), try it.next(&buf));
}
