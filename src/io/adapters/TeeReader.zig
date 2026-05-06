//! `volt.io.TeeReader` — read from a source, mirror to a writer.
//!
//! On every `read`, the bytes returned to the caller are also
//! `writeAll`'d to a side writer. Common uses: hashing-while-reading
//! (the side writer is a hasher's writer interface), traffic capture,
//! request-body audit logging.
//!
//! ## Best-effort mirror
//!
//! Mirror failures DO NOT fail the primary read. The TeeReader stores
//! the most recent mirror error in `mirror_error`; callers who care
//! poll `mirrorError()` post-read to observe it. Rationale: dominant
//! use cases (hashing, audit) have non-failing mirrors, and when the
//! mirror does fail (out of disk on the audit log) the user wants
//! their primary I/O to continue, not stall. This is the behaviour
//! Tokio's stream APIs lean toward; Go's `io.TeeReader` couples them
//! because Go's untyped errors hide the awkwardness — we have an
//! explicit error union, so the asymmetry is honest here.
//!
//! Each successful `read` clears `mirror_error` for the call (it only
//! ever holds the *last* mirror failure, not a history).

const std = @import("std");
const traits = @import("../traits/traits.zig");
const Reader = traits.Reader;
const Writer = traits.Writer;
const ReadError = traits.ReadError;
const WriteError = traits.WriteError;

pub const TeeReader = struct {
    source: Reader,
    mirror: Writer,
    /// Most recent mirror failure, or null. Set on each `read`; the
    /// primary read returns its own success value regardless.
    mirror_error: ?WriteError = null,

    pub fn init(source: Reader, mirror: Writer) TeeReader {
        return .{ .source = source, .mirror = mirror };
    }

    pub fn reader(self: *TeeReader) Reader {
        return .{ .ctx = @ptrCast(self), .vtable = &vtable };
    }

    /// Returns the most recent mirror failure (if any). Cleared on the
    /// next successful mirror write.
    pub fn mirrorError(self: *const TeeReader) ?WriteError {
        return self.mirror_error;
    }
};

const vtable: Reader.VTable = .{ .read = &vtableRead };

fn vtableRead(ctx: *anyopaque, buf: []u8) ReadError!usize {
    const self: *TeeReader = @ptrCast(@alignCast(ctx));
    const got = try self.source.read(buf);
    if (got == 0) return 0;
    if (self.mirror.writeAll(buf[0..got])) {
        self.mirror_error = null;
    } else |err| {
        self.mirror_error = err;
    }
    return got;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const SliceReader = traits.SliceReader;
const BufferWriter = traits.BufferWriter;

test "TeeReader: caller and mirror see the same bytes" {
    var sr = SliceReader{ .bytes = "abcdef" };
    var mirror_buf: [16]u8 = undefined;
    var mirror = BufferWriter{ .buf = &mirror_buf };

    var tee = TeeReader.init(sr.reader(), mirror.writer());

    var buf: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 4), try tee.reader().read(&buf));
    try std.testing.expectEqualStrings("abcd", &buf);
    try std.testing.expectEqualStrings("abcd", mirror.written());

    try std.testing.expectEqual(@as(usize, 2), try tee.reader().read(&buf));
    try std.testing.expectEqualStrings("ef", buf[0..2]);
    try std.testing.expectEqualStrings("abcdef", mirror.written());

    try std.testing.expectEqual(@as(?WriteError, null), tee.mirrorError());
}

test "TeeReader: EOF doesn't write to mirror" {
    var sr = SliceReader{ .bytes = "" };
    var mirror_buf: [16]u8 = undefined;
    var mirror = BufferWriter{ .buf = &mirror_buf };

    var tee = TeeReader.init(sr.reader(), mirror.writer());
    var buf: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try tee.reader().read(&buf));
    try std.testing.expectEqual(@as(usize, 0), mirror.pos);
}

test "TeeReader: mirror failure doesn't fail the read; surfaced via mirrorError" {
    var sr = SliceReader{ .bytes = "alpha" };
    // Tiny mirror buffer — overflows on the first write.
    var mirror_buf: [2]u8 = undefined;
    var mirror = BufferWriter{ .buf = &mirror_buf };

    var tee = TeeReader.init(sr.reader(), mirror.writer());
    var buf: [5]u8 = undefined;
    // Primary read still returns the bytes — full success despite mirror error.
    try std.testing.expectEqual(@as(usize, 5), try tee.reader().read(&buf));
    try std.testing.expectEqualStrings("alpha", &buf);
    // Mirror error captured.
    try std.testing.expect(tee.mirrorError() != null);
    try std.testing.expectEqual(WriteError.NoSpaceLeft, tee.mirrorError().?);
}
