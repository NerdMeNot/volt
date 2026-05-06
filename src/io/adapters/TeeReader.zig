//! `volt.io.TeeReader` — read from a source, mirror to a writer.
//!
//! On every `read`, the bytes returned to the caller are also
//! `writeAll`'d to a side writer. Common uses: hashing-while-reading
//! (the side writer is a hasher's writer interface), traffic capture,
//! request-body audit logging.

const std = @import("std");
const traits = @import("../traits/traits.zig");
const Reader = traits.Reader;
const Writer = traits.Writer;
const ReadError = traits.ReadError;

pub const TeeReader = struct {
    source: Reader,
    mirror: Writer,

    pub fn init(source: Reader, mirror: Writer) TeeReader {
        return .{ .source = source, .mirror = mirror };
    }

    pub fn reader(self: *TeeReader) Reader {
        return .{ .ctx = @ptrCast(self), .vtable = &vtable };
    }
};

const vtable: Reader.VTable = .{ .read = &vtableRead };

fn vtableRead(ctx: *anyopaque, buf: []u8) ReadError!usize {
    const self: *TeeReader = @ptrCast(@alignCast(ctx));
    const got = try self.source.read(buf);
    if (got == 0) return 0;
    // Mirror errors propagate as ReadErrors. Callers that don't want
    // a flaky mirror to block their read can wrap the mirror in a
    // best-effort adapter; we're strict by default.
    self.mirror.writeAll(buf[0..got]) catch |err| return mirrorErrorToRead(err);
    return got;
}

fn mirrorErrorToRead(err: traits.WriteError) ReadError {
    return switch (err) {
        error.WouldBlock => error.WouldBlock,
        error.Interrupted => error.Interrupted,
        error.BrokenPipe => error.BrokenPipe,
        error.ConnectionResetByPeer => error.ConnectionResetByPeer,
        error.InputOutput => error.InputOutput,
        error.AccessDenied => error.AccessDenied,
        error.Cancelled => error.Cancelled,
        error.OutOfMemory => error.OutOfMemory,
        error.SystemResources => error.SystemResources,
        error.WaitRegistrationFailed => error.WaitRegistrationFailed,
        else => error.Unexpected,
    };
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
