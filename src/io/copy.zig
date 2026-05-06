//! `volt.io.copy(dst, src)` — blocking copy from a Reader to a Writer.
//!
//! Returns total bytes copied. Stops at EOF on the source or at the
//! first error from either end.
//!
//! ## Dispatch
//!
//! `copy` is the only place in Volt where the trait surface's
//! `as_fd` marker is consulted. The runtime check is:
//!
//!   1. **Fd-to-fd zero-copy** — if both ends expose real fds, ask
//!      the platform-specific helper if a kernel-level zero-copy
//!      applies (`sendfile` / `splice` / `copy_file_range`). A null
//!      return means "specialisation declined" (e.g. cross-device,
//!      EOPNOTSUPP) and we fall through. A non-null return is the
//!      byte count from the kernel.
//!
//!   2. **Classic loop** — read + writeAll over a 64 KiB stack
//!      buffer. Always available; correctness reference for the
//!      fast paths.
//!
//! ## Status: dispatch shape + fallback only
//!
//! P1 ships the dispatch site and the classic loop. The kernel
//! fast-paths land progressively, alongside a concrete use case:
//!   - **P3 (v1.3)** — `sendfile` (Darwin + Linux), `splice` (Linux),
//!     `copy_file_range` (Linux), `clonefile` / `fcopyfile` (Darwin).
//!     File→socket and file→file are the canonical zero-copy
//!     scenarios; deferring until `fs.File` lands keeps the wiring
//!     paired with a real test surface. TCP→TCP can't use sendfile
//!     on Darwin, so doing it without File would be infra without
//!     measurable value.
//!   - **P4 (v1.4)** — `as_bytes` arm (Mmap → writer) once the
//!     consumption semantics are settled alongside Mmap.
//!
//! The marker reservation is intentional: the dispatch shape is the
//! contract. Real consumers (TcpStream, File, Mmap) plug in by
//! filling vtable slots; `copy` doesn't change.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const traits = @import("traits/traits.zig");
const io_errors = @import("errors.zig");

const Reader = traits.Reader;
const Writer = traits.Writer;

/// Errors `copy` can return — union of read + write side. Callers can
/// narrow with a switch over individual member names.
pub const CopyError = io_errors.ReadError || io_errors.WriteError;

/// Copy bytes from `src` to `dst` until `src` reports EOF. Returns
/// the total number of bytes copied.
pub fn copy(dst: Writer, src: Reader) CopyError!u64 {
    // 1. Fd-to-fd kernel zero-copy fast path.
    if (src.vtable.as_fd) |src_fd_fn| {
        if (dst.vtable.as_fd) |dst_fd_fn| {
            if (src_fd_fn(src.ctx)) |src_fd| {
                if (dst_fd_fn(dst.ctx)) |dst_fd| {
                    if (try fastFdCopy(src_fd, dst_fd)) |n| return n;
                }
            }
        }
    }

    // 2. Classic loop fallback.
    return classicCopy(dst, src);
}

fn classicCopy(dst: Writer, src: Reader) CopyError!u64 {
    var buf: [64 * 1024]u8 = undefined;
    var total: u64 = 0;
    while (true) {
        const n = try src.read(&buf);
        if (n == 0) return total;
        try dst.writeAll(buf[0..n]);
        total += n;
    }
}

/// Try a kernel-level zero-copy between two fds. Returns:
///   - `null` — specialisation doesn't apply on this platform / for
///     this fd pair. Caller falls through to the loop.
///   - `u64` — bytes copied.
///   - error — a real I/O error that the loop would also hit;
///     propagate.
///
/// P1 returns null unconditionally — every later phase fills in arms.
fn fastFdCopy(src_fd: posix.fd_t, dst_fd: posix.fd_t) CopyError!?u64 {
    // Suppressed until P2/P3 land platform-specific helpers (sendfile,
    // splice, copy_file_range, clonefile). The dispatch shape is here;
    // the dispatch targets are the only thing left.
    _ = src_fd;
    _ = dst_fd;
    return switch (builtin.os.tag) {
        .linux, .macos, .ios, .freebsd, .netbsd, .dragonfly, .openbsd => null,
        else => null,
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const SliceReader = traits.SliceReader;
const BufferWriter = traits.BufferWriter;

test "copy: classic loop transfers all bytes" {
    var sr = SliceReader{ .bytes = "alpha bravo charlie delta echo" };
    var dst_buf: [64]u8 = undefined;
    var bw = BufferWriter{ .buf = &dst_buf };

    const n = try copy(bw.writer(), sr.reader());
    try std.testing.expectEqual(@as(u64, 30), n);
    try std.testing.expectEqualStrings("alpha bravo charlie delta echo", bw.written());
}

test "copy: empty source returns 0, no writes" {
    var sr = SliceReader{ .bytes = "" };
    var dst_buf: [16]u8 = undefined;
    var bw = BufferWriter{ .buf = &dst_buf };

    try std.testing.expectEqual(@as(u64, 0), try copy(bw.writer(), sr.reader()));
    try std.testing.expectEqual(@as(usize, 0), bw.pos);
}

test "copy: payload larger than internal buffer loops correctly" {
    // 200 KiB — exceeds the 64 KiB stack buffer, forces multiple
    // read+write iterations.
    const big_size = 200 * 1024;
    const big = try std.testing.allocator.alloc(u8, big_size);
    defer std.testing.allocator.free(big);
    for (big, 0..) |*b, i| b.* = @truncate(i);

    var sr = SliceReader{ .bytes = big };
    const dst = try std.testing.allocator.alloc(u8, big_size);
    defer std.testing.allocator.free(dst);
    var bw = BufferWriter{ .buf = dst };

    try std.testing.expectEqual(@as(u64, big_size), try copy(bw.writer(), sr.reader()));
    try std.testing.expectEqualSlices(u8, big, bw.written());
}

test "copy: write error from sink propagates" {
    var sr = SliceReader{ .bytes = "this is more than the dst can hold" };
    var dst_buf: [4]u8 = undefined; // way too small
    var bw = BufferWriter{ .buf = &dst_buf };

    try std.testing.expectError(error.NoSpaceLeft, copy(bw.writer(), sr.reader()));
}
