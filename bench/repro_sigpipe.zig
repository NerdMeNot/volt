//! Regression guard — `Runtime.init` must ignore SIGPIPE.
//!
//! Writing to a socket whose peer has closed raises SIGPIPE; its
//! default action terminates the process. Volt's socket I/O uses raw
//! `write(2)` (no MSG_NOSIGNAL) and Linux has no per-socket
//! SO_NOSIGPIPE, so `Runtime.init` installs a process-wide
//! `SIG_IGN(SIGPIPE)` (see `signal.ignoreSigpipe`).
//!
//! This is a STANDALONE executable, not a `zig test` test, on purpose:
//! the Zig test runner installs its own SIGPIPE handler that masks the
//! crash, so an in-suite test would pass vacuously. A standalone
//! process starts at the default disposition (SIG_DFL), so the bug is
//! observable here — without the fix this exits 141 (128 + SIGPIPE);
//! with it the writes return -1/EPIPE and we exit 0.
//!
//! Run: `zig build bench-repro-sigpipe`.

const std = @import("std");
const volt = @import("volt");

extern "c" fn socketpair(domain: c_int, typ: c_int, proto: c_int, sv: *[2]c_int) c_int;
extern "c" fn close(fd: c_int) c_int;
extern "c" fn sigaction(sig: c_int, act: ?*const anyopaque, old: ?*anyopaque) c_int;
const c_write = @extern(*const fn (c_int, [*]const u8, usize) callconv(.c) isize, .{ .name = "write" });

const AF_UNIX: c_int = 1; // identical on Linux + Darwin
const SOCK_STREAM: c_int = 1;
const SIGPIPE: c_int = 13;

fn sigpipeHandlerPtr() usize {
    var old: [256]u8 = @splat(0);
    _ = sigaction(SIGPIPE, null, &old);
    return std.mem.readInt(usize, old[0..8], .little);
}

pub fn main() !void {
    const before = sigpipeHandlerPtr();
    std.debug.print("SIGPIPE disposition before Runtime.init = {d} (0=SIG_DFL)\n", .{before});

    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator, .workers = 1 });
    defer rt.deinit();

    const after = sigpipeHandlerPtr();
    std.debug.print("SIGPIPE disposition after  Runtime.init = {d} (1=SIG_IGN)\n", .{after});

    // Connected pair, close the peer, then write. The default-
    // disposition process dies on SIGPIPE here without the fix.
    var sv: [2]c_int = undefined;
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, &sv) != 0) return error.SocketpairFailed;
    _ = close(sv[1]);
    const buf = [_]u8{0xAB} ** 64;
    var got_epipe = false;
    var i: u32 = 0;
    while (i < 8) : (i += 1) {
        if (c_write(sv[0], &buf, buf.len) < 0) got_epipe = true;
    }
    _ = close(sv[0]);

    if (!got_epipe) {
        std.debug.print("FAIL: writes never errored — did not exercise the broken-pipe path\n", .{});
        std.process.exit(1);
    }
    std.debug.print("PASS — survived write-to-closed-peer; EPIPE returned, no SIGPIPE kill\n", .{});
}
