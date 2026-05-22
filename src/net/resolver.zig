//! DNS resolver — name to address lookup.
//!
//! Bridges libc `getaddrinfo` through `volt.spawnBlocking` so the
//! calling coroutine parks (on an OS thread in the blocking pool)
//! while the sync resolution runs. The result — an owned
//! `[]Address` — comes back to the calling coro on completion.
//!
//! ## Scope
//!
//! v1 ships sync `getaddrinfo` bridged through the blocking pool.
//! That's the realistic shape: name resolution rarely needs
//! microsecond latency, and a coroutine parked on the blocking
//! pool keeps the runtime workers free to dispatch other work.
//!
//! A true async resolver (UDP DNS protocol + caching + TCP
//! fallback + DNSSEC) is genuinely a multi-week effort — deferred
//! to v1.x; this is the path real services start with.
//!
//! ## API
//!
//! ```zig
//! const addrs = try volt.net.lookupHost(allocator, "example.com", 443);
//! defer allocator.free(addrs);
//! for (addrs) |addr| { ... }
//!
//! const a = try volt.net.lookupHostFirst(allocator, "example.com", 443);
//! var stream = try volt.net.TcpStream.connect(a);
//! ```

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const lib = @import("../lib.zig");
const address_mod = @import("address.zig");

const Address = address_mod.Address;

// ─── libc bindings for getaddrinfo ──────────────────────────────────

// `struct addrinfo` per POSIX. Field order matches Linux/Darwin/BSD;
// Windows mostly matches but the `ai_canonname` and `ai_addr` come
// in the opposite order — we'd #ifdef but for v1 the POSIX layout
// is what we need (POSIX-only deployment until Windows-validated).
const addrinfo = extern struct {
    ai_flags: c_int,
    ai_family: c_int,
    ai_socktype: c_int,
    ai_protocol: c_int,
    ai_addrlen: posix.socklen_t,
    // Darwin / BSD: ai_canonname then ai_addr
    // Linux: ai_addr then ai_canonname
    // We dispatch via comptime so accesses work cross-platform.
    a: usize,
    b: usize,
    ai_next: ?*addrinfo,

    inline fn addr(self: *const addrinfo) ?*posix.sockaddr {
        if (builtin.os.tag == .linux) {
            return @ptrFromInt(self.a);
        }
        return @ptrFromInt(self.b);
    }
};

const c_getaddrinfo = @extern(
    *const fn (?[*:0]const u8, ?[*:0]const u8, ?*const addrinfo, *?*addrinfo) callconv(.c) c_int,
    .{ .name = "getaddrinfo" },
);

const c_freeaddrinfo = @extern(
    *const fn (?*addrinfo) callconv(.c) void,
    .{ .name = "freeaddrinfo" },
);

// AF_UNSPEC = 0 — return both IPv4 and IPv6 results, kernel order.
const AF_UNSPEC: c_int = 0;
const AI_NUMERICSERV: c_int = switch (builtin.os.tag) {
    .linux => 0x0400,
    .windows => 0x00000008,
    else => 0x1000, // Darwin / BSD
};

// ─── Public API ─────────────────────────────────────────────────────

pub const LookupError = error{
    NameNotFound, // EAI_NONAME / EAI_NODATA
    TemporaryFailure, // EAI_AGAIN — try again
    BadService, // EAI_SERVICE
    SystemError, // EAI_SYSTEM
    OutOfMemory, // EAI_MEMORY or allocator failure
    Unexpected,
};

/// Resolve `host` (a domain name, IPv4, or IPv6 literal) plus
/// `port` to one or more `Address` values via libc `getaddrinfo`.
/// Returns an owned slice — caller frees with the same allocator.
///
/// Runs on the runtime's blocking pool (via `volt.spawnBlocking`)
/// so the calling coroutine parks rather than blocking a worker
/// thread. Must be called from inside a coroutine.
pub fn lookupHost(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
) !([]Address) {
    const Input = struct {
        allocator: std.mem.Allocator,
        host: []const u8,
        port: u16,
    };
    const Output = ResolveResult;
    const Resolver = struct {
        fn run(input: Input) Output {
            return doResolveSync(input.allocator, input.host, input.port);
        }
    };
    const input = Input{ .allocator = allocator, .host = host, .port = port };
    const out = try lib.spawnBlocking(Resolver.run, .{input});
    return switch (out) {
        .ok => |slice| slice,
        .err => |e| e,
    };
}

/// Convenience: return the first resolved address. The error union
/// includes `error.NameNotFound` for empty results.
pub fn lookupHostFirst(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
) !Address {
    const addrs = try lookupHost(allocator, host, port);
    defer allocator.free(addrs);
    if (addrs.len == 0) return error.NameNotFound;
    return addrs[0];
}

/// Resolve + connect — common shape for "open a TCP stream to a
/// hostname." Tries addresses in resolver-returned order until one
/// connects; returns the first success or the last error.
///
/// (Happy Eyeballs / IPv4-IPv6 race fallback is a v1.x stretch.)
pub fn connectHost(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
) !@import("../net.zig").TcpStream {
    const TcpStream = @import("../net.zig").TcpStream;
    const addrs = try lookupHost(allocator, host, port);
    defer allocator.free(addrs);
    var last_err: anyerror = error.NameNotFound;
    for (addrs) |addr| {
        if (TcpStream.connect(addr)) |s| return s else |e| {
            last_err = e;
        }
    }
    return last_err;
}

// ─── Internal — resolution on the blocking thread ───────────────────

const ResolveResult = union(enum) {
    ok: []Address,
    err: LookupError,
};

fn doResolveSync(allocator: std.mem.Allocator, host: []const u8, port: u16) ResolveResult {
    // getaddrinfo wants nul-terminated strings. We dup into stack
    // buffers; host names are short (RFC 1035 max 255 bytes) and
    // ports are ≤ 5 chars + nul.
    var host_buf: [256]u8 = undefined;
    if (host.len >= host_buf.len) return .{ .err = error.NameNotFound };
    @memcpy(host_buf[0..host.len], host);
    host_buf[host.len] = 0;

    var port_buf: [8]u8 = undefined;
    const port_str = std.fmt.bufPrint(&port_buf, "{d}", .{port}) catch
        return .{ .err = error.BadService };
    if (port_str.len >= port_buf.len) return .{ .err = error.BadService };
    port_buf[port_str.len] = 0;

    var hints = std.mem.zeroes(addrinfo);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = 1; // SOCK_STREAM
    hints.ai_flags = AI_NUMERICSERV;

    var res: ?*addrinfo = null;
    const rc = c_getaddrinfo(@ptrCast(&host_buf[0]), @ptrCast(&port_buf[0]), &hints, &res);
    if (rc != 0) {
        return .{ .err = mapEaiError(rc) };
    }
    defer c_freeaddrinfo(res);

    // Count results.
    var count: usize = 0;
    var cur = res;
    while (cur) |ai| : (cur = ai.ai_next) count += 1;

    if (count == 0) return .{ .err = error.NameNotFound };

    const out = allocator.alloc(Address, count) catch return .{ .err = error.OutOfMemory };
    var i: usize = 0;
    cur = res;
    while (cur) |ai| : (cur = ai.ai_next) {
        if (ai.addr()) |sa| {
            out[i] = Address.fromSockaddr(sa, ai.ai_addrlen);
            i += 1;
        }
    }
    return .{ .ok = out[0..i] };
}

fn mapEaiError(rc: c_int) LookupError {
    // EAI_* codes — POSIX standard set. Negative on Linux,
    // positive on Darwin / BSD; we accept both. Common subset:
    //   EAI_NONAME    -2 (Linux) / 8 (Darwin)
    //   EAI_AGAIN     -3 (Linux) / 2 (Darwin)
    //   EAI_SERVICE   -8 (Linux) / 9 (Darwin)
    //   EAI_MEMORY    -10 / 6
    //   EAI_SYSTEM    -11 / 11
    const code = if (rc < 0) -rc else rc;
    return switch (code) {
        2, 3 => error.TemporaryFailure,
        6, 10 => error.OutOfMemory,
        8 => error.NameNotFound,
        9 => error.BadService,
        11 => error.SystemError,
        else => error.Unexpected,
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const test_alloc = @import("../testing.zig").allocator;

fn lookupLocalhostRoot() !void {
    const addrs = try lookupHost(test_alloc, "localhost", 80);
    defer test_alloc.free(addrs);
    try testing.expect(addrs.len > 0);
    // First resolved address should be loopback (either v4 or v6).
    try testing.expect(addrs[0].isLoopback());
    try testing.expectEqual(@as(u16, 80), addrs[0].port());
}

test "Resolver: lookupHost(\"localhost\", 80) returns a loopback address" {
    var rt = try lib.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(lookupLocalhostRoot, .{}));
}

fn lookupBogusRoot() !void {
    const r = lookupHost(test_alloc, "this.host.absolutely.does.not.exist.invalid", 80);
    // Either NameNotFound (most environments) or TemporaryFailure
    // (intermittent DNS): both are valid "no such host" signals.
    if (r) |slice| {
        test_alloc.free(slice);
        return error.UnexpectedlySucceeded;
    } else |e| switch (e) {
        error.NameNotFound, error.TemporaryFailure => {},
        else => return e,
    }
}

test "Resolver: lookupHost on bogus name surfaces NameNotFound" {
    var rt = try lib.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(lookupBogusRoot, .{}));
}

fn lookupFirstRoot() !void {
    const addr = try lookupHostFirst(test_alloc, "127.0.0.1", 8080);
    try testing.expect(addr.isLoopback());
    try testing.expectEqual(@as(u16, 8080), addr.port());
}

test "Resolver: lookupHostFirst on IP literal returns that address" {
    var rt = try lib.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(lookupFirstRoot, .{}));
}
