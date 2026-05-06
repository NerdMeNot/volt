//! `volt.net.dns` — DNS resolution.
//!
//! ## Honest about the architecture
//!
//! `lookupHost` calls libc's `getaddrinfo` on the **blocking pool**.
//! It is not async DNS — the pool thread blocks on the resolver
//! while the calling coroutine parks. Tokio, Trio, and .NET all
//! ship the same shape because:
//!
//!   - `getaddrinfo` is the only universally available system
//!     resolver, and it's blocking-only.
//!   - glibc's `getaddrinfo_a` (async variant) is broken in known
//!     ways and not portable.
//!   - A non-blocking resolver requires either statically linking
//!     a C library (c-ares) or hand-rolling the full DNS protocol.
//!
//! For a real async DNS resolver — pick a c-ares wrapper or write
//! one — defer to post-v2.0 unless workloads actually need it.
//!
//! What you get from this module:
//!   - The calling coroutine doesn't block its worker thread (the
//!     resolution runs on the blocking pool).
//!   - You DO pay a thread-pool dispatch + handoff per call.
//!   - Resolution latency is bounded by the system resolver, NOT by
//!     Volt's reactor.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const Address = @import("Address.zig").Address;
const spawnBlocking = @import("../api/spawn_blocking.zig").spawnBlocking;

pub const DnsError = error{
    /// The name didn't resolve to any address.
    HostNotFound,
    /// A temporary name-server failure — try again later.
    TemporaryFailure,
    /// The service name (port string) couldn't be resolved.
    ServiceNotSupported,
    /// A non-recoverable name-server error.
    NameServerFailure,
    OutOfMemory,
    Unexpected,
};

const LookupArgs = struct {
    allocator: std.mem.Allocator,
    name: []const u8,
    port: u16,
};

/// Resolve `name` (hostname or numeric IP literal) to one or more
/// `Address` values, all bound to `port`. Caller owns the returned
/// slice and must free with `allocator`. Returns
/// `error.HostNotFound` for an empty result; never returns an
/// empty slice.
pub fn lookupHost(allocator: std.mem.Allocator, name: []const u8, port: u16) DnsError![]Address {
    var args = LookupArgs{ .allocator = allocator, .name = name, .port = port };
    return spawnBlocking(lookupBlocking, .{&args}) catch |err| switch (err) {
        error.HostNotFound => error.HostNotFound,
        error.TemporaryFailure => error.TemporaryFailure,
        error.ServiceNotSupported => error.ServiceNotSupported,
        error.NameServerFailure => error.NameServerFailure,
        error.OutOfMemory => error.OutOfMemory,
        else => error.Unexpected,
    };
}

/// Convenience: resolve and return the first address. `null` if no
/// matches; errors propagated.
pub fn lookupHostFirst(allocator: std.mem.Allocator, name: []const u8, port: u16) DnsError!?Address {
    const list = lookupHost(allocator, name, port) catch |err| switch (err) {
        error.HostNotFound => return null,
        else => return err,
    };
    defer allocator.free(list);
    if (list.len == 0) return null;
    return list[0];
}

fn lookupBlocking(args: *LookupArgs) DnsError![]Address {
    // Null-terminate the inputs for libc.
    const name_z = args.allocator.dupeZ(u8, args.name) catch return error.OutOfMemory;
    defer args.allocator.free(name_z);

    var port_buf: [6]u8 = undefined;
    const port_z = std.fmt.bufPrintZ(&port_buf, "{d}", .{args.port}) catch unreachable;

    var hints: std.c.addrinfo = std.mem.zeroes(std.c.addrinfo);
    hints.family = posix.AF.UNSPEC;
    hints.socktype = posix.SOCK.STREAM;

    var res: ?*std.c.addrinfo = null;
    const eai = std.c.getaddrinfo(name_z.ptr, port_z.ptr, &hints, &res);
    if (eai != @as(std.c.EAI, @enumFromInt(0))) {
        return mapEai(eai);
    }
    defer if (res) |r| std.c.freeaddrinfo(r);

    // First pass: count IPv4/IPv6 entries.
    var count: usize = 0;
    var cur: ?*std.c.addrinfo = res;
    while (cur) |node| : (cur = node.next) {
        if (node.family == posix.AF.INET or node.family == posix.AF.INET6) count += 1;
    }
    if (count == 0) return error.HostNotFound;

    // Second pass: fill the output slice.
    const out = args.allocator.alloc(Address, count) catch return error.OutOfMemory;
    errdefer args.allocator.free(out);

    var i: usize = 0;
    cur = res;
    while (cur) |node| : (cur = node.next) {
        const addr_ptr = node.addr orelse continue;
        if (node.family == posix.AF.INET) {
            const in_ptr: *const posix.sockaddr.in = @ptrCast(@alignCast(addr_ptr));
            out[i] = .{ .in = in_ptr.* };
            i += 1;
        } else if (node.family == posix.AF.INET6) {
            const in6_ptr: *const posix.sockaddr.in6 = @ptrCast(@alignCast(addr_ptr));
            out[i] = .{ .in6 = in6_ptr.* };
            i += 1;
        }
    }
    return out[0..i];
}

fn mapEai(eai: std.c.EAI) DnsError {
    // EAI codes vary slightly across libc impls; switch on the
    // numeric value to stay portable.
    return switch (@intFromEnum(eai)) {
        // EAI_NONAME / EAI_NODATA — name doesn't resolve.
        @intFromEnum(std.c.EAI.NONAME) => error.HostNotFound,
        @intFromEnum(std.c.EAI.AGAIN) => error.TemporaryFailure,
        @intFromEnum(std.c.EAI.SERVICE) => error.ServiceNotSupported,
        @intFromEnum(std.c.EAI.FAIL) => error.NameServerFailure,
        @intFromEnum(std.c.EAI.MEMORY) => error.OutOfMemory,
        else => error.Unexpected,
    };
}
