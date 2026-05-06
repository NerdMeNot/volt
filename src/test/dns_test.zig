//! P2.E — DNS resolution.
//!
//! Tests use `localhost` and numeric literals so they work offline.
//! Real-network resolution (`example.com` etc.) is intentionally
//! skipped — flakes in CI environments without DNS / behind
//! proxies.

const std = @import("std");
const volt = @import("../lib.zig");

fn lookupNumericRoot() !usize {
    const list = try volt.net.dns.lookupHost(std.testing.allocator, "127.0.0.1", 8080);
    defer std.testing.allocator.free(list);
    // Numeric literal — getaddrinfo with AI_NUMERICHOST-style behaviour
    // returns exactly one match.
    return list.len;
}

test "P2.E: dns.lookupHost numeric IPv4 literal" {
    const n = try volt.run(.{ .allocator = std.testing.allocator }, lookupNumericRoot, .{});
    try std.testing.expect(n >= 1);
}

fn lookupLocalhostRoot() !usize {
    // Most systems resolve "localhost" via /etc/hosts → 127.0.0.1
    // (and ::1 if IPv6 is enabled). At least one address expected.
    const list = try volt.net.dns.lookupHost(std.testing.allocator, "localhost", 80);
    defer std.testing.allocator.free(list);
    return list.len;
}

test "P2.E: dns.lookupHost localhost" {
    const n = try volt.run(.{ .allocator = std.testing.allocator }, lookupLocalhostRoot, .{});
    try std.testing.expect(n >= 1);
}

fn lookupFirstRoot() !?volt.net.Address {
    return volt.net.dns.lookupHostFirst(std.testing.allocator, "127.0.0.1", 443);
}

test "P2.E: dns.lookupHostFirst returns the first match" {
    const addr = try volt.run(.{ .allocator = std.testing.allocator }, lookupFirstRoot, .{});
    try std.testing.expect(addr != null);
    try std.testing.expectEqual(@as(u16, 443), addr.?.getPort());
}
