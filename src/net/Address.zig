//! `volt.net.Address` — IPv4 / IPv6 socket address.
//!
//! Self-contained `extern union` over `posix.sockaddr_in` / `_in6` —
//! no `std.Io` dependency since `std.Io` requires a runtime, which is
//! what we *are*. Naming mirrors `std.net.Address` so users coming
//! from the std API reach for the right ctor.
//!
//! ## IPv6 parser
//!
//! Implements RFC 5952 zero-compression (`::`) plus full hex groups.
//! Limits documented below; not yet supported:
//!   - **IPv4-mapped addresses** like `::ffff:192.0.2.1`. Parser
//!     treats them as InvalidIPv6 today; planned for P2.B.
//!   - **Scope IDs** like `fe80::1%en0`. Returns `InvalidIPv6` if the
//!     `%` separator is present; planned alongside `if_nametoindex`
//!     wiring.

const std = @import("std");
const posix = std.posix;

/// IPv4 / IPv6 socket address.
pub const Address = extern union {
    any: posix.sockaddr,
    in: posix.sockaddr.in,
    in6: posix.sockaddr.in6,

    /// Construct an IPv4 address from a 4-byte octet array + port.
    pub fn initIp4(octets: [4]u8, port: u16) Address {
        // sockaddr_in.addr is a 4-byte field in network byte order.
        // bytesToValue preserves in-memory byte order, so a.b.c.d on
        // the wire stays a.b.c.d.
        return .{ .in = .{
            .family = posix.AF.INET,
            .port = std.mem.nativeToBig(u16, port),
            .addr = std.mem.bytesToValue(u32, &octets),
            .zero = .{0} ** 8,
        } };
    }

    /// Construct an IPv6 address from a 16-byte array + port.
    pub fn initIp6(bytes: [16]u8, port: u16) Address {
        return .{ .in6 = .{
            .family = posix.AF.INET6,
            .port = std.mem.nativeToBig(u16, port),
            .flowinfo = 0,
            .addr = bytes,
            .scope_id = 0,
        } };
    }

    /// Loopback IPv4 (127.0.0.1).
    pub fn loopback4(port: u16) Address {
        return initIp4(.{ 127, 0, 0, 1 }, port);
    }

    /// Loopback IPv6 (::1).
    pub fn loopback6(port: u16) Address {
        var b: [16]u8 = .{0} ** 16;
        b[15] = 1;
        return initIp6(b, port);
    }

    /// Wildcard IPv4 (0.0.0.0).
    pub fn any4(port: u16) Address {
        return initIp4(.{ 0, 0, 0, 0 }, port);
    }

    /// Wildcard IPv6 (::).
    pub fn any6(port: u16) Address {
        return initIp6(.{0} ** 16, port);
    }

    /// Parse a dotted-quad IPv4 string.
    pub fn parseIp4(str: []const u8, port: u16) error{InvalidIPv4}!Address {
        var octets: [4]u8 = undefined;
        var i: usize = 0;
        var it = std.mem.splitScalar(u8, str, '.');
        while (it.next()) |part| : (i += 1) {
            if (i >= 4) return error.InvalidIPv4;
            octets[i] = std.fmt.parseInt(u8, part, 10) catch return error.InvalidIPv4;
        }
        if (i != 4) return error.InvalidIPv4;
        return initIp4(octets, port);
    }

    /// Parse an IPv6 text address per RFC 5952. Supports zero-
    /// compression (`::`) and standard hex groups; rejects scope IDs
    /// (`%`) and IPv4-mapped (`::ffff:192.0.2.1`) for now (see file
    /// header).
    pub fn parseIp6(str: []const u8, port: u16) error{InvalidIPv6}!Address {
        if (str.len == 0) return error.InvalidIPv6;
        // Scope IDs not yet supported.
        if (std.mem.indexOfScalar(u8, str, '%') != null) return error.InvalidIPv6;
        // Disallow embedded dots — IPv4-mapped is not yet supported,
        // and rejecting up-front gives a clean error.
        if (std.mem.indexOfScalar(u8, str, '.') != null) return error.InvalidIPv6;

        // Find the optional `::` zero-compression marker. At most one.
        var has_double_colon = false;
        var double_colon_pos: usize = 0;
        if (std.mem.indexOf(u8, str, "::")) |idx| {
            if (std.mem.indexOf(u8, str[idx + 2 ..], "::") != null) return error.InvalidIPv6;
            has_double_colon = true;
            double_colon_pos = idx;
        }

        const left_str = if (has_double_colon) str[0..double_colon_pos] else str;
        const right_str = if (has_double_colon) str[double_colon_pos + 2 ..] else "";

        var left_groups: [8]u16 = undefined;
        var left_count: usize = 0;
        var right_groups: [8]u16 = undefined;
        var right_count: usize = 0;

        if (left_str.len > 0) {
            var it = std.mem.splitScalar(u8, left_str, ':');
            while (it.next()) |group| {
                if (left_count >= 8) return error.InvalidIPv6;
                if (group.len == 0 or group.len > 4) return error.InvalidIPv6;
                left_groups[left_count] = std.fmt.parseInt(u16, group, 16) catch return error.InvalidIPv6;
                left_count += 1;
            }
        }

        if (right_str.len > 0) {
            var it = std.mem.splitScalar(u8, right_str, ':');
            while (it.next()) |group| {
                if (right_count >= 8) return error.InvalidIPv6;
                if (group.len == 0 or group.len > 4) return error.InvalidIPv6;
                right_groups[right_count] = std.fmt.parseInt(u16, group, 16) catch return error.InvalidIPv6;
                right_count += 1;
            }
        }

        // Without compression, all 8 groups must be present. With
        // compression, RFC 4291 requires `::` to replace AT LEAST one
        // group of zeros — so left + right must leave room for at
        // least one zero group (sum ≤ 7).
        if (!has_double_colon) {
            if (left_count != 8) return error.InvalidIPv6;
        } else {
            if (left_count + right_count > 7) return error.InvalidIPv6;
        }

        var groups: [8]u16 = .{0} ** 8;
        var i: usize = 0;
        while (i < left_count) : (i += 1) groups[i] = left_groups[i];
        const right_start = 8 - right_count;
        i = 0;
        while (i < right_count) : (i += 1) groups[right_start + i] = right_groups[i];

        // Network byte order: each u16 group is big-endian on the wire.
        var bytes: [16]u8 = undefined;
        for (groups, 0..) |g, idx| {
            bytes[idx * 2] = @intCast(g >> 8);
            bytes[idx * 2 + 1] = @intCast(g & 0xff);
        }
        return initIp6(bytes, port);
    }

    /// Parse a `host:port` string. IPv4 is bare (`a.b.c.d:port`);
    /// IPv6 must be bracketed (`[2001:db8::1]:8080`). Returns
    /// `error.InvalidAddress` on any failure.
    pub fn parse(str: []const u8) error{InvalidAddress}!Address {
        // IPv6: [host]:port
        if (str.len > 0 and str[0] == '[') {
            const close = std.mem.indexOfScalar(u8, str, ']') orelse return error.InvalidAddress;
            if (close + 1 >= str.len or str[close + 1] != ':') return error.InvalidAddress;
            const host = str[1..close];
            const port_str = str[close + 2 ..];
            const port = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidAddress;
            return parseIp6(host, port) catch error.InvalidAddress;
        }
        // IPv4: a.b.c.d:port
        const colon = std.mem.lastIndexOfScalar(u8, str, ':') orelse return error.InvalidAddress;
        const host = str[0..colon];
        const port_str = str[colon + 1 ..];
        const port = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidAddress;
        return parseIp4(host, port) catch error.InvalidAddress;
    }

    /// Address family (`AF_INET` / `AF_INET6`).
    pub fn family(self: *const Address) u16 {
        return self.any.family;
    }

    /// Length to pass to bind/connect/accept.
    pub fn osSockLen(self: *const Address) posix.socklen_t {
        return switch (self.any.family) {
            posix.AF.INET => @sizeOf(posix.sockaddr.in),
            posix.AF.INET6 => @sizeOf(posix.sockaddr.in6),
            else => @panic("Address.osSockLen: unsupported family — only AF_INET / AF_INET6"),
        };
    }

    /// Port in native byte order.
    pub fn getPort(self: *const Address) u16 {
        return switch (self.any.family) {
            posix.AF.INET => std.mem.bigToNative(u16, self.in.port),
            posix.AF.INET6 => std.mem.bigToNative(u16, self.in6.port),
            else => @panic("Address.getPort: unsupported family"),
        };
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests — IPv4 + IPv6 parsing, edge cases.
// ─────────────────────────────────────────────────────────────────────

test "Address.parse: ipv4 + port" {
    const a = try Address.parse("127.0.0.1:8080");
    try std.testing.expectEqual(@as(u16, 8080), a.getPort());
    try std.testing.expectEqual(@as(u16, posix.AF.INET), a.family());
}

test "Address.parse: ipv6 loopback bracketed" {
    const a = try Address.parse("[::1]:9090");
    try std.testing.expectEqual(@as(u16, 9090), a.getPort());
    try std.testing.expectEqual(@as(u16, posix.AF.INET6), a.family());
}

test "Address.parse: rejects garbage" {
    try std.testing.expectError(error.InvalidAddress, Address.parse("not-an-address"));
    try std.testing.expectError(error.InvalidAddress, Address.parse("256.0.0.1:1"));
}

test "Address.any4/loopback4 round-trip port" {
    try std.testing.expectEqual(@as(u16, 80), Address.any4(80).getPort());
    try std.testing.expectEqual(@as(u16, 81), Address.loopback4(81).getPort());
}

test "Address.initIp4 round-trip" {
    const addr = Address.initIp4(.{ 192, 168, 1, 50 }, 8080);
    try std.testing.expectEqual(@as(u16, posix.AF.INET), addr.family());
    try std.testing.expectEqual(@as(u16, 8080), addr.getPort());
}

// ── IPv6 parser ──────────────────────────────────────────────────────

fn expectIp6Bytes(str: []const u8, expected: [16]u8) !void {
    const a = try Address.parseIp6(str, 0);
    try std.testing.expectEqual(@as(u16, posix.AF.INET6), a.family());
    try std.testing.expectEqualSlices(u8, &expected, &a.in6.addr);
}

test "parseIp6: loopback ::1" {
    try expectIp6Bytes("::1", .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 });
}

test "parseIp6: wildcard ::" {
    try expectIp6Bytes("::", .{0} ** 16);
}

test "parseIp6: link-local fe80::1" {
    try expectIp6Bytes("fe80::1", .{ 0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 });
}

test "parseIp6: documentation 2001:db8::1" {
    try expectIp6Bytes("2001:db8::1", .{ 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 });
}

test "parseIp6: trailing zero-compress 1::" {
    try expectIp6Bytes("1::", .{ 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
}

test "parseIp6: full eight groups, no compression" {
    try expectIp6Bytes(
        "2001:db8:85a3:0:0:8a2e:370:7334",
        .{ 0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0, 0, 0, 0, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34 },
    );
}

test "parseIp6: rejects multiple ::" {
    try std.testing.expectError(error.InvalidIPv6, Address.parseIp6("1::2::3", 0));
}

test "parseIp6: rejects empty string" {
    try std.testing.expectError(error.InvalidIPv6, Address.parseIp6("", 0));
}

test "parseIp6: rejects > 4 hex digits in a group" {
    try std.testing.expectError(error.InvalidIPv6, Address.parseIp6("12345::1", 0));
}

test "parseIp6: rejects nine groups without compression" {
    try std.testing.expectError(error.InvalidIPv6, Address.parseIp6("1:2:3:4:5:6:7:8:9", 0));
}

test "parseIp6: rejects too many groups even with compression" {
    // "1:2:3:4:5:6:7::8" has 8 explicit groups + one zero-fill = 9 → error
    try std.testing.expectError(error.InvalidIPv6, Address.parseIp6("1:2:3:4:5:6:7::8", 0));
}

test "parseIp6: rejects single-colon at start" {
    try std.testing.expectError(error.InvalidIPv6, Address.parseIp6(":1", 0));
}

test "parseIp6: rejects IPv4-mapped (deferred to P2.B)" {
    try std.testing.expectError(error.InvalidIPv6, Address.parseIp6("::ffff:192.0.2.1", 0));
}

test "parseIp6: rejects scope IDs (deferred)" {
    try std.testing.expectError(error.InvalidIPv6, Address.parseIp6("fe80::1%en0", 0));
}

test "Address.parse: bracketed full IPv6" {
    const a = try Address.parse("[2001:db8::1]:443");
    try std.testing.expectEqual(@as(u16, 443), a.getPort());
    try std.testing.expectEqual(@as(u16, posix.AF.INET6), a.family());
}
