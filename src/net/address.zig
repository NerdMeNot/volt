//! Socket address — IPv4 and IPv6 with full parsing support.
//!
//! `Address` wraps `posix.sockaddr.storage` (the polymorphic
//! "fits-any-family" struct) plus a `socklen_t` of the actual
//! length. This lets one type cover IPv4 / IPv6 / Unix in a
//! uniform shape; the syscalls (`bind`, `connect`, `getsockname`,
//! etc.) all take `*const sockaddr + socklen_t` pairs that read
//! the family field and dispatch internally.
//!
//! ## Parsing
//!
//! ```zig
//! const a1 = try Address.parse("127.0.0.1:8080");
//! const a2 = try Address.parse("[::1]:8080");
//! const a3 = try Address.parse("[2001:db8::1]:443");
//! const a4 = try Address.parseHost("example.com", 80);  // bare host + default port
//! ```
//!
//! ## Construction
//!
//! ```zig
//! const a1 = Address.loopback4(8080);   // 127.0.0.1:8080
//! const a2 = Address.loopback6(8080);   // [::1]:8080
//! const a3 = Address.any4(8080);        // 0.0.0.0:8080  (listen on all v4 ifaces)
//! const a4 = Address.any6(8080);        // [::]:8080     (listen on all v6 ifaces; dual-stack via opts)
//! const a5 = Address.initIpv4(.{ 192, 168, 1, 1 }, 3000);
//! const a6 = Address.initIpv6(.{ 0x2001, 0xdb8, 0, 0, 0, 0, 0, 1 }, 443);
//! ```
//!
//! ## Accessors
//!
//! `.port`, `.family`, `.isIpv4`, `.isIpv6`, `.isLoopback`,
//! `.isUnspecified`, `.isPrivate`, `.ipv4Octets`, `.ipv6Bytes`,
//! `.format` (for `std.fmt` integration).

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const mem = std.mem;

/// Storage size of platform's `sockaddr.storage` — same as
/// `posix.sockaddr.storage` SS_MAXSIZE on all platforms we ship.
const SS_MAXSIZE = 128;

pub const Address = struct {
    /// Polymorphic storage — holds an in / in6 / un at any time.
    /// `family` field at offset 0 (on Linux) or 1 (Darwin's `len`-
    /// prefixed layout) discriminates.
    storage: posix.sockaddr.storage,
    /// Actual length of the address — `@sizeOf(sockaddr.in)` for
    /// IPv4, `@sizeOf(sockaddr.in6)` for IPv6, etc.
    len: posix.socklen_t,

    pub const ParseError = error{ InvalidAddress, InvalidPort };

    // ─── Parsing ─────────────────────────────────────────────────────

    /// Parse a "host:port" address. Supports:
    ///   * IPv4: "127.0.0.1:8080"
    ///   * IPv6 bracketed: "[::1]:8080", "[2001:db8::1]:443"
    ///   * IPv6 unbracketed without port: "::1" → port 0
    pub fn parse(addr_str: []const u8) ParseError!Address {
        if (addr_str.len == 0) return error.InvalidAddress;

        // IPv6 bracketed: [addr]:port
        if (addr_str[0] == '[') return parseIpv6Bracketed(addr_str);

        if (mem.lastIndexOfScalar(u8, addr_str, ':')) |colon_pos| {
            // Multiple colons = IPv6 without port (we treat port = 0).
            if (mem.indexOfScalar(u8, addr_str[0..colon_pos], ':') != null) {
                return parseIpv6(addr_str, 0);
            }
            const host = addr_str[0..colon_pos];
            const port_str = addr_str[colon_pos + 1 ..];
            const port_num = std.fmt.parseInt(u16, port_str, 10) catch
                return error.InvalidPort;
            return parse4(host, port_num);
        }

        return error.InvalidAddress;
    }

    /// Parse a bare host string (IPv4 or IPv6, no port) and apply
    /// `default_port`. Useful when host and port come from separate
    /// config sources.
    pub fn parseHost(host: []const u8, default_port: u16) ParseError!Address {
        if (host.len == 0) return error.InvalidAddress;
        // IPv6 contains `:` — disambiguate from IPv4.
        if (mem.indexOfScalar(u8, host, ':') != null) {
            return parseIpv6(host, default_port);
        }
        return parse4(host, default_port);
    }

    /// Parse a dotted-quad IPv4 string and bind to `port`.
    pub fn parse4(host: []const u8, port_num: u16) ParseError!Address {
        var octets: [4]u8 = undefined;
        var octet_idx: usize = 0;
        var current: u16 = 0;
        var digit_count: u8 = 0;

        for (host) |c| {
            if (c >= '0' and c <= '9') {
                current = current * 10 + (c - '0');
                digit_count += 1;
                if (digit_count > 3 or current > 255) return error.InvalidAddress;
            } else if (c == '.') {
                if (digit_count == 0 or octet_idx >= 3) return error.InvalidAddress;
                octets[octet_idx] = @intCast(current);
                octet_idx += 1;
                current = 0;
                digit_count = 0;
            } else {
                return error.InvalidAddress;
            }
        }
        if (digit_count == 0 or octet_idx != 3) return error.InvalidAddress;
        octets[3] = @intCast(current);
        return initIpv4(octets, port_num);
    }

    /// Parse a colon-separated IPv6 string and bind to `port`.
    pub fn parse6(host: []const u8, port_num: u16) ParseError!Address {
        return parseIpv6(host, port_num);
    }

    fn parseIpv6Bracketed(addr_str: []const u8) ParseError!Address {
        // Format: [addr]:port — addr in [..], port after the right bracket.
        const close_bracket = mem.indexOfScalar(u8, addr_str, ']') orelse return error.InvalidAddress;
        if (close_bracket == addr_str.len - 1 or addr_str[close_bracket + 1] != ':') {
            return error.InvalidAddress;
        }
        const host = addr_str[1..close_bracket];
        const port_str = addr_str[close_bracket + 2 ..];
        const port_num = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidPort;
        return parseIpv6(host, port_num);
    }

    fn parseIpv6(host: []const u8, port_num: u16) ParseError!Address {
        var segments: [8]u16 = @splat(0);
        var seg_idx: usize = 0;

        // Handle :: compression by splitting on it.
        if (mem.indexOf(u8, host, "::")) |dcolon_pos| {
            // Disallow multiple `::`.
            if (mem.indexOf(u8, host[dcolon_pos + 2 ..], "::") != null) return error.InvalidAddress;

            const left = host[0..dcolon_pos];
            const right = host[dcolon_pos + 2 ..];

            // Parse left segments.
            var left_count: usize = 0;
            if (left.len > 0) {
                left_count = try parseIpv6Segments(left, segments[0..]);
            }

            // Parse right segments into temp; then place at the end.
            var right_buf: [8]u16 = @splat(0);
            var right_count: usize = 0;
            if (right.len > 0) {
                right_count = try parseIpv6Segments(right, right_buf[0..]);
            }

            if (left_count + right_count > 8) return error.InvalidAddress;
            // Shift right segments to the tail of `segments`.
            const start = 8 - right_count;
            var i: usize = 0;
            while (i < right_count) : (i += 1) {
                segments[start + i] = right_buf[i];
            }
            seg_idx = 8;
        } else {
            seg_idx = try parseIpv6Segments(host, segments[0..]);
            if (seg_idx != 8) return error.InvalidAddress;
        }

        return initIpv6(segments, port_num);
    }

    fn parseIpv6Segments(s: []const u8, out: []u16) ParseError!usize {
        var count: usize = 0;
        var start: usize = 0;
        for (s, 0..) |c, idx| {
            if (c == ':') {
                if (start == idx) return error.InvalidAddress;
                if (count >= out.len) return error.InvalidAddress;
                out[count] = std.fmt.parseInt(u16, s[start..idx], 16) catch
                    return error.InvalidAddress;
                count += 1;
                start = idx + 1;
            }
        }
        if (start < s.len) {
            if (count >= out.len) return error.InvalidAddress;
            out[count] = std.fmt.parseInt(u16, s[start..], 16) catch
                return error.InvalidAddress;
            count += 1;
        }
        return count;
    }

    // ─── Construction ────────────────────────────────────────────────

    /// Create an IPv4 address.
    pub fn initIpv4(octets: [4]u8, port_num: u16) Address {
        const sa_in = posix.sockaddr.in{
            .port = mem.nativeToBig(u16, port_num),
            .addr = @bitCast(octets), // Bytes are already in network order.
        };
        return fromSockaddrIn(sa_in);
    }

    /// Create an IPv6 address.
    pub fn initIpv6(segments: [8]u16, port_num: u16) Address {
        var addr_bytes: [16]u8 = undefined;
        for (segments, 0..) |seg, i| {
            const be = mem.nativeToBig(u16, seg);
            addr_bytes[i * 2] = @intCast(be & 0xFF);
            addr_bytes[i * 2 + 1] = @intCast(be >> 8);
        }
        const sa_in6 = posix.sockaddr.in6{
            .port = mem.nativeToBig(u16, port_num),
            .flowinfo = 0,
            .addr = addr_bytes,
            .scope_id = 0,
        };
        return fromSockaddrIn6(sa_in6);
    }

    /// IPv4 loopback (127.0.0.1) on `port`.
    pub fn loopback4(port_num: u16) Address {
        return initIpv4(.{ 127, 0, 0, 1 }, port_num);
    }

    /// IPv6 loopback (::1) on `port`.
    pub fn loopback6(port_num: u16) Address {
        return initIpv6(.{ 0, 0, 0, 0, 0, 0, 0, 1 }, port_num);
    }

    /// IPv4 wildcard (0.0.0.0) — listen on every IPv4 interface.
    pub fn any4(port_num: u16) Address {
        return initIpv4(.{ 0, 0, 0, 0 }, port_num);
    }

    /// IPv6 wildcard (::) — listen on every IPv6 interface. Pair
    /// with the dual-stack listener helper (Phase A.2 TCP options)
    /// to accept IPv4 too via IPv4-mapped IPv6.
    pub fn any6(port_num: u16) Address {
        return initIpv6(.{ 0, 0, 0, 0, 0, 0, 0, 0 }, port_num);
    }

    fn fromSockaddrIn(sa_in: posix.sockaddr.in) Address {
        var storage: posix.sockaddr.storage = undefined;
        @memcpy(@as([*]u8, @ptrCast(&storage))[0..@sizeOf(posix.sockaddr.in)], @as([*]const u8, @ptrCast(&sa_in))[0..@sizeOf(posix.sockaddr.in)]);
        return .{
            .storage = storage,
            .len = @sizeOf(posix.sockaddr.in),
        };
    }

    fn fromSockaddrIn6(sa_in6: posix.sockaddr.in6) Address {
        var storage: posix.sockaddr.storage = undefined;
        @memcpy(@as([*]u8, @ptrCast(&storage))[0..@sizeOf(posix.sockaddr.in6)], @as([*]const u8, @ptrCast(&sa_in6))[0..@sizeOf(posix.sockaddr.in6)]);
        return .{
            .storage = storage,
            .len = @sizeOf(posix.sockaddr.in6),
        };
    }

    /// Build an Address from a raw `*const sockaddr` + length —
    /// the shape returned by `getsockname` / `getpeername` /
    /// `accept`. Caller must ensure `sa` points to a valid sockaddr
    /// of family AF_INET, AF_INET6, or AF_UNIX. Returns an
    /// uninitialized storage past the copied length.
    pub fn fromSockaddr(sa: *const posix.sockaddr, sa_len: posix.socklen_t) Address {
        var storage: posix.sockaddr.storage = undefined;
        const copy_len = @min(@as(usize, sa_len), @sizeOf(posix.sockaddr.storage));
        @memcpy(@as([*]u8, @ptrCast(&storage))[0..copy_len], @as([*]const u8, @ptrCast(sa))[0..copy_len]);
        return .{ .storage = storage, .len = sa_len };
    }

    // ─── Accessors ───────────────────────────────────────────────────

    pub fn family(self: Address) posix.sa_family_t {
        return self.storage.family;
    }

    pub fn isIpv4(self: Address) bool {
        return self.storage.family == posix.AF.INET;
    }

    pub fn isIpv6(self: Address) bool {
        return self.storage.family == posix.AF.INET6;
    }

    pub fn port(self: Address) u16 {
        if (self.isIpv4()) {
            const sa: *const posix.sockaddr.in = @ptrCast(@alignCast(&self.storage));
            return mem.bigToNative(u16, sa.port);
        } else if (self.isIpv6()) {
            const sa: *const posix.sockaddr.in6 = @ptrCast(@alignCast(&self.storage));
            return mem.bigToNative(u16, sa.port);
        }
        return 0;
    }

    /// IPv4 octets in network order (a.b.c.d → {a,b,c,d}).
    /// Returns null for non-IPv4 addresses.
    pub fn ipv4Octets(self: Address) ?[4]u8 {
        if (!self.isIpv4()) return null;
        const sa: *const posix.sockaddr.in = @ptrCast(@alignCast(&self.storage));
        return @bitCast(sa.addr);
    }

    /// Raw IPv6 address bytes (16). Returns null for non-IPv6.
    pub fn ipv6Bytes(self: Address) ?[16]u8 {
        if (!self.isIpv6()) return null;
        const sa: *const posix.sockaddr.in6 = @ptrCast(@alignCast(&self.storage));
        return sa.addr;
    }

    /// True for loopback (127.0.0.0/8 or ::1).
    pub fn isLoopback(self: Address) bool {
        if (self.ipv4Octets()) |o| return o[0] == 127;
        if (self.ipv6Bytes()) |b| {
            for (b[0..15]) |x| if (x != 0) return false;
            return b[15] == 1;
        }
        return false;
    }

    /// True for wildcard / unspecified (0.0.0.0 or ::).
    pub fn isUnspecified(self: Address) bool {
        if (self.ipv4Octets()) |o| {
            for (o) |x| if (x != 0) return false;
            return true;
        }
        if (self.ipv6Bytes()) |b| {
            for (b) |x| if (x != 0) return false;
            return true;
        }
        return false;
    }

    /// True for RFC 1918 IPv4 ranges (10/8, 172.16/12, 192.168/16)
    /// or IPv6 ULA (fc00::/7).
    pub fn isPrivate(self: Address) bool {
        if (self.ipv4Octets()) |o| {
            if (o[0] == 10) return true;
            if (o[0] == 172 and (o[1] >= 16 and o[1] <= 31)) return true;
            if (o[0] == 192 and o[1] == 168) return true;
            return false;
        }
        if (self.ipv6Bytes()) |b| {
            return (b[0] & 0xFE) == 0xFC; // fc00::/7
        }
        return false;
    }

    /// Pointer to the underlying sockaddr — for `bind`, `connect`,
    /// `sendto`, etc. The kernel reads `family` to dispatch.
    pub fn sockaddr(self: *const Address) *const posix.sockaddr {
        return @ptrCast(@alignCast(&self.storage));
    }

    pub fn sockaddrMut(self: *Address) *posix.sockaddr {
        return @ptrCast(@alignCast(&self.storage));
    }

    /// `std.fmt` integration — `{}` prints `"127.0.0.1:8080"` or
    /// `"[::1]:8080"`.
    pub fn format(self: Address, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        if (self.ipv4Octets()) |o| {
            try writer.print("{d}.{d}.{d}.{d}:{d}", .{ o[0], o[1], o[2], o[3], self.port() });
        } else if (self.ipv6Bytes()) |b| {
            try writer.writeAll("[");
            var i: usize = 0;
            while (i < 8) : (i += 1) {
                if (i > 0) try writer.writeAll(":");
                const seg: u16 = (@as(u16, b[i * 2]) << 8) | b[i * 2 + 1];
                try writer.print("{x}", .{seg});
            }
            try writer.print("]:{d}", .{self.port()});
        } else {
            try writer.print("<unknown family {d}>", .{@intFromEnum(self.storage.family)});
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "Address.parse IPv4 with port" {
    const a = try Address.parse("127.0.0.1:8080");
    try testing.expect(a.isIpv4());
    try testing.expectEqual(@as(u16, 8080), a.port());
    try testing.expectEqual([4]u8{ 127, 0, 0, 1 }, a.ipv4Octets().?);
    try testing.expect(a.isLoopback());
}

test "Address.parse IPv6 bracketed" {
    const a = try Address.parse("[::1]:443");
    try testing.expect(a.isIpv6());
    try testing.expectEqual(@as(u16, 443), a.port());
    try testing.expect(a.isLoopback());
    const b = a.ipv6Bytes().?;
    var i: usize = 0;
    while (i < 15) : (i += 1) try testing.expectEqual(@as(u8, 0), b[i]);
    try testing.expectEqual(@as(u8, 1), b[15]);
}

test "Address.parse IPv6 compressed bracketed" {
    const a = try Address.parse("[2001:db8::1]:80");
    try testing.expect(a.isIpv6());
    try testing.expectEqual(@as(u16, 80), a.port());
    const b = a.ipv6Bytes().?;
    try testing.expectEqual(@as(u8, 0x20), b[0]);
    try testing.expectEqual(@as(u8, 0x01), b[1]);
    try testing.expectEqual(@as(u8, 0x0D), b[2]);
    try testing.expectEqual(@as(u8, 0xB8), b[3]);
    try testing.expectEqual(@as(u8, 0), b[4]);
    try testing.expectEqual(@as(u8, 0), b[14]);
    try testing.expectEqual(@as(u8, 1), b[15]);
}

test "Address.parseHost — bare IPv4 / IPv6 with default port" {
    const a = try Address.parseHost("10.0.0.1", 80);
    try testing.expect(a.isIpv4());
    try testing.expectEqual(@as(u16, 80), a.port());
    try testing.expect(a.isPrivate());

    const b = try Address.parseHost("::1", 443);
    try testing.expect(b.isIpv6());
    try testing.expectEqual(@as(u16, 443), b.port());
}

test "Address constructors — loopback / any / init" {
    const l4 = Address.loopback4(8080);
    try testing.expect(l4.isLoopback());
    try testing.expectEqual(@as(u16, 8080), l4.port());

    const l6 = Address.loopback6(8080);
    try testing.expect(l6.isLoopback());
    try testing.expect(l6.isIpv6());

    const a4 = Address.any4(0);
    try testing.expect(a4.isUnspecified());
    try testing.expect(a4.isIpv4());

    const a6 = Address.any6(0);
    try testing.expect(a6.isUnspecified());
    try testing.expect(a6.isIpv6());

    const c = Address.initIpv4(.{ 192, 168, 1, 1 }, 22);
    try testing.expect(c.isPrivate());
}

test "Address.isPrivate — RFC 1918 + ULA" {
    try testing.expect((Address.initIpv4(.{ 10, 0, 0, 1 }, 0)).isPrivate());
    try testing.expect((Address.initIpv4(.{ 172, 20, 0, 1 }, 0)).isPrivate());
    try testing.expect((Address.initIpv4(.{ 192, 168, 1, 1 }, 0)).isPrivate());
    try testing.expect(!(Address.initIpv4(.{ 8, 8, 8, 8 }, 0)).isPrivate());
    try testing.expect(!(Address.initIpv4(.{ 172, 32, 0, 1 }, 0)).isPrivate());
}

test "Address.parse — invalid forms return errors" {
    try testing.expectError(error.InvalidAddress, Address.parse(""));
    try testing.expectError(error.InvalidAddress, Address.parse("not.an.address"));
    try testing.expectError(error.InvalidPort, Address.parse("127.0.0.1:notaport"));
}
