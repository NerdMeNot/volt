//! Socket Address - IPv4 and IPv6 with full parsing support
//!
//! Provides a unified address type for TCP/UDP sockets with:
//! - Full IPv4 parsing (dotted decimal)
//! - Full IPv6 parsing (with :: compression, bracketed notation)
//! - Port handling
//! - Utility methods (isLoopback, isPrivate, etc.)
//!
//! ## Examples
//!
//! ```zig
//! // Parse various formats
//! const a1 = try Address.parse("127.0.0.1:8080");
//! const a2 = try Address.parse("[::1]:8080");
//! const a3 = try Address.parse("0.0.0.0:80");
//!
//! // Create directly
//! const a4 = Address.initIpv4(.{ 192, 168, 1, 1 }, 3000);
//! const a5 = Address.fromPort(8080); // 0.0.0.0:8080
//! ```
//!
//! Modeled after standard socket address types with full parsing support.

const std = @import("std");
const posix = std.posix;
const mem = std.mem;

/// Socket address supporting IPv4 and IPv6.
pub const Address = struct {
    /// Internal storage for sockaddr.
    storage: posix.sockaddr.storage,
    /// Length of the address structure.
    len: posix.socklen_t,

    // ═══════════════════════════════════════════════════════════════════════════
    // Parsing
    // ═══════════════════════════════════════════════════════════════════════════

    /// Parse an address string.
    ///
    /// Supported formats:
    /// - IPv4: "127.0.0.1:8080", "0.0.0.0:80"
    /// - IPv6: "[::1]:8080", "[2001:db8::1]:443"
    /// - IPv6 without brackets (no port): "::1", "2001:db8::1"
    pub fn parse(addr_str: []const u8) !Address {
        // Check for IPv6 bracketed notation: [addr]:port
        if (addr_str.len > 0 and addr_str[0] == '[') {
            return parseIpv6Bracketed(addr_str);
        }

        // Try to find the last colon for port separation
        // For IPv4, there's only one colon (the port separator)
        // For IPv6 without brackets, we don't support port (ambiguous)
        if (mem.lastIndexOfScalar(u8, addr_str, ':')) |colon_pos| {
            // Check if there are multiple colons (IPv6 without port)
            if (mem.indexOfScalar(u8, addr_str[0..colon_pos], ':') != null) {
                // Multiple colons = IPv6 address without port
                return parseIpv6(addr_str, 0);
            }

            // Single colon = IPv4 with port
            const host = addr_str[0..colon_pos];
            const port_str = addr_str[colon_pos + 1 ..];
            const port_num = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidPort;
            return parseIpv4(host, port_num);
        }

        // No colon at all - could be IPv4 without port (not supported) or invalid
        return error.InvalidAddress;
    }

    /// Parse IPv4 address string (without port).
    fn parseIpv4(host: []const u8, port_num: u16) !Address {
        var octets: [4]u8 = undefined;
        var octet_idx: usize = 0;
        var current_octet: u16 = 0;
        var digit_count: u8 = 0;

        for (host) |c| {
            if (c >= '0' and c <= '9') {
                current_octet = current_octet * 10 + (c - '0');
                digit_count += 1;
                if (digit_count > 3 or current_octet > 255) return error.InvalidAddress;
            } else if (c == '.') {
                if (digit_count == 0 or octet_idx >= 3) return error.InvalidAddress;
                octets[octet_idx] = @intCast(current_octet);
                octet_idx += 1;
                current_octet = 0;
                digit_count = 0;
            } else {
                return error.InvalidAddress;
            }
        }

        // Final octet
        if (digit_count == 0 or octet_idx != 3) return error.InvalidAddress;
        octets[3] = @intCast(current_octet);

        return initIpv4(octets, port_num);
    }

    /// Parse bracketed IPv6: [addr]:port
    fn parseIpv6Bracketed(addr_str: []const u8) !Address {
        // Must start with [ and contain ]:
        if (addr_str.len < 4 or addr_str[0] != '[') return error.InvalidAddress;

        const close_bracket = mem.indexOfScalar(u8, addr_str, ']') orelse return error.InvalidAddress;
        if (close_bracket + 1 >= addr_str.len or addr_str[close_bracket + 1] != ':') {
            return error.InvalidAddress;
        }

        const ipv6_str = addr_str[1..close_bracket];
        const port_str = addr_str[close_bracket + 2 ..];
        const port_num = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidPort;

        return parseIpv6(ipv6_str, port_num);
    }

    /// Parse IPv6 address string.
    fn parseIpv6(ipv6_str: []const u8, port_num: u16) !Address {
        var segments: [8]u16 = .{ 0, 0, 0, 0, 0, 0, 0, 0 };
        var segment_idx: usize = 0;
        var compression_idx: ?usize = null;
        var current_segment: u16 = 0;
        var digit_count: u8 = 0;
        var i: usize = 0;

        while (i < ipv6_str.len) {
            const c = ipv6_str[i];

            if (c == ':') {
                if (i + 1 < ipv6_str.len and ipv6_str[i + 1] == ':') {
                    // :: compression
                    if (compression_idx != null) return error.InvalidAddress; // Only one :: allowed
                    if (digit_count > 0) {
                        if (segment_idx >= 8) return error.InvalidAddress;
                        segments[segment_idx] = current_segment;
                        segment_idx += 1;
                    }
                    compression_idx = segment_idx;
                    current_segment = 0;
                    digit_count = 0;
                    i += 2;
                    continue;
                } else {
                    // Single colon - segment separator
                    if (digit_count == 0) return error.InvalidAddress;
                    if (segment_idx >= 8) return error.InvalidAddress;
                    segments[segment_idx] = current_segment;
                    segment_idx += 1;
                    current_segment = 0;
                    digit_count = 0;
                    i += 1;
                    continue;
                }
            }

            // Hex digit
            const hex_val: u16 = if (c >= '0' and c <= '9')
                c - '0'
            else if (c >= 'a' and c <= 'f')
                c - 'a' + 10
            else if (c >= 'A' and c <= 'F')
                c - 'A' + 10
            else
                return error.InvalidAddress;

            current_segment = (current_segment << 4) | hex_val;
            digit_count += 1;
            if (digit_count > 4) return error.InvalidAddress;
            i += 1;
        }

        // Handle final segment
        if (digit_count > 0) {
            if (segment_idx >= 8) return error.InvalidAddress;
            segments[segment_idx] = current_segment;
            segment_idx += 1;
        }

        // Apply :: compression
        if (compression_idx) |comp_idx| {
            const segments_after = segment_idx - comp_idx;
            const zeros_needed = 8 - segment_idx;

            // Shift segments after compression point to the right
            var j: usize = 7;
            while (j >= comp_idx + zeros_needed) : (j -= 1) {
                segments[j] = segments[j - zeros_needed];
                if (j == 0) break;
            }

            // Fill zeros
            for (comp_idx..comp_idx + zeros_needed) |k| {
                segments[k] = 0;
            }
            _ = segments_after;
        } else {
            // No compression - must have exactly 8 segments
            if (segment_idx != 8) return error.InvalidAddress;
        }

        return initIpv6(segments, port_num);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /// Create an IPv4 address.
    pub fn initIpv4(octets: [4]u8, port_num: u16) Address {
        var addr: Address = undefined;
        addr.storage = mem.zeroes(posix.sockaddr.storage);

        const in_ptr: *posix.sockaddr.in = @ptrCast(&addr.storage);
        in_ptr.* = .{
            .family = posix.AF.INET,
            .port = mem.nativeToBig(u16, port_num),
            .addr = @bitCast(octets),
        };
        addr.len = @sizeOf(posix.sockaddr.in);

        return addr;
    }

    /// Create an IPv6 address.
    pub fn initIpv6(segments: [8]u16, port_num: u16) Address {
        var addr: Address = undefined;
        addr.storage = mem.zeroes(posix.sockaddr.storage);

        const in6_ptr: *posix.sockaddr.in6 = @ptrCast(&addr.storage);
        in6_ptr.* = .{
            .family = posix.AF.INET6,
            .port = mem.nativeToBig(u16, port_num),
            .flowinfo = 0,
            .addr = undefined,
            .scope_id = 0,
        };

        // Convert segments to bytes (network byte order)
        var bytes: [16]u8 = undefined;
        for (segments, 0..) |seg, i| {
            bytes[i * 2] = @intCast((seg >> 8) & 0xFF);
            bytes[i * 2 + 1] = @intCast(seg & 0xFF);
        }
        in6_ptr.addr = bytes;
        addr.len = @sizeOf(posix.sockaddr.in6);

        return addr;
    }

    /// Create from port only (binds to all interfaces - 0.0.0.0).
    pub fn fromPort(port_num: u16) Address {
        return initIpv4(.{ 0, 0, 0, 0 }, port_num);
    }

    /// Create IPv4 loopback address (127.0.0.1).
    pub fn loopbackV4(port_num: u16) Address {
        return initIpv4(.{ 127, 0, 0, 1 }, port_num);
    }

    /// Create IPv6 loopback address (::1).
    pub fn loopbackV6(port_num: u16) Address {
        return initIpv6(.{ 0, 0, 0, 0, 0, 0, 0, 1 }, port_num);
    }

    /// Create from host string and port.
    pub fn init(host: ?[]const u8, port_num: u16) !Address {
        if (host) |h| {
            // Try IPv4 first
            if (parseIpv4(h, port_num)) |addr| {
                return addr;
            } else |_| {}

            // Try IPv6
            if (parseIpv6(h, port_num)) |addr| {
                return addr;
            } else |_| {}

            return error.InvalidAddress;
        } else {
            return fromPort(port_num);
        }
    }

    /// Parse an IP address (without port), applying a default port.
    ///
    /// Unlike `parse()`, this accepts bare IP addresses without port:
    /// - "127.0.0.1" → 127.0.0.1:default_port
    /// - "::1" → [::1]:default_port
    /// - "2001:db8::1" → [2001:db8::1]:default_port
    ///
    /// Use this when you have a configuration that specifies host and port separately,
    /// or when the port comes from a different source (e.g., environment variable).
    ///
    /// ## Example
    ///
    /// ```zig
    /// const host = std.process.getEnvVarOwned(allocator, "HOST") orelse "127.0.0.1";
    /// const port = std.process.getEnvVarOwned(allocator, "PORT") orelse "8080";
    /// const addr = try Address.parseHost(host, try std.fmt.parseInt(u16, port, 10));
    /// ```
    pub fn parseHost(host: []const u8, default_port: u16) !Address {
        // Try IPv4 first
        if (parseIpv4(host, default_port)) |addr| {
            return addr;
        } else |_| {}

        // Try IPv6
        if (parseIpv6(host, default_port)) |addr| {
            return addr;
        } else |_| {}

        return error.InvalidAddress;
    }

    /// Create from std.net.Address (e.g., from getAddressList).
    ///
    /// This allows interop with the standard library's DNS resolver.
    pub fn fromStd(addr: std.net.Address) Address {
        var result: Address = undefined;
        result.storage = mem.zeroes(posix.sockaddr.storage);

        switch (addr.any.family) {
            posix.AF.INET => {
                const in_ptr: *posix.sockaddr.in = @ptrCast(&result.storage);
                in_ptr.* = addr.in;
                result.len = @sizeOf(posix.sockaddr.in);
            },
            posix.AF.INET6 => {
                const in6_ptr: *posix.sockaddr.in6 = @ptrCast(&result.storage);
                in6_ptr.* = addr.in6;
                result.len = @sizeOf(posix.sockaddr.in6);
            },
            else => {
                // Fallback - copy the raw storage
                const src_ptr: *const posix.sockaddr.storage = @ptrCast(&addr.any);
                result.storage = src_ptr.*;
                result.len = @sizeOf(posix.sockaddr.storage);
            },
        }

        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Accessors
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the port number.
    pub fn port(self: Address) u16 {
        const addr_family = self.family();
        if (addr_family == posix.AF.INET) {
            const in_ptr: *const posix.sockaddr.in = @ptrCast(&self.storage);
            return mem.bigToNative(u16, in_ptr.port);
        } else if (addr_family == posix.AF.INET6) {
            const in6_ptr: *const posix.sockaddr.in6 = @ptrCast(&self.storage);
            return mem.bigToNative(u16, in6_ptr.port);
        }
        return 0;
    }

    /// Get the address family.
    pub fn family(self: Address) posix.sa_family_t {
        return @as(*const posix.sockaddr, @ptrCast(&self.storage)).family;
    }

    /// Check if this is an IPv4 address.
    pub fn isIpv4(self: Address) bool {
        return self.family() == posix.AF.INET;
    }

    /// Check if this is an IPv6 address.
    pub fn isIpv6(self: Address) bool {
        return self.family() == posix.AF.INET6;
    }

    /// Check if this is a loopback address.
    pub fn isLoopback(self: Address) bool {
        if (self.isIpv4()) {
            const in_ptr: *const posix.sockaddr.in = @ptrCast(&self.storage);
            const bytes: [4]u8 = @bitCast(in_ptr.addr);
            return bytes[0] == 127;
        } else if (self.isIpv6()) {
            const in6_ptr: *const posix.sockaddr.in6 = @ptrCast(&self.storage);
            // ::1
            const expected: [16]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
            return mem.eql(u8, &in6_ptr.addr, &expected);
        }
        return false;
    }

    /// Check if this is an unspecified address (0.0.0.0 or ::).
    pub fn isUnspecified(self: Address) bool {
        if (self.isIpv4()) {
            const in_ptr: *const posix.sockaddr.in = @ptrCast(&self.storage);
            return in_ptr.addr == 0;
        } else if (self.isIpv6()) {
            const in6_ptr: *const posix.sockaddr.in6 = @ptrCast(&self.storage);
            const zero: [16]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
            return mem.eql(u8, &in6_ptr.addr, &zero);
        }
        return false;
    }

    /// Check if this is a private/local address.
    /// For IPv4: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
    /// For IPv6: fc00::/7 (unique local)
    pub fn isPrivate(self: Address) bool {
        if (self.isIpv4()) {
            const in_ptr: *const posix.sockaddr.in = @ptrCast(&self.storage);
            const bytes: [4]u8 = @bitCast(in_ptr.addr);
            // 10.0.0.0/8
            if (bytes[0] == 10) return true;
            // 172.16.0.0/12
            if (bytes[0] == 172 and (bytes[1] & 0xF0) == 16) return true;
            // 192.168.0.0/16
            if (bytes[0] == 192 and bytes[1] == 168) return true;
            return false;
        } else if (self.isIpv6()) {
            const in6_ptr: *const posix.sockaddr.in6 = @ptrCast(&self.storage);
            // fc00::/7 (unique local addresses)
            return (in6_ptr.addr[0] & 0xFE) == 0xFC;
        }
        return false;
    }

    /// Get IPv4 octets (returns null for IPv6).
    pub fn ipv4Octets(self: Address) ?[4]u8 {
        if (!self.isIpv4()) return null;
        const in_ptr: *const posix.sockaddr.in = @ptrCast(&self.storage);
        return @bitCast(in_ptr.addr);
    }

    /// Get IPv6 bytes (returns null for IPv4).
    pub fn ipv6Bytes(self: Address) ?[16]u8 {
        if (!self.isIpv6()) return null;
        const in6_ptr: *const posix.sockaddr.in6 = @ptrCast(&self.storage);
        return in6_ptr.addr;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Low-Level Access
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the underlying sockaddr pointer (for syscalls).
    pub fn sockaddr(self: *const Address) *const posix.sockaddr {
        return @ptrCast(&self.storage);
    }

    /// Get a mutable sockaddr pointer.
    pub fn sockaddrMut(self: *Address) *posix.sockaddr {
        return @ptrCast(&self.storage);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Formatting
    // ═══════════════════════════════════════════════════════════════════════════

    /// Format address as string.
    pub fn format(
        self: Address,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        if (self.isIpv4()) {
            const octets = self.ipv4Octets().?;
            try writer.print("{}.{}.{}.{}:{}", .{
                octets[0],
                octets[1],
                octets[2],
                octets[3],
                self.port(),
            });
        } else if (self.isIpv6()) {
            const bytes = self.ipv6Bytes().?;
            try writer.writeAll("[");

            // Find longest run of zeros for :: compression
            var zero_start: ?usize = null;
            var zero_len: usize = 0;
            var best_start: ?usize = null;
            var best_len: usize = 0;

            var i: usize = 0;
            while (i < 8) {
                const seg = @as(u16, bytes[i * 2]) << 8 | bytes[i * 2 + 1];
                if (seg == 0) {
                    if (zero_start == null) zero_start = i;
                    zero_len += 1;
                } else {
                    if (zero_len > best_len) {
                        best_start = zero_start;
                        best_len = zero_len;
                    }
                    zero_start = null;
                    zero_len = 0;
                }
                i += 1;
            }
            if (zero_len > best_len) {
                best_start = zero_start;
                best_len = zero_len;
            }

            // Only use :: for runs of 2 or more zeros
            if (best_len < 2) best_start = null;

            var wrote_double_colon = false;
            i = 0;
            while (i < 8) {
                if (best_start != null and i == best_start.?) {
                    try writer.writeAll(if (i == 0) "::" else ":");
                    i += best_len;
                    wrote_double_colon = true;
                    continue;
                }

                if (i > 0 and !(wrote_double_colon and i == best_start.? + best_len)) {
                    if (!wrote_double_colon or i != best_start.? + best_len) {
                        try writer.writeAll(":");
                    }
                }

                const seg = @as(u16, bytes[i * 2]) << 8 | bytes[i * 2 + 1];
                try writer.print("{x}", .{seg});
                wrote_double_colon = false;
                i += 1;
            }

            try writer.print("]:{}", .{self.port()});
        } else {
            try writer.writeAll("<unknown>");
        }
    }

    /// Write IP address to buffer (without port).
    pub fn ipString(self: Address, buf: []u8) []u8 {
        var stream = std.io.fixedBufferStream(buf);
        const writer = stream.writer();

        if (self.isIpv4()) {
            const octets = self.ipv4Octets().?;
            writer.print("{}.{}.{}.{}", .{
                octets[0],
                octets[1],
                octets[2],
                octets[3],
            }) catch return buf[0..0];
        } else if (self.isIpv6()) {
            // Simplified: just write full form
            const bytes = self.ipv6Bytes().?;
            var i: usize = 0;
            while (i < 8) : (i += 1) {
                if (i > 0) writer.writeAll(":") catch return buf[0..0];
                const seg = @as(u16, bytes[i * 2]) << 8 | bytes[i * 2 + 1];
                writer.print("{x}", .{seg}) catch return buf[0..0];
            }
        }

        return buf[0..stream.pos];
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Address - parse IPv4" {
    const addr = try Address.parse("127.0.0.1:8080");
    try std.testing.expectEqual(@as(u16, 8080), addr.port());
    try std.testing.expect(addr.isIpv4());
    try std.testing.expect(addr.isLoopback());

    const octets = addr.ipv4Octets().?;
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, octets);
}

test "Address - parse IPv4 various" {
    const a1 = try Address.parse("0.0.0.0:80");
    try std.testing.expect(a1.isUnspecified());
    try std.testing.expectEqual(@as(u16, 80), a1.port());

    const a2 = try Address.parse("192.168.1.1:3000");
    try std.testing.expect(a2.isPrivate());

    const a3 = try Address.parse("10.0.0.1:443");
    try std.testing.expect(a3.isPrivate());
}

test "Address - parse IPv6 bracketed" {
    const addr = try Address.parse("[::1]:8080");
    try std.testing.expectEqual(@as(u16, 8080), addr.port());
    try std.testing.expect(addr.isIpv6());
    try std.testing.expect(addr.isLoopback());
}

test "Address - parse IPv6 full" {
    const addr = try Address.parse("[2001:db8:85a3:0:0:8a2e:370:7334]:443");
    try std.testing.expectEqual(@as(u16, 443), addr.port());
    try std.testing.expect(addr.isIpv6());
}

test "Address - parse IPv6 compressed" {
    // :: at start
    const a1 = try Address.parse("[::1]:80");
    try std.testing.expect(a1.isLoopback());

    // :: in middle
    const a2 = try Address.parse("[2001:db8::1]:80");
    try std.testing.expect(a2.isIpv6());

    // :: at end
    const a3 = try Address.parse("[2001:db8::]:80");
    try std.testing.expect(a3.isIpv6());
}

test "Address - fromPort" {
    const addr = Address.fromPort(3000);
    try std.testing.expectEqual(@as(u16, 3000), addr.port());
    try std.testing.expect(addr.isUnspecified());
    try std.testing.expect(addr.isIpv4());
}

test "Address - loopback constructors" {
    const v4 = Address.loopbackV4(8080);
    try std.testing.expect(v4.isLoopback());
    try std.testing.expect(v4.isIpv4());

    const v6 = Address.loopbackV6(8080);
    try std.testing.expect(v6.isLoopback());
    try std.testing.expect(v6.isIpv6());
}

test "Address - initIpv4" {
    const addr = Address.initIpv4(.{ 192, 168, 1, 100 }, 22);
    try std.testing.expectEqual(@as(u16, 22), addr.port());
    try std.testing.expect(addr.isPrivate());
    try std.testing.expectEqual([4]u8{ 192, 168, 1, 100 }, addr.ipv4Octets().?);
}

test "Address - initIpv6" {
    const addr = Address.initIpv6(.{ 0x2001, 0x0db8, 0, 0, 0, 0, 0, 1 }, 80);
    try std.testing.expectEqual(@as(u16, 80), addr.port());
    try std.testing.expect(addr.isIpv6());
}

test "Address - format IPv4" {
    const addr = Address.initIpv4(.{ 127, 0, 0, 1 }, 8080);
    // Use ipString for testing the IP formatting
    var buf: [64]u8 = undefined;
    const ip = addr.ipString(&buf);
    try std.testing.expectEqualStrings("127.0.0.1", ip);
    try std.testing.expectEqual(@as(u16, 8080), addr.port());
}

test "Address - ipString" {
    const addr = Address.initIpv4(.{ 10, 0, 0, 1 }, 22);
    var buf: [64]u8 = undefined;
    const ip = addr.ipString(&buf);
    try std.testing.expectEqualStrings("10.0.0.1", ip);
}

test "Address - invalid addresses" {
    // Missing port
    try std.testing.expectError(error.InvalidAddress, Address.parse("127.0.0.1"));

    // Invalid octet
    try std.testing.expectError(error.InvalidAddress, Address.parse("256.0.0.1:80"));

    // Invalid port
    try std.testing.expectError(error.InvalidPort, Address.parse("127.0.0.1:99999"));

    // Invalid IPv6
    try std.testing.expectError(error.InvalidAddress, Address.parse("[:::1]:80"));
}

test "Address - private ranges" {
    // 10.x.x.x
    try std.testing.expect((try Address.parse("10.0.0.1:80")).isPrivate());
    try std.testing.expect((try Address.parse("10.255.255.255:80")).isPrivate());

    // 172.16-31.x.x
    try std.testing.expect((try Address.parse("172.16.0.1:80")).isPrivate());
    try std.testing.expect((try Address.parse("172.31.255.255:80")).isPrivate());
    try std.testing.expect(!(try Address.parse("172.32.0.1:80")).isPrivate());

    // 192.168.x.x
    try std.testing.expect((try Address.parse("192.168.0.1:80")).isPrivate());
    try std.testing.expect((try Address.parse("192.168.255.255:80")).isPrivate());

    // Public
    try std.testing.expect(!(try Address.parse("8.8.8.8:80")).isPrivate());
}

test "Address - parseHost IPv4" {
    const addr = try Address.parseHost("127.0.0.1", 8080);
    try std.testing.expectEqual(@as(u16, 8080), addr.port());
    try std.testing.expect(addr.isIpv4());
    try std.testing.expect(addr.isLoopback());

    const octets = addr.ipv4Octets().?;
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, octets);
}

test "Address - parseHost IPv6" {
    const addr = try Address.parseHost("::1", 443);
    try std.testing.expectEqual(@as(u16, 443), addr.port());
    try std.testing.expect(addr.isIpv6());
    try std.testing.expect(addr.isLoopback());
}

test "Address - parseHost full IPv6" {
    const addr = try Address.parseHost("2001:db8:85a3:0:0:8a2e:370:7334", 80);
    try std.testing.expectEqual(@as(u16, 80), addr.port());
    try std.testing.expect(addr.isIpv6());
}

test "Address - parseHost invalid" {
    // Not a valid IP
    try std.testing.expectError(error.InvalidAddress, Address.parseHost("localhost", 80));
    try std.testing.expectError(error.InvalidAddress, Address.parseHost("example.com", 80));
    try std.testing.expectError(error.InvalidAddress, Address.parseHost("", 80));
}
