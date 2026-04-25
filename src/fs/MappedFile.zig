//! Memory-Mapped File
//!
//! Provides zero-copy access to file contents via memory mapping. The kernel
//! pages file data in and out on demand, so only the actively accessed pages
//! consume physical memory.
//!
//! ## Example
//!
//! ```zig
//! const fs = @import("volt").fs;
//!
//! // Map a file read-only
//! var mapped = try fs.mmapFile("data.csv", .{});
//! defer mapped.unmap();
//!
//! // mapped.slice() is []const u8 — pass directly to any parser
//! const data = mapped.slice();
//! ```
//!
//! ## Read-Write Mapping
//!
//! ```zig
//! var mapped = try fs.mmapFile("output.dat", .{ .protection = .read_write });
//! defer mapped.unmap();
//!
//! // Write directly into the mapped region
//! @memcpy(mapped.sliceMut()[offset..][0..data.len], data);
//! try mapped.sync();
//! ```

const std = @import("std");
const posix = std.posix;
const syscall = @import("../internal/syscall.zig");
const builtin = @import("builtin");

const File = @import("file.zig").File;
const OpenOptions = @import("open_options.zig").OpenOptions;

const page_size = std.heap.page_size_min;

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Protection mode for memory-mapped files.
pub const MmapProtection = enum {
    /// Pages can be read but not written.
    read_only,
    /// Pages can be read and written. Changes are flushed to disk.
    read_write,
};

/// Options for memory-mapping a file.
pub const MmapOptions = struct {
    /// Protection mode for the mapping.
    protection: MmapProtection = .read_only,
    /// Hint to kernel: sequential access pattern (enables read-ahead).
    sequential: bool = false,
    /// Hint to kernel: random access pattern (disables read-ahead).
    random: bool = false,
    /// Populate page tables eagerly (avoid page faults during access).
    /// Linux: uses MAP_POPULATE. macOS: uses madvise(MADV_WILLNEED).
    populate: bool = false,
};

/// Advisory hint for mapped memory regions.
pub const MadviceHint = enum {
    /// Default behavior.
    normal,
    /// Sequential access — increase read-ahead.
    sequential,
    /// Random access — disable read-ahead.
    random,
    /// Will need this data soon — start prefetching.
    will_need,
    /// Done with this data — can evict from cache.
    dont_need,
};

// ═══════════════════════════════════════════════════════════════════════════════
// MappedFile
// ═══════════════════════════════════════════════════════════════════════════════

/// A memory-mapped file region.
///
/// Read-only mappings are safe for concurrent reads from multiple threads.
/// Read-write mappings require external synchronization for concurrent access.
pub const MappedFile = struct {
    data: [*]align(page_size) u8,
    len: usize,
    protection: MmapProtection,

    /// Get an immutable slice of the mapped region.
    pub fn slice(self: MappedFile) []const u8 {
        return self.data[0..self.len];
    }

    /// Get a mutable slice of the mapped region.
    /// Returns `error.ReadOnlyMapping` if the file was mapped read-only.
    pub fn sliceMut(self: MappedFile) error{ReadOnlyMapping}![]u8 {
        if (self.protection != .read_write) return error.ReadOnlyMapping;
        return self.data[0..self.len];
    }

    /// Flush changes to disk (synchronous).
    /// Only meaningful for read-write mappings.
    pub fn sync(self: *MappedFile) !void {
        const mem = self.data[0..alignToPage(self.len)];
        try posix.msync(mem, std.c.MSF.SYNC);
    }

    /// Give advice about expected access patterns for a region of the mapping.
    pub fn advise(self: *MappedFile, offset: usize, len: usize, hint: MadviceHint) !void {
        const adv_offset = alignDownToPage(offset);
        const adv_len = alignToPage(len + (offset - adv_offset));
        if (adv_len == 0) return;

        const advice: u32 = switch (hint) {
            .normal => posix.MADV.NORMAL,
            .sequential => posix.MADV.SEQUENTIAL,
            .random => posix.MADV.RANDOM,
            .will_need => posix.MADV.WILLNEED,
            .dont_need => posix.MADV.DONTNEED,
        };
        // adv_offset is page-aligned, so the resulting pointer preserves page alignment.
        const ptr: [*]align(page_size) u8 = @alignCast(self.data + adv_offset);
        try posix.madvise(ptr, adv_len, advice);
    }

    /// Unmap the file from memory.
    pub fn unmap(self: *MappedFile) void {
        if (self.len == 0) return;
        const aligned_len = alignToPage(self.len);
        posix.munmap(@alignCast(self.data[0..aligned_len]));
        self.len = 0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Mapping Functions
// ═══════════════════════════════════════════════════════════════════════════════

/// Memory-map a file by path.
///
/// The file is opened, mapped, and the file descriptor is closed (the mapping
/// survives the close per POSIX). For read-write mappings, changes are backed
/// by the file on disk via MAP_SHARED.
///
/// Returns `error.EmptyFile` if the file has zero length.
pub fn mmapFile(path: []const u8, options: MmapOptions) !MappedFile {
    var opts = OpenOptions.new().setRead(true);
    if (options.protection == .read_write) {
        opts = opts.setWrite(true);
    }
    const std_file = try opts.open(path);
    var file = File.fromRawFd(std_file);
    defer file.close();

    return mmapFd(file.handle, options);
}

/// Memory-map an open file handle.
///
/// Does NOT take ownership of the file — the caller is still responsible for
/// closing it. The mapping remains valid even after the file is closed.
///
/// Returns `error.EmptyFile` if the file has zero length.
pub fn mmapHandle(file: *File, options: MmapOptions) !MappedFile {
    return mmapFd(file.handle, options);
}

fn mmapFd(fd: posix.fd_t, options: MmapOptions) !MappedFile {
    const stat = try syscall.fstat(fd);
    const size: usize = @intCast(stat.size);
    if (size == 0) return error.EmptyFile;

    // Protection flags. In Zig 0.16, posix.PROT is a packed struct (was u32 in 0.15).
    var prot: posix.PROT = .{ .READ = true };
    if (options.protection == .read_write) {
        prot.WRITE = true;
    }

    // MAP flags.
    var flags = if (options.protection == .read_write)
        posix.MAP{ .TYPE = .SHARED }
    else
        posix.MAP{ .TYPE = .PRIVATE };

    // MAP_POPULATE is Linux-only.
    if (comptime builtin.os.tag == .linux) {
        if (options.populate) {
            flags.POPULATE = true;
        }
    }

    const data = try posix.mmap(null, size, prot, flags, fd, 0);

    var mapped = MappedFile{
        .data = data.ptr,
        .len = size,
        .protection = options.protection,
    };

    // Apply post-mapping hints.
    if (options.sequential) {
        mapped.advise(0, size, .sequential) catch {};
    }
    if (options.random) {
        mapped.advise(0, size, .random) catch {};
    }
    // On macOS, populate via madvise(WILLNEED) since there's no MAP_POPULATE.
    if (comptime builtin.os.tag != .linux) {
        if (options.populate) {
            mapped.advise(0, size, .will_need) catch {};
        }
    }

    return mapped;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn alignToPage(len: usize) usize {
    return (len + page_size - 1) & ~@as(usize, page_size - 1);
}

fn alignDownToPage(offset: usize) usize {
    return offset & ~@as(usize, page_size - 1);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "MappedFile - read only" {
    const path = "/tmp/blitz_io_mmap_ro.txt";
    const content = "Hello from memory-mapped file!";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll(content);
    }
    defer syscall.unlink(path) catch {};

    var mapped = try mmapFile(path, .{});
    defer mapped.unmap();

    try testing.expectEqualStrings(content, mapped.slice());
}

test "MappedFile - read write and sync" {
    const path = "/tmp/blitz_io_mmap_rw.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("original content!");
    }
    defer syscall.unlink(path) catch {};

    // Map read-write and modify.
    {
        var mapped = try mmapFile(path, .{ .protection = .read_write });
        defer mapped.unmap();

        const buf = try mapped.sliceMut();
        @memcpy(buf[0..8], "MODIFIED");
        try mapped.sync();
    }

    // Read back and verify the change persisted.
    {
        var file = try File.open(path);
        defer file.close();
        var buf: [64]u8 = undefined;
        const n = try file.read(&buf);
        try testing.expectEqualStrings("MODIFIED content!", buf[0..n]);
    }
}

test "MappedFile - sliceMut on read only" {
    const path = "/tmp/blitz_io_mmap_ro_mut.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("data");
    }
    defer syscall.unlink(path) catch {};

    var mapped = try mmapFile(path, .{});
    defer mapped.unmap();

    try testing.expectError(error.ReadOnlyMapping, mapped.sliceMut());
}

test "MappedFile - empty file" {
    const path = "/tmp/blitz_io_mmap_empty.txt";

    {
        var file = try File.create(path);
        file.close();
    }
    defer syscall.unlink(path) catch {};

    const result = mmapFile(path, .{});
    try testing.expectError(error.EmptyFile, result);
}

test "MappedFile - mmapHandle" {
    const path = "/tmp/blitz_io_mmap_handle.txt";
    const content = "handle-based mapping";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll(content);
    }
    defer syscall.unlink(path) catch {};

    var file = try File.open(path);
    defer file.close();

    var mapped = try mmapHandle(&file, .{});
    defer mapped.unmap();

    try testing.expectEqualStrings(content, mapped.slice());

    // File handle should still be valid after mapping.
    try testing.expect(file.handle != -1);
}

test "MappedFile - sequential hint" {
    const path = "/tmp/blitz_io_mmap_seq.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("sequential access data");
    }
    defer syscall.unlink(path) catch {};

    var mapped = try mmapFile(path, .{ .sequential = true });
    defer mapped.unmap();

    try testing.expectEqualStrings("sequential access data", mapped.slice());
}

test "MappedFile - random hint" {
    const path = "/tmp/blitz_io_mmap_rand.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("random access data");
    }
    defer syscall.unlink(path) catch {};

    var mapped = try mmapFile(path, .{ .random = true });
    defer mapped.unmap();

    try testing.expectEqualStrings("random access data", mapped.slice());
}

test "MappedFile - populate hint" {
    const path = "/tmp/blitz_io_mmap_pop.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("populated mapping");
    }
    defer syscall.unlink(path) catch {};

    var mapped = try mmapFile(path, .{ .populate = true });
    defer mapped.unmap();

    try testing.expectEqualStrings("populated mapping", mapped.slice());
}

test "MappedFile - advise on mapped region" {
    const path = "/tmp/blitz_io_mmap_advise.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("a" ** 8192);
    }
    defer syscall.unlink(path) catch {};

    var mapped = try mmapFile(path, .{});
    defer mapped.unmap();

    try mapped.advise(0, 4096, .will_need);
    try mapped.advise(4096, 4096, .dont_need);
    try mapped.advise(0, mapped.len, .sequential);
}

test "MappedFile - large file" {
    const path = "/tmp/blitz_io_mmap_large.txt";
    const size: usize = 1024 * 1024; // 1MB

    // Generate test data.
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    for (data, 0..) |*b, i| {
        b.* = @intCast(i % 256);
    }

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll(data);
    }
    defer syscall.unlink(path) catch {};

    var mapped = try mmapFile(path, .{});
    defer mapped.unmap();

    try testing.expectEqual(size, mapped.len);
    try testing.expectEqualSlices(u8, data, mapped.slice());
}
