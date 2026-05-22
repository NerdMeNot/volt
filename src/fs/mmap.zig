//! Memory-mapped files (`MappedFile`) and anonymous mappings.
//!
//! Zero-copy file access — the kernel pages in regions on demand and
//! the slice you hold is the kernel's view of the file. Read-only
//! mappings are safe for concurrent readers; write mappings need
//! external sync.
//!
//! **Truncation safety**: if a file is truncated while mapped, the
//! pages beyond the new EOF fault with SIGBUS on access. v1 leaves
//! that as the caller's problem (don't truncate live mappings).
//! A follow-up wave adds a SIGBUS handler + setjmp-based recovery.

const std = @import("std");
const builtin = @import("builtin");

const syscall = @import("syscall.zig");
const fs_error = @import("error.zig");

const is_windows = builtin.os.tag == .windows;
const page_size = std.heap.page_size_min;

pub const FsError = fs_error.FsError;

// ─── Options ─────────────────────────────────────────────────────

pub const Protection = enum { read_only, read_write };

pub const Sharing = enum {
    /// `MAP_PRIVATE` — writes stay local to this process (COW).
    private,
    /// `MAP_SHARED` — writes are visible to other mappers and
    /// flushed back to the file. Required for write-through.
    shared,
};

/// Options for `MappedFile.mapFile`.
pub const MapOptions = struct {
    protection: Protection = .read_only,
    sharing: Sharing = .private,
    /// File offset to start the mapping at. Must be page-aligned.
    offset: u64 = 0,
    /// Length to map. `0` ⇒ map the whole file from `offset`.
    length: usize = 0,
};

/// Options for `MappedFile.mapAnonymous`.
pub const AnonOptions = struct {
    length: usize,
    protection: Protection = .read_write,
    sharing: Sharing = .private,
};

/// Access advice (`madvise(2)` hint).
pub const Advice = enum { normal, sequential, random, will_need, dont_need };

// ─── MappedFile ──────────────────────────────────────────────────

/// A live memory mapping. `deinit` unmaps; the slice returned by
/// `asBytes` is invalid afterward.
pub const MappedFile = struct {
    /// Raw kernel mapping pointer. Page-aligned.
    data: [*]align(page_size) u8,
    /// Length the caller asked for (may be less than the page-
    /// rounded mapping behind the scenes — we always round up).
    len: usize,
    /// Whether the mapping is writable (drives `asBytesMut` access).
    writable: bool,

    /// Read-only slice of the mapped region.
    pub fn asBytes(self: MappedFile) []const u8 {
        return self.data[0..self.len];
    }

    /// Mutable slice — errors if the mapping was opened read-only.
    pub fn asBytesMut(self: MappedFile) error{ReadOnlyMapping}![]u8 {
        if (!self.writable) return error.ReadOnlyMapping;
        return self.data[0..self.len];
    }

    /// Pass an access hint to the kernel for the whole mapping.
    pub fn advise(self: *MappedFile, hint: Advice) FsError!void {
        if (is_windows) @compileError("Windows madvise: pending");
        const adv = darwinOrLinuxAdvice(hint);
        if (std.c.madvise(@ptrCast(self.data), pageRound(self.len), adv) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    /// Lock pages in physical memory — kernel won't page them out.
    /// Requires CAP_IPC_LOCK / root on Linux; bounded by RLIMIT_MEMLOCK.
    pub fn lock(self: *MappedFile) FsError!void {
        if (is_windows) @compileError("Windows VirtualLock: pending");
        if (c_mlock(@ptrCast(self.data), pageRound(self.len)) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    pub fn unlock(self: *MappedFile) FsError!void {
        if (is_windows) @compileError("Windows VirtualUnlock: pending");
        if (c_munlock(@ptrCast(self.data), pageRound(self.len)) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    /// Synchronously flush dirty pages back to the file.
    pub fn flush(self: *MappedFile) FsError!void {
        if (is_windows) @compileError("Windows FlushViewOfFile: pending");
        if (std.c.msync(@ptrCast(self.data), pageRound(self.len), std.c.MSF.SYNC) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    /// Async msync — kicks off the flush, doesn't wait.
    pub fn flushAsync(self: *MappedFile) FsError!void {
        if (is_windows) @compileError("Windows FlushViewOfFile: pending");
        if (std.c.msync(@ptrCast(self.data), pageRound(self.len), std.c.MSF.ASYNC) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    /// Change protection on the live mapping. Useful for write-once-
    /// then-protect patterns.
    pub fn protect(self: *MappedFile, protection: Protection) FsError!void {
        if (is_windows) @compileError("Windows VirtualProtect: pending");
        const prot = protToC(protection);
        if (std.c.mprotect(@ptrCast(self.data), pageRound(self.len), prot) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
        self.writable = (protection == .read_write);
    }

    /// Touch every page so the kernel commits backing pages now
    /// rather than on first access. Useful before a latency-sensitive
    /// pass.
    pub fn prefault(self: *MappedFile) void {
        var i: usize = 0;
        var sum: u8 = 0;
        while (i < self.len) : (i += page_size) {
            sum +%= self.data[i];
        }
        // Touch the result so LLVM doesn't dead-code-eliminate the
        // entire prefault loop.
        std.mem.doNotOptimizeAway(sum);
    }

    /// Release the mapping.
    pub fn deinit(self: *MappedFile) void {
        if (self.len == 0) return;
        if (is_windows) @compileError("Windows UnmapViewOfFile: pending");
        _ = std.c.munmap(@ptrCast(self.data), pageRound(self.len));
        self.len = 0;
    }
};

// ─── Mapping entry points ────────────────────────────────────────

/// Map an open file's contents into memory.
pub fn mapFile(fd: c_int, opts: MapOptions) FsError!MappedFile {
    if (is_windows) @compileError("Windows MapViewOfFile: pending");
    if (opts.offset != 0 and opts.offset % page_size != 0) return error.InvalidPath;

    var actual_len = opts.length;
    if (actual_len == 0) {
        var st: std.c.Stat = undefined;
        if (syscall.fstat(fd, &st) != 0) return fs_error.fromErrno(fs_error.currentErrno());
        const file_size: u64 = @intCast(st.size);
        if (file_size <= opts.offset) return error.InvalidPath;
        actual_len = @intCast(file_size - opts.offset);
    }

    if (actual_len == 0) return error.InvalidPath;

    const prot = protToC(opts.protection);
    const map = mapToC(opts.sharing, false);

    const raw = std.c.mmap(null, actual_len, prot, map, fd, @intCast(opts.offset));
    if (@intFromPtr(raw) == std.math.maxInt(usize)) {
        // MAP_FAILED == (void *)-1
        return fs_error.fromErrno(fs_error.currentErrno());
    }

    return .{
        .data = @ptrCast(@alignCast(raw)),
        .len = actual_len,
        .writable = opts.protection == .read_write,
    };
}

/// Anonymous mapping — backed by zeroed pages, not by a file.
/// Useful for large scratch buffers that the kernel can swap.
pub fn mapAnonymous(opts: AnonOptions) FsError!MappedFile {
    if (is_windows) @compileError("Windows anonymous mapping: pending");
    if (opts.length == 0) return error.InvalidPath;

    const prot = protToC(opts.protection);
    const map = mapToC(opts.sharing, true);

    const raw = std.c.mmap(null, opts.length, prot, map, -1, 0);
    if (@intFromPtr(raw) == std.math.maxInt(usize)) {
        return fs_error.fromErrno(fs_error.currentErrno());
    }

    return .{
        .data = @ptrCast(@alignCast(raw)),
        .len = opts.length,
        .writable = opts.protection == .read_write,
    };
}

// ─── Helpers ─────────────────────────────────────────────────────

fn pageRound(len: usize) usize {
    return (len + page_size - 1) & ~@as(usize, page_size - 1);
}

fn protToC(p: Protection) std.c.PROT {
    return switch (p) {
        .read_only => .{ .READ = true },
        .read_write => .{ .READ = true, .WRITE = true },
    };
}

fn mapToC(sharing: Sharing, anonymous: bool) std.c.MAP {
    var m: std.c.MAP = .{
        .TYPE = switch (sharing) {
            .private => .PRIVATE,
            .shared => .SHARED,
        },
    };
    if (anonymous) m.ANONYMOUS = true;
    return m;
}

fn darwinOrLinuxAdvice(hint: Advice) u32 {
    return switch (hint) {
        .normal => std.c.MADV.NORMAL,
        .sequential => std.c.MADV.SEQUENTIAL,
        .random => std.c.MADV.RANDOM,
        .will_need => std.c.MADV.WILLNEED,
        .dont_need => std.c.MADV.DONTNEED,
    };
}

const c_mlock = if (is_windows) {} else @extern(
    *const fn (*align(page_size) const anyopaque, usize) callconv(.c) c_int,
    .{ .name = "mlock" },
);
const c_munlock = if (is_windows) {} else @extern(
    *const fn (*align(page_size) const anyopaque, usize) callconv(.c) c_int,
    .{ .name = "munlock" },
);

// ─── Tests ───────────────────────────────────────────────────────

const testing = std.testing;

fn allocMmapTmp(allocator: std.mem.Allocator) ![:0]u8 {
    const tmpl = "/tmp/volt-mmap-XXXXXX";
    const buf = try allocator.allocSentinel(u8, tmpl.len, 0);
    @memcpy(buf[0..tmpl.len], tmpl);
    if (syscall.c_mkdtemp(buf.ptr) == null) {
        allocator.free(buf);
        return error.MkdtempFailed;
    }
    return buf;
}

test "MappedFile: map read-only file, read slice" {
    if (is_windows) return error.SkipZigTest;
    const tmp = try allocMmapTmp(testing.allocator);
    defer testing.allocator.free(tmp);
    defer _ = syscall.c_rmdir(tmp.ptr);

    var fp_buf: [256:0]u8 = undefined;
    const fp = try std.fmt.bufPrintZ(&fp_buf, "{s}/data.bin", .{tmp});
    defer _ = syscall.c_unlink(fp.ptr);

    const fd_create = syscall.c_open(fp.ptr, syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC, 0o644);
    try testing.expect(fd_create >= 0);
    const payload = "hello mmap";
    try testing.expectEqual(@as(isize, payload.len), syscall.c_write(fd_create, payload.ptr, payload.len));
    _ = syscall.c_close(fd_create);

    const fd_read = syscall.c_open(fp.ptr, syscall.O_RDONLY, 0);
    try testing.expect(fd_read >= 0);
    defer _ = syscall.c_close(fd_read);

    var m = try mapFile(fd_read, .{});
    defer m.deinit();
    try testing.expectEqualStrings(payload, m.asBytes()[0..payload.len]);
}

test "MappedFile: anonymous mapping is zeroed + writable" {
    if (is_windows) return error.SkipZigTest;
    var m = try mapAnonymous(.{ .length = page_size });
    defer m.deinit();

    const buf = try m.asBytesMut();
    try testing.expectEqual(@as(u8, 0), buf[0]);
    buf[0] = 0xAB;
    buf[page_size - 1] = 0xCD;
    try testing.expectEqual(@as(u8, 0xAB), buf[0]);
    try testing.expectEqual(@as(u8, 0xCD), buf[page_size - 1]);
}

test "MappedFile: writable shared map + flush round-trip" {
    if (is_windows) return error.SkipZigTest;
    const tmp = try allocMmapTmp(testing.allocator);
    defer testing.allocator.free(tmp);
    defer _ = syscall.c_rmdir(tmp.ptr);

    var fp_buf: [256:0]u8 = undefined;
    const fp = try std.fmt.bufPrintZ(&fp_buf, "{s}/rw.bin", .{tmp});
    defer _ = syscall.c_unlink(fp.ptr);

    // Create a 1-page file (mmap requires non-zero size).
    const fd = syscall.c_open(fp.ptr, syscall.O_RDWR | syscall.O_CREAT | syscall.O_TRUNC, 0o644);
    try testing.expect(fd >= 0);
    var zeros = [_]u8{0} ** 64;
    try testing.expect(syscall.c_write(fd, &zeros, zeros.len) == zeros.len);
    {
        var m = try mapFile(fd, .{ .protection = .read_write, .sharing = .shared, .length = zeros.len });
        defer m.deinit();
        const mut = try m.asBytesMut();
        @memcpy(mut[0..5], "READY");
        try m.flush();
    }
    _ = syscall.c_close(fd);

    // Reopen + readback.
    const fd2 = syscall.c_open(fp.ptr, syscall.O_RDONLY, 0);
    defer _ = syscall.c_close(fd2);
    var buf: [5]u8 = undefined;
    const c_read = @extern(*const fn (c_int, [*]u8, usize) callconv(.c) isize, .{ .name = "read" });
    _ = c_read(fd2, &buf, buf.len);
    try testing.expectEqualStrings("READY", &buf);
}

test "MappedFile: advise works on a live mapping" {
    if (is_windows) return error.SkipZigTest;
    var m = try mapAnonymous(.{ .length = page_size * 4 });
    defer m.deinit();
    try m.advise(.sequential);
    try m.advise(.will_need);
    try m.advise(.dont_need);
}

test "MappedFile: protect read-only blocks subsequent write" {
    if (is_windows) return error.SkipZigTest;
    var m = try mapAnonymous(.{ .length = page_size });
    defer m.deinit();
    const mut = try m.asBytesMut();
    mut[0] = 1;
    try m.protect(.read_only);
    // Caller can no longer obtain mutable access via the type.
    try testing.expectError(error.ReadOnlyMapping, m.asBytesMut());
    // Read still works.
    try testing.expectEqual(@as(u8, 1), m.asBytes()[0]);
}
