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
const win32 = @import("win32.zig");
const PlatformStat = @import("metadata.zig").PlatformStat;

const is_windows = builtin.os.tag == .windows;
const page_size = std.heap.page_size_min;

pub const FsError = fs_error.FsError;

/// Handle type accepted by `mapFile` — a POSIX fd or a Windows file
/// `HANDLE` (matches `File.fd`).
pub const MapHandle = if (is_windows) win32.HANDLE else c_int;

/// Windows keeps a separate mapping-object handle alongside the view
/// pointer; it must be closed on `deinit`. Empty on POSIX.
const WinMapState = if (is_windows) struct {
    mapping: win32.HANDLE = undefined,
} else struct {};

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
    /// Windows-only mapping-object handle (empty struct on POSIX).
    win: WinMapState = .{},

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
        if (is_windows) {
            // Windows has no direct madvise equivalent for most hints
            // (PrefetchVirtualMemory only covers WILLNEED). Advice is a
            // non-binding hint, so a no-op is a correct implementation.
            return;
        }
        const adv = darwinOrLinuxAdvice(hint);
        if (std.c.madvise(@ptrCast(self.data), pageRound(self.len), adv) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    /// Lock pages in physical memory — kernel won't page them out.
    /// Requires CAP_IPC_LOCK / root on Linux; bounded by RLIMIT_MEMLOCK.
    pub fn lock(self: *MappedFile) FsError!void {
        if (is_windows) {
            if (win32.VirtualLock(@ptrCast(self.data), pageRound(self.len)) == 0) {
                return win32.fromLastError(win32.GetLastError());
            }
            return;
        }
        if (c_mlock(@ptrCast(self.data), pageRound(self.len)) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    pub fn unlock(self: *MappedFile) FsError!void {
        if (is_windows) {
            if (win32.VirtualUnlock(@ptrCast(self.data), pageRound(self.len)) == 0) {
                return win32.fromLastError(win32.GetLastError());
            }
            return;
        }
        if (c_munlock(@ptrCast(self.data), pageRound(self.len)) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    /// Synchronously flush dirty pages back to the file.
    pub fn flush(self: *MappedFile) FsError!void {
        if (is_windows) {
            if (win32.FlushViewOfFile(@ptrCast(self.data), self.len) == 0) {
                return win32.fromLastError(win32.GetLastError());
            }
            return;
        }
        if (std.c.msync(@ptrCast(self.data), pageRound(self.len), std.c.MSF.SYNC) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    /// Async msync — kicks off the flush, doesn't wait. (Windows
    /// FlushViewOfFile has no async variant; behaves like `flush`.)
    pub fn flushAsync(self: *MappedFile) FsError!void {
        if (is_windows) {
            if (win32.FlushViewOfFile(@ptrCast(self.data), self.len) == 0) {
                return win32.fromLastError(win32.GetLastError());
            }
            return;
        }
        if (std.c.msync(@ptrCast(self.data), pageRound(self.len), std.c.MSF.ASYNC) != 0) {
            return fs_error.fromErrno(fs_error.currentErrno());
        }
    }

    /// Change protection on the live mapping. Useful for write-once-
    /// then-protect patterns.
    pub fn protect(self: *MappedFile, protection: Protection) FsError!void {
        if (is_windows) {
            const np: win32.DWORD = if (protection == .read_write) win32.PAGE_READWRITE else win32.PAGE_READONLY;
            var old: win32.DWORD = 0;
            if (win32.VirtualProtect(@ptrCast(self.data), pageRound(self.len), np, &old) == 0) {
                return win32.fromLastError(win32.GetLastError());
            }
            self.writable = (protection == .read_write);
            return;
        }
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
        if (is_windows) {
            _ = win32.UnmapViewOfFile(@ptrCast(self.data));
            _ = win32.CloseHandle(self.win.mapping);
            self.len = 0;
            return;
        }
        _ = std.c.munmap(@ptrCast(self.data), pageRound(self.len));
        self.len = 0;
    }
};

// ─── Mapping entry points ────────────────────────────────────────

/// Map an open file's contents into memory.
pub fn mapFile(fd: MapHandle, opts: MapOptions) FsError!MappedFile {
    if (is_windows) return winMapFile(fd, opts);
    if (opts.offset != 0 and opts.offset % page_size != 0) return error.InvalidPath;

    var actual_len = opts.length;
    if (actual_len == 0) {
        var st: PlatformStat = undefined;
        if (syscall.fstat(fd, &st) != 0) return fs_error.fromErrno(fs_error.currentErrno());
        const file_size: u64 = st.size();
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
    if (is_windows) {
        if (opts.length == 0) return error.InvalidPath;
        // Anonymous = file mapping backed by the system paging file
        // (hFile = INVALID_HANDLE_VALUE). Always shared-style access.
        const flprotect = winMapProtect(opts.protection, .shared);
        const size: u64 = opts.length;
        const hmap = win32.CreateFileMappingW(win32.INVALID_HANDLE_VALUE, null, flprotect, @truncate(size >> 32), @truncate(size), null) orelse
            return win32.fromLastError(win32.GetLastError());
        errdefer _ = win32.CloseHandle(hmap);
        const view = win32.MapViewOfFile(hmap, winMapAccess(opts.protection, .shared), 0, 0, opts.length) orelse
            return win32.fromLastError(win32.GetLastError());
        return .{
            .data = @ptrCast(@alignCast(view)),
            .len = opts.length,
            .writable = opts.protection == .read_write,
            .win = .{ .mapping = hmap },
        };
    }
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

// ─── Windows mapping helpers ─────────────────────────────────────
// Only referenced from `is_windows` branches → never analysed on POSIX.

fn winMapFile(fd: MapHandle, opts: MapOptions) FsError!MappedFile {
    // Windows requires the view offset to be allocation-granularity
    // aligned (64 KiB); MapViewOfFile fails otherwise and we surface
    // it as an error from GetLastError.
    var actual_len = opts.length;
    if (actual_len == 0) {
        var info: win32.BY_HANDLE_FILE_INFORMATION = undefined;
        if (win32.GetFileInformationByHandle(fd, &info) == 0) return win32.fromLastError(win32.GetLastError());
        const file_size = (@as(u64, info.nFileSizeHigh) << 32) | @as(u64, info.nFileSizeLow);
        if (file_size <= opts.offset) return error.InvalidPath;
        actual_len = @intCast(file_size - opts.offset);
    }
    if (actual_len == 0) return error.InvalidPath;

    // The mapping object must be sized to cover offset + length.
    const max_size: u64 = opts.offset + actual_len;
    const hmap = win32.CreateFileMappingW(fd, null, winMapProtect(opts.protection, opts.sharing), @truncate(max_size >> 32), @truncate(max_size), null) orelse
        return win32.fromLastError(win32.GetLastError());
    errdefer _ = win32.CloseHandle(hmap);

    const view = win32.MapViewOfFile(hmap, winMapAccess(opts.protection, opts.sharing), @truncate(opts.offset >> 32), @truncate(opts.offset), actual_len) orelse
        return win32.fromLastError(win32.GetLastError());

    return .{
        .data = @ptrCast(@alignCast(view)),
        .len = actual_len,
        .writable = opts.protection == .read_write,
        .win = .{ .mapping = hmap },
    };
}

fn winMapProtect(p: Protection, s: Sharing) win32.DWORD {
    return switch (p) {
        .read_only => win32.PAGE_READONLY,
        .read_write => switch (s) {
            .shared => win32.PAGE_READWRITE,
            .private => win32.PAGE_WRITECOPY,
        },
    };
}

fn winMapAccess(p: Protection, s: Sharing) win32.DWORD {
    return switch (p) {
        .read_only => win32.FILE_MAP_READ,
        .read_write => switch (s) {
            .shared => win32.FILE_MAP_WRITE,
            .private => win32.FILE_MAP_COPY,
        },
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
const test_util = @import("../testing.zig");
const File = @import("file.zig").File;

test "MappedFile: map read-only file, read slice" {
    var tmp = try test_util.TempDir.create(testing.allocator);
    defer tmp.deinit();
    const fp = try tmp.writeChild(testing.allocator, "data.bin", "hello mmap");
    defer testing.allocator.free(fp);

    var f = try File.open(fp);
    defer f.close();
    var m = try mapFile(f.fd, .{});
    defer m.deinit();
    try testing.expectEqualStrings("hello mmap", m.asBytes()[0..10]);
}

test "MappedFile: anonymous mapping is zeroed + writable" {
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
    var tmp = try test_util.TempDir.create(testing.allocator);
    defer tmp.deinit();
    const fp = try tmp.childPath(testing.allocator, "rw.bin");
    defer testing.allocator.free(fp);

    // Create a 64-byte file (mmap requires non-zero size).
    {
        var cf = try File.create(fp);
        try cf.writeAll(&([_]u8{0} ** 64));
        cf.close();
    }
    {
        var f = try File.openOptions(fp, .{ .read = true, .write = true });
        defer f.close();
        var m = try mapFile(f.fd, .{ .protection = .read_write, .sharing = .shared, .length = 64 });
        defer m.deinit();
        const mut = try m.asBytesMut();
        @memcpy(mut[0..5], "READY");
        try m.flush();
    }

    // Reopen + readback through the (cross-platform) File API.
    var rf = try File.open(fp);
    defer rf.close();
    var buf: [5]u8 = undefined;
    _ = try rf.readFull(&buf);
    try testing.expectEqualStrings("READY", &buf);
}

test "MappedFile: advise works on a live mapping" {
    var m = try mapAnonymous(.{ .length = page_size * 4 });
    defer m.deinit();
    try m.advise(.sequential);
    try m.advise(.will_need);
    try m.advise(.dont_need);
}

test "MappedFile: protect read-only blocks subsequent write" {
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
