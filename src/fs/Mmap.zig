//! `volt.fs.Mmap` — memory-mapped file or anonymous region.
//!
//! ## Page-fault contract — read this first
//!
//! Touching a cold page of a file-backed `Mmap` triggers a synchronous
//! disk read on the worker thread the calling coroutine is currently
//! scheduled on. **The runtime cannot intercept this.** There is no
//! syscall to suspend; the CPU traps, the kernel handles the fault
//! synchronously, and the worker is blocked on storage until it
//! returns. No async runtime escapes this — it is a property of mmap.
//!
//! Mitigations Volt provides:
//!   - `prefault(range)` — runs on the blocking pool; calls
//!     `madvise(MADV_WILLNEED)` and force-touches every page so
//!     subsequent access from the calling coroutine is RAM-only.
//!     Use this before a hot read loop over a file-backed map.
//!   - `MapOptions.populate = true` — Linux: `MAP_POPULATE` faults
//!     in all pages at map time. Darwin: emulated by walking +
//!     touching.
//!   - `MapOptions.locked = true` — `mlock`s the mapping; pages
//!     stay resident until `unlock` or `deinit`.
//!   - For sparse random access on a huge file, prefer
//!     `volt.fs.File.pread` (blocking-pool dispatch) over mmap. Mmap
//!     is for layout + sequential / hot data, not arbitrary RAM-
//!     backed random access on cold disk.
//!
//! Callers that hold a slice into a `Mmap` MUST NOT cache the
//! pointer across a `remap` call — see Risk #4 contract on `remap`.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const spawnBlocking = @import("../api/spawn_blocking.zig").spawnBlocking;
const traits = @import("../io/traits/traits.zig");
const io_errors = @import("../io/errors.zig");
const File = @import("File.zig").File;

// ─────────────────────────────────────────────────────────────────────
// Configuration types
// ─────────────────────────────────────────────────────────────────────

/// Lifetime + visibility model. `read_only` and `copy_on_write` both
/// map MAP_PRIVATE; `read_write` maps MAP_SHARED. Distinct from
/// `Perms` so `protect()` can change page permissions independently
/// of the mapping's sharing semantics.
pub const Mode = enum {
    read_only,
    read_write,
    copy_on_write,
};

/// Page-table permission bits. Maps to `PROT_*`. Mutated by
/// `protect()`.
pub const Perms = packed struct {
    read: bool = true,
    write: bool = false,
    exec: bool = false,
};

pub const HugePageSize = enum {
    /// `MAP_HUGE_2MB` on Linux. Not supported on Darwin in v1.1.
    @"2MB",
    /// `MAP_HUGE_1GB` on Linux. Not supported on Darwin in v1.1.
    @"1GB",
};

pub const Advice = enum {
    sequential,
    random,
    will_need,
    dont_need,
    /// `MADV_FREE` (Linux + Darwin) — zero-cost release for anonymous regions.
    free,
};

pub const FlushSync = enum {
    /// `MS_ASYNC` — kernel writes pages back at its own pace.
    async_,
    /// `MS_SYNC` — block until pages hit storage.
    sync_,
};

pub const Range = struct {
    offset: usize,
    length: usize,
};

pub const MapOptions = struct {
    mode: Mode,
    perms: Perms,
    /// Length to map. `null` = from `offset` to file end (fstat'd
    /// internally).
    len: ?usize = null,
    /// `MAP_POPULATE` (Linux) / emulated walk-and-touch (Darwin).
    /// Faults in every page at map time. Mitigates Risk #3 by
    /// front-loading the disk wait.
    populate: bool = false,
    /// Huge-page request. Linux only in v1.1; returns
    /// `error.HugePagesUnsupported` on other platforms.
    huge_pages: ?HugePageSize = null,
    /// `mlock` after map.
    locked: bool = false,
    /// Offset into the backing file. Must be page-aligned.
    offset: u64 = 0,
};

pub const AnonOptions = struct {
    perms: Perms = .{ .read = true, .write = true, .exec = false },
    huge_pages: ?HugePageSize = null,
    locked: bool = false,
};

// ─────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────

pub const MmapError = error{
    AccessDenied,
    InvalidArgument,
    SystemResources,
    OutOfMemory,
    HugePagesUnsupported,
    InvalidRange,
    /// Returned by `remap` when growth would collide with an existing
    /// adjacent mapping AND we couldn't relocate (Linux: `mremap`
    /// with `MREMAP_MAYMOVE` failed). Callers can fall back to deinit
    /// + new mapFile.
    RemapFailed,
    Cancelled,
    Unexpected,
};

// ─────────────────────────────────────────────────────────────────────
// The handle
// ─────────────────────────────────────────────────────────────────────

pub const Mmap = struct {
    ptr: [*]u8,
    len: usize,
    perms: Perms,
    mode: Mode,
    /// Backing fd, owned by this Mmap. Duplicated from the caller's
    /// `File` at `mapFile` time so `file.close()` doesn't pull the
    /// rug. `null` for anonymous maps.
    backing_fd: ?posix.fd_t,

    /// Map a region of `file` into the address space.
    ///
    /// The mapping holds a duplicated fd internally — the caller's
    /// `File` can be closed independently without invalidating the
    /// mapping. `deinit` closes the duplicated fd.
    pub fn mapFile(file: *File, opts: MapOptions) MmapError!Mmap {
        if (opts.huge_pages != null) return error.HugePagesUnsupported;

        // Determine length. If null, fstat the file and use size - offset.
        const length = if (opts.len) |l| l else blk: {
            const stat = file.metadata() catch return error.Unexpected;
            if (opts.offset >= stat.size) return error.InvalidRange;
            break :blk @as(usize, @intCast(stat.size - opts.offset));
        };
        if (length == 0) return error.InvalidRange;

        // Duplicate the fd so the mapping outlives the caller's File.
        const dup_fd = std.c.dup(file.fd);
        if (dup_fd < 0) return error.SystemResources;
        errdefer _ = std.c.close(dup_fd);

        const prot = makeProt(opts.perms);
        const map_flags = makeFileMap(opts.mode, opts.populate);

        const result = std.c.mmap(null, length, prot, map_flags, dup_fd, @intCast(opts.offset));
        const MAP_FAILED: usize = @bitCast(@as(isize, -1));
        if (@intFromPtr(result) == MAP_FAILED) {
            return mmapErrno();
        }

        const base: [*]u8 = @ptrCast(result);
        // Darwin: emulate MAP_POPULATE by force-touching every page
        // synchronously. Best-effort; ignore errors.
        if (opts.populate and builtin.os.tag != .linux) {
            forceTouchPages(base, length);
        }
        if (opts.locked) {
            const rc = std.c.mlock(@ptrCast(@alignCast(base)), length);
            if (rc != 0) {
                _ = std.c.munmap(@ptrCast(@alignCast(base)), length);
                _ = std.c.close(dup_fd);
                return mmapErrno();
            }
        }
        return Mmap{
            .ptr = base,
            .len = length,
            .perms = opts.perms,
            .mode = opts.mode,
            .backing_fd = dup_fd,
        };
    }

    /// Map an anonymous region. No backing file; useful for huge-
    /// buffer allocations and hugepage-backed scratch space.
    pub fn anonymous(len: usize, opts: AnonOptions) MmapError!Mmap {
        if (opts.huge_pages != null) return error.HugePagesUnsupported;
        if (len == 0) return error.InvalidRange;

        const prot = makeProt(opts.perms);
        const map_flags = makeAnonMap();

        // -1 fd for anonymous mappings on most platforms.
        const result = std.c.mmap(null, len, prot, map_flags, -1, 0);
        const MAP_FAILED: usize = @bitCast(@as(isize, -1));
        if (@intFromPtr(result) == MAP_FAILED) {
            return mmapErrno();
        }

        const base: [*]u8 = @ptrCast(result);
        if (opts.locked) {
            const rc = std.c.mlock(@ptrCast(@alignCast(base)), len);
            if (rc != 0) {
                _ = std.c.munmap(@ptrCast(@alignCast(base)), len);
                return mmapErrno();
            }
        }
        return Mmap{
            .ptr = base,
            .len = len,
            .perms = opts.perms,
            .mode = .read_write,
            .backing_fd = null,
        };
    }

    /// Mutable view of the entire mapping.
    pub fn slice(self: *Mmap) []u8 {
        return self.ptr[0..self.len];
    }

    /// Read-only view of the entire mapping.
    pub fn sliceConst(self: *const Mmap) []const u8 {
        return self.ptr[0..self.len];
    }

    /// Unmap and close the backing fd (if any). Idempotent: calling
    /// on a freshly zeroed Mmap is a no-op.
    pub fn deinit(self: *Mmap) void {
        if (self.len == 0) return;
        _ = std.c.munmap(@ptrCast(@alignCast(self.ptr)), self.len);
        if (self.backing_fd) |fd| _ = std.c.close(fd);
        self.* = undefined;
    }

    /// `madvise` over `range`. Hints don't affect correctness; they
    /// guide the kernel's page-cache behaviour. Pass `Advice.free`
    /// on anonymous regions you no longer need (pages can be
    /// reclaimed without write-back).
    pub fn advise(self: *Mmap, range: Range, hint: Advice) MmapError!void {
        try self.checkRange(range);
        const advice: u32 = switch (hint) {
            .sequential => std.c.MADV.SEQUENTIAL,
            .random => std.c.MADV.RANDOM,
            .will_need => std.c.MADV.WILLNEED,
            .dont_need => std.c.MADV.DONTNEED,
            .free => std.c.MADV.FREE,
        };
        const ptr: *align(std.heap.pageSize()) anyopaque = @ptrCast(@alignCast(self.ptr + range.offset));
        const rc = std.c.madvise(ptr, range.length, advice);
        if (rc != 0) return mmapErrno();
    }

    /// `mlock` over `range` — pin those pages in RAM until `unlock`
    /// or `deinit`. The whole-mapping lock at `init` time uses this
    /// path internally.
    pub fn lock(self: *Mmap, range: Range) MmapError!void {
        try self.checkRange(range);
        const ptr: *align(std.heap.pageSize()) const anyopaque = @ptrCast(@alignCast(self.ptr + range.offset));
        const rc = std.c.mlock(ptr, range.length);
        if (rc != 0) return mmapErrno();
    }

    pub fn unlock(self: *Mmap, range: Range) MmapError!void {
        try self.checkRange(range);
        const ptr: *align(std.heap.pageSize()) const anyopaque = @ptrCast(@alignCast(self.ptr + range.offset));
        const rc = std.c.munlock(ptr, range.length);
        if (rc != 0) return mmapErrno();
    }

    /// `msync` — flush dirty pages to backing storage. `sync_`
    /// blocks until pages hit storage; `async_` schedules and
    /// returns immediately. No-op on anonymous mappings.
    pub fn flush(self: *Mmap, range: Range, sync: FlushSync) MmapError!void {
        try self.checkRange(range);
        if (self.backing_fd == null) return; // anonymous — nothing to flush
        const flags: c_int = switch (sync) {
            .async_ => MS_ASYNC,
            .sync_ => MS_SYNC,
        };
        const ptr: *align(std.heap.pageSize()) const anyopaque = @ptrCast(@alignCast(self.ptr + range.offset));
        const rc = std.c.msync(ptr, range.length, flags);
        if (rc != 0) return mmapErrno();
    }

    /// `mprotect` — change page-table permissions over `range`. Use
    /// to e.g. mark a region read-only after writing it.
    pub fn protect(self: *Mmap, range: Range, perms: Perms) MmapError!void {
        try self.checkRange(range);
        const prot = makeProt(perms);
        const ptr: *align(std.heap.pageSize()) anyopaque = @ptrCast(@alignCast(self.ptr + range.offset));
        const rc = std.c.mprotect(ptr, range.length, prot);
        if (rc != 0) return mmapErrno();
    }

    fn checkRange(self: *Mmap, range: Range) MmapError!void {
        if (range.offset > self.len or range.length > self.len - range.offset) {
            return error.InvalidRange;
        }
    }

    /// Risk-3 escape hatch: fault every page in `range` into RAM
    /// **on the blocking pool**, so subsequent access from the
    /// calling coroutine doesn't block the worker on disk.
    ///
    /// The calling coroutine parks while a blocking-pool thread
    /// issues `madvise(MADV_WILLNEED)` and walks the range with
    /// volatile reads, page by page. When this returns, every page
    /// in `range` is RAM-resident; subsequent access (until the
    /// kernel reclaims) is non-blocking.
    pub fn prefault(self: *Mmap, range: Range) MmapError!void {
        try self.checkRange(range);
        if (range.length == 0) return;

        const Args = struct {
            ptr: [*]u8,
            offset: usize,
            length: usize,
        };
        var args = Args{ .ptr = self.ptr, .offset = range.offset, .length = range.length };
        spawnBlocking(struct {
            fn run(a: *Args) !void {
                const ptr_aligned: *align(std.heap.pageSize()) anyopaque =
                    @ptrCast(@alignCast(a.ptr + a.offset));
                _ = std.c.madvise(ptr_aligned, a.length, std.c.MADV.WILLNEED);
                forceTouchPages(@ptrCast(a.ptr + a.offset), a.length);
            }
        }.run, .{&args}) catch |err| switch (err) {
            error.Cancelled => return error.Cancelled,
            else => return error.Unexpected,
        };
    }

    pub fn remap(self: *Mmap, new_len: usize) MmapError![]u8 {
        _ = self;
        _ = new_len;
        @compileError("Mmap.remap: P4.D");
    }
};

// ─────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────

fn makeProt(perms: Perms) std.c.PROT {
    var p: std.c.PROT = .{};
    if (perms.read) p.READ = true;
    if (perms.write) p.WRITE = true;
    if (perms.exec) p.EXEC = true;
    return p;
}

fn makeFileMap(mode: Mode, populate: bool) std.c.MAP {
    var m: std.c.MAP = .{ .TYPE = switch (mode) {
        .read_only, .copy_on_write => .PRIVATE,
        .read_write => .SHARED,
    } };
    if (builtin.os.tag == .linux and populate and @hasField(std.c.MAP, "POPULATE")) {
        // Linux's MAP packed struct exposes POPULATE; set it via a
        // bit mask since the field name varies per ABI.
        @field(m, "POPULATE") = true;
    }
    return m;
}

fn makeAnonMap() std.c.MAP {
    var m: std.c.MAP = .{ .TYPE = .PRIVATE };
    @field(m, "ANONYMOUS") = true;
    return m;
}

// MS_* flags for msync. POSIX says implementation-defined; values
// differ across platforms.
const MS_ASYNC: c_int = switch (builtin.os.tag) {
    .linux => 1,
    .macos, .ios, .tvos, .watchos, .visionos, .driverkit => 1,
    else => 1,
};
const MS_SYNC: c_int = switch (builtin.os.tag) {
    .linux => 4,
    .macos, .ios, .tvos, .watchos, .visionos, .driverkit => 0x10,
    else => 4,
};

fn mmapErrno() MmapError {
    return switch (posix.errno(@as(c_int, -1))) {
        .ACCES, .PERM => error.AccessDenied,
        .INVAL => error.InvalidArgument,
        .NOMEM => error.OutOfMemory,
        .AGAIN => error.SystemResources,
        else => error.Unexpected,
    };
}

fn forceTouchPages(ptr: [*]u8, len: usize) void {
    const page_size: usize = std.heap.pageSize();
    var i: usize = 0;
    var sink: u8 = 0;
    while (i < len) : (i += page_size) {
        // Volatile read prevents the compiler from optimising the
        // touch away. The byte value is discarded.
        const v = @as(*volatile u8, @ptrCast(&ptr[i])).*;
        sink ^= v;
    }
    std.mem.doNotOptimizeAway(sink);
}
