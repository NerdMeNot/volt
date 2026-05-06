//! P0 contract stub for `volt.fs.Mmap` — full implementation lands in P4 (v1.4).
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
//!   - `MapOptions.populate = true` — Linux: `MAP_POPULATE` faults in
//!     all pages at map time. Darwin: emulated by walking + touching.
//!   - `MapOptions.locked = true` — `mlock`s the mapping; pages stay
//!     resident until `unlock` or `deinit`.
//!   - For sparse random access on a huge file, prefer
//!     `volt.fs.File.pread` (blocking-pool dispatch) over mmap. Mmap
//!     is for layout + sequential / hot data, not arbitrary RAM-backed
//!     random access on cold disk.
//!
//! Callers that hold a slice into a `Mmap` MUST NOT cache the pointer
//! across a `remap` call — see Risk #4 contract on `remap` below.
//!
//! ## Status: contracts locked, bodies pending
//!
//! Every public function below has its signature and doc-contract
//! finalised. The bodies `@compileError` so any caller fails at compile
//! time with a pointer to P4. P1/P2/P3 work that needs to *reference*
//! the type for trait integration (e.g. `Mmap.reader()` in `copy()`)
//! can do so through the type's existence; only attempts to call into
//! the unimplemented bodies break.

const std = @import("std");
const posix = std.posix;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration types
// ─────────────────────────────────────────────────────────────────────────────

/// Lifetime + visibility model. Maps to `MAP_PRIVATE` / `MAP_SHARED` /
/// `MAP_PRIVATE` (with manual COW promotion for `copy_on_write`).
/// Distinct from `Perms` so `protect()` can change page permissions
/// independently of the mapping's sharing semantics.
pub const Mode = enum {
    read_only,
    read_write,
    copy_on_write,
};

/// Page-table permission bits. Maps to `PROT_*`. Mutated by `protect()`.
pub const Perms = packed struct {
    read: bool = true,
    write: bool = false,
    exec: bool = false,
};

pub const HugePageSize = enum {
    /// `MAP_HUGE_2MB` on Linux; emulated on Darwin via `VM_FLAGS_SUPERPAGE_SIZE_2MB`.
    @"2MB",
    /// `MAP_HUGE_1GB` on Linux; not supported on Darwin (returns `error.HugePagesUnsupported`).
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
    /// `MAP_POPULATE` (Linux) / emulated (Darwin). Faults in every page
    /// at map time. Mitigates Risk #3 by front-loading the disk wait.
    populate: bool = false,
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

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

pub const MmapError = error{
    AccessDenied,
    InvalidArgument,
    SystemResources,
    OutOfMemory,
    HugePagesUnsupported,
    InvalidRange,
    /// Returned by `remap` when growth would collide with an existing
    /// adjacent mapping AND we couldn't relocate (Linux: `mremap` with
    /// `MREMAP_MAYMOVE` failed). Callers can fall back to deinit + new
    /// mapFile.
    RemapFailed,
    Cancelled,
    Unexpected,
};

// ─────────────────────────────────────────────────────────────────────────────
// The handle
// ─────────────────────────────────────────────────────────────────────────────

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
    pub fn mapFile(file: anytype, opts: MapOptions) MmapError!Mmap {
        _ = file;
        _ = opts;
        @compileError("Mmap.mapFile: not implemented yet — landing in P4 (v1.4). Contract is locked: see file header.");
    }

    /// Map an anonymous region. No backing file; useful for huge-buffer
    /// allocations and hugepage-backed scratch space.
    pub fn anonymous(len: usize, opts: AnonOptions) MmapError!Mmap {
        _ = len;
        _ = opts;
        @compileError("Mmap.anonymous: not implemented yet — landing in P4 (v1.4).");
    }

    /// Mutable view of the entire mapping.
    pub fn slice(self: *Mmap) []u8 {
        _ = self;
        @compileError("Mmap.slice: not implemented yet — landing in P4 (v1.4).");
    }

    /// Read-only view of the entire mapping.
    pub fn sliceConst(self: *const Mmap) []const u8 {
        _ = self;
        @compileError("Mmap.sliceConst: not implemented yet — landing in P4 (v1.4).");
    }

    /// `madvise` over the given range. See `Advice` for values.
    pub fn advise(self: *Mmap, range: Range, hint: Advice) MmapError!void {
        _ = self;
        _ = range;
        _ = hint;
        @compileError("Mmap.advise: not implemented yet — landing in P4 (v1.4).");
    }

    /// Risk-3 escape hatch — fault every page in `range` into RAM
    /// **on the blocking pool**, so subsequent access from the
    /// calling coroutine doesn't block the worker on disk.
    ///
    /// Implementation (P4): the calling coroutine parks; a blocking-
    /// pool thread issues `madvise(MADV_WILLNEED)` over the range,
    /// then walks every `page_size`-th byte to force resident-page
    /// allocation. Returns when every page in `range` is RAM-resident.
    pub fn prefault(self: *Mmap, range: Range) MmapError!void {
        _ = self;
        _ = range;
        @compileError("Mmap.prefault: not implemented yet — landing in P4 (v1.4). This is the Risk-3 mitigation.");
    }

    pub fn lock(self: *Mmap, range: Range) MmapError!void {
        _ = self;
        _ = range;
        @compileError("Mmap.lock: not implemented yet — landing in P4 (v1.4).");
    }

    pub fn unlock(self: *Mmap, range: Range) MmapError!void {
        _ = self;
        _ = range;
        @compileError("Mmap.unlock: not implemented yet — landing in P4 (v1.4).");
    }

    /// `msync` over the range. `sync_` blocks until pages hit storage;
    /// `async_` schedules and returns immediately.
    pub fn flush(self: *Mmap, range: Range, sync: FlushSync) MmapError!void {
        _ = self;
        _ = range;
        _ = sync;
        @compileError("Mmap.flush: not implemented yet — landing in P4 (v1.4).");
    }

    /// `mprotect` over the range with new permissions.
    pub fn protect(self: *Mmap, range: Range, perms: Perms) MmapError!void {
        _ = self;
        _ = range;
        _ = perms;
        @compileError("Mmap.protect: not implemented yet — landing in P4 (v1.4).");
    }

    /// Risk-4 contract: **resize the mapping and return a fresh slice.**
    ///
    /// The signature returns `[]u8`, not `void`, deliberately — callers
    /// MUST rebind any cached slice or pointer to the returned value.
    /// The address may have moved:
    ///   - Darwin: `mremap` does not exist. Implementation always
    ///     unmaps + re-maps, so the address ALWAYS moves on grow.
    ///   - Linux: `mremap(MREMAP_MAYMOVE)` may move at the kernel's
    ///     discretion when growing in-place isn't possible.
    /// Treat any cached pointer into the old mapping as invalid the
    /// instant `remap` returns. The returned slice is the only valid
    /// way to access the resized region.
    ///
    /// Shrinking (`new_len < self.len`) does not move the address,
    /// but the contract is uniform across platforms — always rebind.
    pub fn remap(self: *Mmap, new_len: usize) MmapError![]u8 {
        _ = self;
        _ = new_len;
        @compileError("Mmap.remap: not implemented yet — landing in P4 (v1.4). Contract: returns the new slice; callers MUST rebind.");
    }

    /// Unmap and close the backing fd (if any). Idempotent: calling on
    /// a `Mmap` whose `ptr` is sentinel-null is a no-op.
    pub fn deinit(self: *Mmap) void {
        _ = self;
        @compileError("Mmap.deinit: not implemented yet — landing in P4 (v1.4).");
    }
};
