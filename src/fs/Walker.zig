//! P0 contract stub for `volt.fs.Walker` — full implementation lands in P3 (v1.3).
//!
//! Recursive directory iterator. Yields one `Entry` per file/directory
//! reached, depth-first. The full type lives in P3 alongside `Dir` and
//! `DirEntry` — but two API contracts are locked here:
//!
//! ## Risk #5 contracts (locked)
//!
//! 1. **Bounded depth.** `WalkOptions.max_depth` defaults to 4096 and
//!    is the absolute cap. If a tree is deeper than `max_depth`, the
//!    iterator returns `error.MaxDepthExceeded` rather than blowing
//!    the allocator-backed stack. The walker uses an explicit allocator-
//!    backed stack of `Dir` handles, never recursion.
//!
//! 2. **`skipSubtree()` is part of the API.** User-side filters can
//!    prune cheaply mid-traversal — common case is excluding `.git/`,
//!    `node_modules/`, `target/`, `.zig-cache/`, etc. without paying
//!    to descend. Calling `skipSubtree` after a directory `Entry`
//!    causes the iterator to NOT descend into it.
//!
//! 3. **Symlink-loop guard.** `WalkOptions.follow_symlinks` defaults to
//!    `false`. When `true`, the walker tracks visited inodes and
//!    surfaces `error.SymLinkLoop` when a cycle is detected.
//!
//! ## Status: contracts locked, bodies pending
//!
//! Bodies `@compileError`. Calling fails at compile time with a pointer
//! to P3.

const std = @import("std");
const posix = std.posix;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

pub const WalkOptions = struct {
    /// Maximum recursion depth. The walker never blows the stack —
    /// when a tree exceeds this, `next()` returns `error.MaxDepthExceeded`.
    /// Default 4096 is generous enough for any sane filesystem and
    /// strict enough that pathological / hostile inputs surface
    /// quickly. Risk #5 mitigation.
    max_depth: u32 = 4096,

    /// Follow symbolic links during traversal. Default `false` — when
    /// `true`, the walker tracks visited inodes to detect cycles and
    /// returns `error.SymLinkLoop` on detection.
    follow_symlinks: bool = false,

    /// If `true`, hidden entries (names starting with `.`) are skipped
    /// without yielding. The walker still descends into hidden
    /// directories if their parent passed; this flag governs which
    /// entries the iterator surfaces, not which directories it enters.
    skip_hidden: bool = false,
};

pub const Kind = enum {
    file,
    directory,
    symlink,
    block_device,
    character_device,
    named_pipe,
    unix_domain_socket,
    unknown,
};

pub const Entry = struct {
    /// Path relative to the root the walker was opened on. Borrowed
    /// from the walker's internal buffer — copy if you need it past
    /// the next `next()` call.
    path: []const u8,
    name: []const u8,
    kind: Kind,
    depth: u32,
};

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

pub const WalkError = error{
    AccessDenied,
    FileNotFound,
    NotDir,
    NameTooLong,
    SystemResources,
    OutOfMemory,
    /// Tree is deeper than `WalkOptions.max_depth`. Risk #5.
    MaxDepthExceeded,
    /// Symlink cycle detected (only when `follow_symlinks = true`).
    SymLinkLoop,
    Cancelled,
    Unexpected,
};

// ─────────────────────────────────────────────────────────────────────────────
// The walker
// ─────────────────────────────────────────────────────────────────────────────

pub const Walker = struct {
    /// Open a recursive walker rooted at `path`.
    pub fn open(allocator: std.mem.Allocator, path: []const u8, opts: WalkOptions) WalkError!Walker {
        _ = allocator;
        _ = path;
        _ = opts;
        @compileError("Walker.open: not implemented yet — landing in P3 (v1.3).");
    }

    /// Yield the next entry, or `null` when the tree is exhausted.
    /// Lifetime: the returned `Entry`'s `path` and `name` slices are
    /// borrowed from the walker's internal buffer and become invalid
    /// at the next `next()` call.
    pub fn next(self: *Walker) WalkError!?Entry {
        _ = self;
        @compileError("Walker.next: not implemented yet — landing in P3 (v1.3).");
    }

    /// Tell the walker NOT to descend into the most recently yielded
    /// directory. Common case: skip `.git`, `node_modules`, etc.
    /// No-op if the most recent yield was not a directory or if
    /// nothing has been yielded yet.
    ///
    /// Risk #5 mitigation: lets users prune subtrees cheaply rather
    /// than filtering after the fact.
    pub fn skipSubtree(self: *Walker) void {
        _ = self;
        @compileError("Walker.skipSubtree: not implemented yet — landing in P3 (v1.3).");
    }

    /// Close the walker and free the iteration stack. Idempotent.
    pub fn deinit(self: *Walker) void {
        _ = self;
        @compileError("Walker.deinit: not implemented yet — landing in P3 (v1.3).");
    }
};
