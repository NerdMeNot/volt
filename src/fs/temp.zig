//! `volt.fs.temp` — temporary files and directories.
//!
//! `createTemp` and `mkdtemp` generate unique paths under a parent
//! directory using a random alphanumeric suffix. `TempDir` wraps a
//! created directory with RAII cleanup — `deinit()` calls
//! `tree.removeTree` to dispose of the dir and any contents.
//!
//! ## Implementation
//!
//! Volt rolls its own random-suffix retry loop instead of using
//! libc's `mkstemp`/`mkdtemp` so the surface stays portable and
//! the random bytes come from `std.crypto.random` (CSPRNG, not the
//! hash-of-pid sloppiness that some `mkstemp` impls used to ship).

const std = @import("std");
const posix = std.posix;

const File = @import("File.zig").File;
const Dir = @import("Dir.zig").Dir;
const OpenOptionsT = @import("OpenOptions.zig").OpenOptions;
const tree = @import("tree.zig");
const nanoTimestamp = @import("../time.zig").nanoTimestamp;

/// Result of `createTemp` — the open File handle plus the path the
/// caller can use to e.g. rename or remove. Caller frees `path`.
pub const TempFile = struct {
    file: File,
    /// Heap-allocated path. Caller frees with the same allocator
    /// passed to `createTemp`.
    path: []u8,
};

/// Create a unique temporary file under `dir` with the given
/// `prefix`. The file is opened read+write; mode `0o600` so other
/// users can't read it. Caller closes the File and frees the path.
pub fn createTemp(
    allocator: std.mem.Allocator,
    dir: []const u8,
    prefix: []const u8,
) !TempFile {
    const N_RETRIES = 100;
    var attempt: u32 = 0;
    while (attempt < N_RETRIES) : (attempt += 1) {
        var suffix: [12]u8 = undefined;
        randomSuffix(&suffix);

        const path = try std.fmt.allocPrint(allocator, "{s}/{s}-{s}", .{ dir, prefix, suffix });
        errdefer allocator.free(path);

        const file = (OpenOptionsT{
            .read = true,
            .write = true,
            .create = true,
            .exclusive = true,
            .mode = 0o600,
        }).open(path) catch |err| switch (err) {
            error.PathAlreadyExists => {
                allocator.free(path);
                continue;
            },
            else => return err,
        };
        return TempFile{ .file = file, .path = path };
    }
    return error.TempRetriesExceeded;
}

/// RAII handle for a temporary directory. `deinit()` removes the
/// directory and all its contents.
pub const TempDir = struct {
    dir: Dir,
    /// Heap-allocated path. Lives until `deinit`.
    path: []u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *TempDir) void {
        self.dir.close();
        // Best-effort cleanup. If the tree disappeared from under us
        // (e.g., user manually removed it), don't surface an error.
        tree.removeTree(self.allocator, self.path) catch {};
        self.allocator.free(self.path);
        self.* = undefined;
    }
};

/// Create a unique temporary directory under `parent` with the given
/// `prefix`. Returns a `TempDir` whose `deinit()` removes the
/// directory and contents.
pub fn mkdtemp(
    allocator: std.mem.Allocator,
    parent: []const u8,
    prefix: []const u8,
) !TempDir {
    const N_RETRIES = 100;
    var attempt: u32 = 0;
    while (attempt < N_RETRIES) : (attempt += 1) {
        var suffix: [12]u8 = undefined;
        randomSuffix(&suffix);

        const path = try std.fmt.allocPrint(allocator, "{s}/{s}-{s}", .{ parent, prefix, suffix });
        errdefer allocator.free(path);

        tree.makeDir(path, 0o700) catch |err| switch (err) {
            error.PathAlreadyExists => {
                allocator.free(path);
                continue;
            },
            else => return err,
        };

        const dir = Dir.open(path) catch |err| {
            // Open failed after mkdir succeeded — clean up before
            // bubbling.
            tree.removeDir(path) catch {};
            return err;
        };
        return TempDir{ .dir = dir, .path = path, .allocator = allocator };
    }
    return error.TempRetriesExceeded;
}

fn randomSuffix(out: []u8) void {
    // Seed from time XOR pid — for collision-resistant temp paths
    // (36^12 ≈ 5e18 of name space), not cryptographic uniqueness.
    const now: u64 = @bitCast(@as(i64, @truncate(nanoTimestamp())));
    const pid: u64 = @intCast(std.posix.system.getpid());
    var prng = std.Random.DefaultPrng.init(now ^ (pid << 32));
    var random = prng.random();
    for (out) |*c| {
        const v = random.uintLessThan(u8, 36);
        c.* = if (v < 10) '0' + v else 'a' + (v - 10);
    }
}

test "randomSuffix: alphanumeric output" {
    var buf: [16]u8 = undefined;
    randomSuffix(&buf);
    for (buf) |c| {
        const ok = (c >= '0' and c <= '9') or (c >= 'a' and c <= 'z');
        try std.testing.expect(ok);
    }
}
