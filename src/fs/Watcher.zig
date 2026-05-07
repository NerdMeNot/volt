//! `volt.fs.Watcher` — async filesystem change notifications.
//!
//! Single-directory, non-recursive watcher. Yields events as files
//! are created / deleted / modified inside the watched directory.
//!
//! ## Platform asymmetry (honest)
//!
//! - **Linux** (inotify): per-event granularity. Each event carries
//!   the file name and a precise mask. The inotify fd is just a
//!   readable fd — Volt's existing reactor handles the async wait.
//!
//! - **Darwin** (kqueue + EVFILT_VNODE): coarse-grained "directory
//!   changed" events without per-file detail. Each `next()` call
//!   dispatches a `kevent` syscall on the blocking pool, which
//!   blocks until something happens. The `name` field of returned
//!   events is empty; the event mask is `.modified`. Per-file
//!   granularity on Darwin would require re-walking and diffing
//!   each event — a v1.2 follow-up.
//!
//! - **Other platforms**: `error.Unsupported`.
//!
//! ## Lifetime
//!
//! `next()` returns null on clean EOF (currently unreachable —
//! watchers run until `deinit`). `deinit()` closes the inotify /
//! kqueue fd; if a coroutine is parked inside `next()` at that
//! moment, the syscall surfaces an error.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const wait = @import("../io/wait.zig");
const lowlevel = @import("../io/io.zig");
const spawnBlocking = @import("../api/spawn_blocking.zig").spawnBlocking;

pub const EventMask = packed struct {
    created: bool = false,
    deleted: bool = false,
    modified: bool = false,
    renamed: bool = false,
};

pub const Event = struct {
    kind: EventMask,
    /// Path of the changed entry, relative to the watched
    /// directory. Empty on Darwin (kqueue can't tell us).
    name: []const u8,
};

pub const WatchError = error{
    AccessDenied,
    FileNotFound,
    NotDir,
    NameTooLong,
    SystemResources,
    OutOfMemory,
    Unsupported,
    Cancelled,
    Unexpected,
};

pub const Watcher = switch (builtin.os.tag) {
    .linux => LinuxImpl,
    .macos, .ios, .tvos, .watchos, .visionos, .driverkit => DarwinImpl,
    else => UnsupportedImpl,
};

// ─────────────────────────────────────────────────────────────────────
// Linux — inotify
// ─────────────────────────────────────────────────────────────────────

const LinuxImpl = struct {
    inotify_fd: posix.fd_t,
    /// Buffer for accumulated inotify event records. Drained by
    /// `next()` one event at a time.
    buf: [4096]u8 = undefined,
    buf_len: usize = 0,
    buf_pos: usize = 0,

    pub fn open(path: []const u8) WatchError!LinuxImpl {
        if (builtin.os.tag != .linux) return error.Unsupported;
        var path_buf: [4096]u8 = undefined;
        if (path.len >= path_buf.len) return error.NameTooLong;
        const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

        // IN_CLOEXEC = 0x80000, IN_NONBLOCK = 0x800 — declared via
        // bit-offset of O.* flags in std.os.linux. Hard-code here
        // since we link libc and don't pull in std.os.linux.
        const IN_CLOEXEC: c_uint = 0o2000000;
        const IN_NONBLOCK: c_uint = 0o4000;
        const fd = std.c.inotify_init1(IN_CLOEXEC | IN_NONBLOCK);
        if (fd < 0) return error.SystemResources;

        const IN_CREATE: u32 = 0x100;
        const IN_DELETE: u32 = 0x200;
        const IN_MODIFY: u32 = 0x002;
        const IN_MOVED_FROM: u32 = 0x040;
        const IN_MOVED_TO: u32 = 0x080;
        const mask = IN_CREATE | IN_DELETE | IN_MODIFY | IN_MOVED_FROM | IN_MOVED_TO;

        const wd = std.c.inotify_add_watch(fd, path_z.ptr, mask);
        if (wd < 0) {
            _ = std.c.close(fd);
            return switch (posix.errno(wd)) {
                .ACCES => error.AccessDenied,
                .NOENT => error.FileNotFound,
                .NOTDIR => error.NotDir,
                .NOMEM => error.OutOfMemory,
                else => error.Unexpected,
            };
        }

        return .{ .inotify_fd = fd };
    }

    pub fn next(self: *LinuxImpl) WatchError!?Event {
        // Drain pending events from the buffer first.
        while (true) {
            if (self.buf_pos >= self.buf_len) {
                // Buffer empty — read more from inotify fd. Parks on
                // EAGAIN via the reactor (inotify fd is non-blocking).
                const n = lowlevel.read(self.inotify_fd, &self.buf) catch |err| switch (err) {
                    error.Cancelled => return error.Cancelled,
                    else => return error.Unexpected,
                };
                if (n == 0) return null;
                self.buf_len = n;
                self.buf_pos = 0;
            }

            const rec = parseInotifyEvent(self.buf[self.buf_pos..self.buf_len]) orelse {
                // Truncated record — discard and try again.
                self.buf_pos = self.buf_len;
                continue;
            };
            self.buf_pos += rec.size;

            const mask = rec.mask;
            const kind = EventMask{
                .created = (mask & 0x100) != 0,
                .deleted = (mask & 0x200) != 0,
                .modified = (mask & 0x002) != 0,
                .renamed = (mask & 0x040) != 0 or (mask & 0x080) != 0,
            };
            // Skip housekeeping events (IN_IGNORED, IN_Q_OVERFLOW, etc.).
            if (!kind.created and !kind.deleted and !kind.modified and !kind.renamed) continue;
            return Event{ .kind = kind, .name = rec.name };
        }
    }

    pub fn deinit(self: *LinuxImpl) void {
        _ = std.c.close(self.inotify_fd);
        self.* = undefined;
    }
};

const InotifyRecord = struct {
    mask: u32,
    name: []const u8,
    /// Total bytes consumed by this record (header + name padding).
    size: usize,
};

/// Parse one inotify_event record from `buf`. Returns null if the
/// buffer doesn't hold a complete record.
fn parseInotifyEvent(buf: []const u8) ?InotifyRecord {
    // struct inotify_event { __s32 wd; __u32 mask; __u32 cookie; __u32 len; char name[]; }
    const HEADER_SIZE = 16;
    if (buf.len < HEADER_SIZE) return null;
    const mask = std.mem.bytesToValue(u32, buf[4..8]);
    const name_len = std.mem.bytesToValue(u32, buf[12..16]);
    const total = HEADER_SIZE + name_len;
    if (buf.len < total) return null;
    const name_field = buf[HEADER_SIZE..total];
    const name = std.mem.sliceTo(name_field, 0);
    return .{ .mask = mask, .name = name, .size = total };
}

// ─────────────────────────────────────────────────────────────────────
// Darwin — kqueue EVFILT_VNODE
// ─────────────────────────────────────────────────────────────────────

const DarwinImpl = struct {
    kq: c_int,
    dirfd: posix.fd_t,

    pub fn open(path: []const u8) WatchError!DarwinImpl {
        var path_buf: [4096]u8 = undefined;
        if (path.len >= path_buf.len) return error.NameTooLong;
        const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

        const flags = posix.O{
            .ACCMODE = .RDONLY,
            .DIRECTORY = true,
            .CLOEXEC = true,
        };
        const dirfd = posix.openatZ(posix.AT.FDCWD, path_z.ptr, flags, 0) catch return error.Unexpected;

        const kq = syscall.kqueue() catch {
            syscall.close(dirfd);
            return error.SystemResources;
        };

        // EVFILT_VNODE on the dir fd; fire on write / delete / rename.
        const NOTE_WRITE: u32 = 0x00000002;
        const NOTE_DELETE: u32 = 0x00000001;
        const NOTE_RENAME: u32 = 0x00000020;
        const NOTE_ATTRIB: u32 = 0x00000008;
        const fflags = NOTE_WRITE | NOTE_DELETE | NOTE_RENAME | NOTE_ATTRIB;

        const ev = posix.Kevent{
            .ident = @intCast(dirfd),
            .filter = -4, // EVFILT_VNODE
            .flags = 0x0001 | 0x0010, // EV_ADD | EV_CLEAR (edge-triggered)
            .fflags = fflags,
            .data = 0,
            .udata = 0,
        };
        const changes = [_]posix.Kevent{ev};
        var dummy: [0]posix.Kevent = undefined;
        _ = syscall.kevent(kq, &changes, &dummy, null) catch {
            syscall.close(kq);
            syscall.close(dirfd);
            return error.Unexpected;
        };

        return .{ .kq = kq, .dirfd = dirfd };
    }

    pub fn next(self: *DarwinImpl) WatchError!?Event {
        // Block on kevent in the blocking pool — caller's coroutine
        // parks until the kqueue fires.
        const Args = struct { kq: c_int };
        var args = Args{ .kq = self.kq };
        const fired = spawnBlocking(struct {
            fn run(a: *Args) !u32 {
                var events: [1]posix.Kevent = undefined;
                const changes: []const posix.Kevent = &.{};
                const n = try syscall.kevent(a.kq, changes, &events, null);
                if (n == 0) return 0;
                return events[0].fflags;
            }
        }.run, .{&args}) catch |err| switch (err) {
            error.Cancelled => return error.Cancelled,
            else => return error.Unexpected,
        };
        if (fired == 0) return null;

        const NOTE_WRITE: u32 = 0x00000002;
        const NOTE_DELETE: u32 = 0x00000001;
        const NOTE_RENAME: u32 = 0x00000020;
        return Event{
            .kind = .{
                .modified = (fired & NOTE_WRITE) != 0,
                .deleted = (fired & NOTE_DELETE) != 0,
                .renamed = (fired & NOTE_RENAME) != 0,
            },
            .name = "",
        };
    }

    pub fn deinit(self: *DarwinImpl) void {
        syscall.close(self.kq);
        syscall.close(self.dirfd);
        self.* = undefined;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Unsupported platforms
// ─────────────────────────────────────────────────────────────────────

const UnsupportedImpl = struct {
    pub fn open(path: []const u8) WatchError!UnsupportedImpl {
        _ = path;
        return error.Unsupported;
    }
    pub fn next(self: *UnsupportedImpl) WatchError!?Event {
        _ = self;
        return error.Unsupported;
    }
    pub fn deinit(self: *UnsupportedImpl) void {
        _ = self;
    }
};
