//! Race-of-N primitives — winner-slot CAS shared by `select`,
//! `withTimeout`, and (Wave 2.1) `joinFirst`.
//!
//! Each primitive in the runtime that "spawns N racers and takes
//! whichever finishes first" needs the same arbitration: a single
//! atomic slot that exactly one racer wins via CAS, plus a Cancel
//! that the winner fires to let the losers unwind cleanly.
//!
//! Before this module existed, `select.zig` and `lib.zig:withTimeout`
//! re-implemented the slot independently (with different enum
//! shapes and slightly different ordering). Wave 1.3 extracts the
//! shared piece so a third copy never has to be written.
//!
//! What's NOT here: the spawn shape itself. `select` spawns N
//! children with `branch.run(*Cancel)` shapes; `withTimeout` spawns
//! one timer child and runs the body on the caller's thread;
//! `joinFirst` won't spawn at all — it takes pre-spawned `*Task`
//! handles. Forcing a single "race" primitive that covers all three
//! would over-abstract; each caller composes `WinnerSlot` with its
//! own spawn shape.

const std = @import("std");

/// CAS-arbitrated winner slot. Exactly one racer's `claim()`
/// returns `true`; the rest return `false`. Pair with
/// `Cancel.fire()` on the winning side to release the losers.
///
/// `N` is the number of racers; the slot encodes an index in
/// `[0, N)` or the sentinel "unclaimed."
pub fn WinnerSlot(comptime N: usize) type {
    comptime {
        if (N == 0) @compileError("WinnerSlot requires at least one racer");
    }
    return struct {
        const Self = @This();

        /// Sentinel for "no racer has claimed the slot yet." Picked
        /// as the max u32 so any valid index `[0, N)` (with
        /// `N <= maxInt(u32) - 1`) is unambiguous. The atomic is u32
        /// because every supported arch has cheap 32-bit CAS.
        pub const UNSET: u32 = std.math.maxInt(u32);

        comptime {
            if (N > UNSET) @compileError("WinnerSlot supports up to maxInt(u32) - 1 racers");
        }

        state: std.atomic.Value(u32) = std.atomic.Value(u32).init(UNSET),

        /// Try to claim the win for racer `idx`. Returns `true` if
        /// this call set the slot (caller is the winner); `false` if
        /// someone else already won.
        ///
        /// Memory ordering: `.acq_rel` on success publishes any
        /// writes the winner did before the claim (e.g. writing its
        /// result into a shared slot — readers will see it via the
        /// matching `.acquire` load on `winner()`).
        pub fn claim(self: *Self, idx: usize) bool {
            std.debug.assert(idx < N);
            return self.state.cmpxchgStrong(
                UNSET,
                @intCast(idx),
                .acq_rel,
                .acquire,
            ) == null;
        }

        /// Read the winning racer's index, or `null` if unclaimed.
        /// `.acquire` pairs with the winner's `.acq_rel` CAS — any
        /// writes the winner published before `claim()` returned
        /// true are visible to readers that observe the index.
        pub fn winner(self: *const Self) ?usize {
            const s = self.state.load(.acquire);
            return if (s == UNSET) null else s;
        }
    };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "WinnerSlot: first claim wins, second returns false" {
    var slot = WinnerSlot(4){};
    try std.testing.expect(slot.winner() == null);
    try std.testing.expect(slot.claim(2));
    try std.testing.expect(!slot.claim(0));
    try std.testing.expect(!slot.claim(3));
    try std.testing.expectEqual(@as(?usize, 2), slot.winner());
}

test "WinnerSlot: concurrent claims — exactly one winner" {
    var slot = WinnerSlot(8){};
    var winners: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

    const Worker = struct {
        fn run(s: *WinnerSlot(8), w: *std.atomic.Value(u32), idx: usize) void {
            // Spin briefly so threads actually race rather than
            // serializing on spawn cost.
            var i: u32 = 0;
            while (i < 100) : (i += 1) std.atomic.spinLoopHint();
            if (s.claim(idx)) _ = w.fetchAdd(1, .acq_rel);
        }
    };

    var threads: [8]std.Thread = undefined;
    for (&threads, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, Worker.run, .{ &slot, &winners, i });
    }
    for (threads) |t| t.join();

    try std.testing.expectEqual(@as(u32, 1), winners.load(.acquire));
    try std.testing.expect(slot.winner() != null);
}
