//! POC-F — comptime-specialized SPSC ring buffer.
//!
//! Single Producer Single Consumer, lock-free. Cache-line padding
//! between head and tail to avoid false sharing. cap must be power of 2
//! for the cheap masked-index trick.
//!
//! Operations (4 atomic ops total per send+recv pair):
//!   send: load tail (acq), write slot, store head (rel)
//!   recv: load head (acq), read slot, store tail (rel)
//!
//! Compare to Vyukov MPMC: per slot has a sequence atomic, both sides
//! CAS the sequence — ~12 atomics per pair. The SPSC fast path
//! eliminates the MPMC machinery.
//!
//! On Full/Empty: caller spins (POC). A real impl would park-on-block.

const std = @import("std");

const CACHE_LINE = 128;

pub fn Spsc(comptime T: type, comptime cap: usize) type {
    comptime {
        std.debug.assert(cap > 0 and (cap & (cap - 1)) == 0);
    }
    return struct {
        const Self = @This();
        const MASK = cap - 1;

        ring: [cap]T align(CACHE_LINE) = undefined,

        // Producer-owned cache line.
        head: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),

        // Consumer-owned cache line.
        tail: std.atomic.Value(u64) align(CACHE_LINE) = std.atomic.Value(u64).init(0),

        /// Producer side. Spin on full.
        pub fn send(self: *Self, v: T) void {
            const h = self.head.load(.monotonic);
            // Wait while full.
            while (h - self.tail.load(.acquire) >= cap) {
                std.atomic.spinLoopHint();
            }
            self.ring[h & MASK] = v;
            self.head.store(h + 1, .release);
        }

        /// Consumer side. Spin on empty.
        pub fn recv(self: *Self) T {
            const t = self.tail.load(.monotonic);
            while (self.head.load(.acquire) == t) {
                std.atomic.spinLoopHint();
            }
            const v = self.ring[t & MASK];
            self.tail.store(t + 1, .release);
            return v;
        }
    };
}
