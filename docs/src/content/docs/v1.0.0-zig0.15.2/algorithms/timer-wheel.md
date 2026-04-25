---
title: Hierarchical Timer Wheel
description: How Volt manages timer expiration with O(1) insertion and
  bit-manipulation optimizations, using a 5-level hierarchical timing wheel.
slug: v1.0.0-zig0.15.2/algorithms/timer-wheel
---

Timer management is critical for an async runtime. Every `sleep()`, `timeout()`, and `interval()` call registers a timer that must fire at the right time. Volt uses a hierarchical timing wheel -- a data structure that provides O(1) timer insertion and O(1) expiration checking through bit-manipulation tricks.

## The Problem with Naive Approaches

A priority queue (heap) gives O(log N) insertion and O(log N) extraction. For a runtime managing millions of timers, the logarithmic factor adds up. Worse, heap operations have poor cache behavior due to pointer chasing.

A sorted list gives O(N) insertion. A skip list gives O(log N) probabilistically. Both are unsuitable for the hot path of an async runtime.

The timing wheel trades space for time: by quantizing time into discrete "slots" and using multiple levels for different time scales, it achieves constant-time operations.

## Wheel Architecture

Volt's timer wheel has 5 levels, each with 64 slots. Each level covers a larger time range with coarser granularity:

```
Level 0:   64 slots x   1ms  =     64ms   range
Level 1:   64 slots x  64ms  =    4.1s    range
Level 2:   64 slots x  4.1s  =    4.3min  range
Level 3:   64 slots x  4.3m  =    4.6hr   range
Level 4:   64 slots x  4.6h  =   12.4day  range
Overflow:  linked list for timers beyond ~12 days
```

The slot durations are precomputed at compile time:

```zig
const SLOT_DURATIONS: [NUM_LEVELS]u64 = blk: {
    var durations: [NUM_LEVELS]u64 = undefined;
    var duration: u64 = LEVEL_0_SLOT_NS;  // 1ms
    for (0..NUM_LEVELS) |i| {
        durations[i] = duration;
        duration *= SLOTS_PER_LEVEL;      // x64 per level
    }
    break :blk durations;
};
```

Each level has:

* **64 slots**: Each slot is a doubly-linked list of timer entries.
* **A current index**: Points to the slot corresponding to "now" at this level's granularity.
* **A 64-bit occupancy bitfield**: Bit `i` is set if slot `i` contains at least one timer.

```
Level 0 (1ms resolution):
                  occupied = 0b...0010_0000_0101
 current
    |
    v
+---+---+---+---+---+---+---+   ...   +---+
| * |   | * |   |   | * |   |         |   |
+---+---+---+---+---+---+---+   ...   +---+
  0   1   2   3   4   5   6             63

  * = slot contains one or more timer entries
```

## O(1) Timer Insertion

When inserting a timer with a given deadline, the wheel must determine which level and slot to use. The naive approach scans levels linearly (O(NUM\_LEVELS)). Volt uses bit manipulation for O(1) level selection.

### Level Selection via `@clz`

The key insight: if the current time and the deadline differ in their most significant bits, the timer belongs at a higher level. The XOR of the two times gives the differing bits, and `@clz` (count leading zeros) finds the most significant difference.

```zig
inline fn levelFor(now_ns: u64, deadline_ns: u64) usize {
    const elapsed_ticks = now_ns / LEVEL_0_SLOT_NS;
    const when_ticks = deadline_ns / LEVEL_0_SLOT_NS;

    // XOR reveals which bits differ between now and deadline
    // OR with SLOT_MASK ensures we consider at least level 0
    const masked = (elapsed_ticks ^ when_ticks) | SLOT_MASK;

    if (masked >= MAX_WHEEL_DURATION / LEVEL_0_SLOT_NS) {
        return NUM_LEVELS;  // Overflow
    }

    const leading_zeros = @clz(masked);
    const significant_bit = 63 - leading_zeros;

    // Each level handles 6 bits (log2(64) = 6)
    return significant_bit / BITS_PER_LEVEL;
}
```

**Example:** If `now = 0ms` and `deadline = 150ms`:

* `elapsed_ticks = 0`, `when_ticks = 150`
* `XOR = 150 = 0b1001_0110`, `|SLOT_MASK = 0b1011_1111`
* `@clz(0b1011_1111) = 56` (for u64), `significant_bit = 7`
* `level = 7 / 6 = 1` -- Level 1 (64ms resolution), correct since 150ms is within Level 1's 4.1s range.

### Slot Placement

Once the level is determined, the slot within that level is computed as a simple division and modulo:

```zig
pub fn slotFor(deadline_ns: u64, level_idx: usize, now_ns: u64) usize {
    const slot_duration = slotDuration(level_idx);
    const delta = deadline_ns -| now_ns;
    const slot_offset = delta / slot_duration;
    return @intCast(slot_offset % SLOTS_PER_LEVEL);
}
```

Insertion then adds the entry to the slot's linked list and sets the occupancy bit:

```zig
pub fn addEntry(self: *Level, slot_idx: usize, entry: *TimerEntry) void {
    self.slots[slot_idx].push(entry);
    self.occupied |= (@as(u64, 1) << @intCast(slot_idx));
}
```

## O(1) Next-Expiry Lookup

Finding the next timer to fire uses `@ctz` (count trailing zeros) on the occupancy bitfield, starting from the current slot position:

```zig
pub fn nextOccupiedSlot(self: *const Level, from_slot: usize) ?usize {
    if (self.occupied == 0) return null;

    // Mask for slots >= from_slot
    const from_mask: u64 = ~((@as(u64, 1) << @intCast(from_slot)) - 1);
    const masked = self.occupied & from_mask;

    if (masked != 0) {
        return @ctz(masked);  // First occupied slot at or after current
    }

    // Wrap around: check slots before from_slot
    const wrap_mask: u64 = (@as(u64, 1) << @intCast(from_slot)) - 1;
    const wrapped = self.occupied & wrap_mask;
    if (wrapped != 0) {
        return @ctz(wrapped);
    }

    return null;
}
```

This is a single bitwise AND plus a `@ctz` instruction -- O(1) regardless of how many timers are registered.

## Cascading

Higher-level timers are placed with coarse granularity. As time advances, they must be moved to lower levels for precise firing. This is called **cascading**.

When level 0's current slot wraps around to 0 (every 64ms), timers from level 1's current slot are cascaded down. If level 1 also wraps, level 2 cascades, and so on.

```
Before cascade (level 0 wraps):

  Level 1, current slot 3:
  +---+---+---+---+---+
  |   |   |   |T,U|   |  ...
  +---+---+---+---+---+

After cascade:
  Timers T and U are re-inserted at level 0 with precise slot placement.
  They may end up in different level 0 slots depending on their exact deadlines.
```

```zig
fn cascade(self: *Self, level_idx: usize) void {
    if (level_idx >= NUM_LEVELS) return;

    const level = &self.levels[level_idx];
    const slot_idx = level.current;
    level.current = (slot_idx + 1) % SLOTS_PER_LEVEL;

    // Recursive cascade if this level also wraps
    if (level.current == 0 and level_idx + 1 < NUM_LEVELS) {
        self.cascade(level_idx + 1);
    }

    // Move all entries from this slot to lower levels
    var entry = level.takeAllFromSlot(slot_idx);
    while (entry) |e| {
        const next = e.next;
        e.next = null;
        e.prev = null;
        if (e.cancelled) {
            self.count -|= 1;
        } else {
            self.insertInternal(e);  // Re-insert at lower level
        }
        entry = next;
    }
}
```

Cascading is triggered during `poll()`:

```zig
pub fn poll(self: *Self) ?*TimerEntry {
    self.updateTime();

    const level0_slot = (self.now_ns / LEVEL_0_SLOT_NS) % SLOTS_PER_LEVEL;
    var current = self.levels[0].current;

    while (current != level0_slot) {
        // Collect all expired entries from this slot
        const entries = self.levels[0].takeAllFromSlot(current);
        self.appendExpired(&expired_head, &expired_tail, entries);

        current = (current + 1) % SLOTS_PER_LEVEL;

        // Cascade from higher levels when slot 0 wraps
        if (current == 0) {
            self.cascade(1);
        }
    }
    // ...
}
```

## Timer Entries (Intrusive, Zero-Allocation)

Timer entries are intrusive -- they contain their own linked-list pointers. This means inserting a timer requires no heap allocation if the caller provides the entry on the stack or embeds it in their struct:

```zig
pub const TimerEntry = struct {
    deadline_ns: u64,
    task: ?*Header,          // Task-based waking
    waker: ?WakerFn,         // Generic waker callback
    waker_ctx: ?*anyopaque,
    user_data: u64,
    next: ?*TimerEntry,      // Intrusive doubly-linked list
    prev: ?*TimerEntry,
    cancelled: bool,
    heap_allocated: bool,    // If true, wheel frees on expiry
};
```

The driver provides both allocation modes:

```zig
// Heap-allocated (driver manages lifetime)
var handle = try driver.registerSleep(Duration.fromMillis(100), ctx, wake_fn);

// Stack-allocated (caller manages lifetime)
var entry: TimerEntry = undefined;
driver.registerSleepEntry(&entry, Duration.fromMillis(100), ctx, wake_fn);
```

Cancellation sets a flag rather than removing the entry from the list. Cancelled entries are skipped during poll and cleaned up during cascading:

```zig
pub fn remove(_: *Self, entry: *TimerEntry) void {
    entry.cancel();  // Lazy removal
}
```

## Integration with the Scheduler

Worker 0 is the primary timer driver. It polls the timer wheel at the start of each tick. Other workers do not poll timers, avoiding contention on the timer mutex:

```zig
// In Worker.run():
if (self.index == 0) {
    _ = self.scheduler.pollTimers();
}
```

When Worker 0 parks, it uses the next timer expiration as its park timeout, ensuring timers fire with bounded latency even when no I/O events are pending:

```zig
const park_timeout: u64 = if (self.index == 0)
    self.scheduler.nextTimerExpiration() orelse PARK_TIMEOUT_NS
else
    PARK_TIMEOUT_NS;
```

## Comparison with Other Timer Approaches

| Approach | Insert | Fire | Cancel | Memory |
|----------|--------|------|--------|--------|
| Hierarchical wheel (Volt) | O(1) | O(1) amortized | O(1) lazy | Fixed 5 \* 64 slots |
| Binary heap | O(log N) | O(log N) | O(N) or O(log N) with index | Dynamic |
| Red-black tree | O(log N) | O(log N) | O(log N) | Per-node allocation |
| Skip list | O(log N) expected | O(1) | O(log N) expected | Per-node allocation |
| Hashed wheel (Kafka) | O(1) | O(1) | O(1) | Fixed but single-level |

The hierarchical wheel's main advantage is constant-time operations with bounded memory. The cascading cost is amortized: a level-1 cascade happens every 64ms, a level-2 cascade every 4 seconds, and so on. In practice, most timers are short-lived (milliseconds to seconds) and never reach higher levels.

The main limitation is resolution: level 0 has 1ms granularity. Timers cannot fire with sub-millisecond precision. For the use cases of an async I/O runtime (network timeouts, sleep, intervals), 1ms resolution is more than sufficient.

### Source Files

* Timer wheel: `src/internal/scheduler/TimerWheel.zig`
* Timer driver: `src/time/driver.zig`

## References

* George Varghese and Tony Lauck, "Hashed and Hierarchical Timing Wheels: Data Structures for the Efficient Implementation of a Timer Facility," *IEEE/ACM Transactions on Networking*, 5(6):824–834, 1997. [doi:10.1109/90.650142](https://doi.org/10.1109/90.650142)
* Tokio timer implementation: [`tokio/src/runtime/time`](https://github.com/tokio-rs/tokio/tree/master/tokio/src/runtime/time)
* Kafka's hierarchical timing wheel: [`kafka/server/src/main/java/kafka/utils/timer`](https://github.com/apache/kafka/tree/trunk/server/src/main/java/kafka/utils/timer)
