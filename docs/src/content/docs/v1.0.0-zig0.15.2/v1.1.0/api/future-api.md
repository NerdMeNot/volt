---
title: Future API
description: The low-level Future engine -- PollResult, Context, Waker, and
  combinator types for building custom futures.
slug: v1.0.0-zig0.15.2/v110/api/future-api
---

:::caution[Engine Internals]
The types on this page (`volt.future.PollResult`, `volt.future.Context`, `volt.future.Waker`, etc.) are **not part of the user-facing API**. They are the low-level engine that powers `io.@"async"`, sync primitives, channels, and I/O operations internally. You only need these if you are implementing a **custom Future type** -- for example, a retry wrapper, a timeout combinator, or a new sync primitive.

Most applications should use `io.@"async"()`, `io.concurrent()`, and the convenience methods on sync primitives (`mutex.lock(io)`, `channel.send(io, value)`) instead.
:::

The Future engine is the foundation of all async operations in Volt. Every sync primitive, channel, timer, and I/O operation produces a Future internally.

## `volt.future.PollResult`

```zig
pub fn PollResult(comptime T: type) type {
    return union(enum) {
        ready: T,
        pending: void,
    };
}
```

The result of polling a future. Either the value is ready, or the future is still pending.

```zig
const result = my_future.poll(&ctx);
if (result == .ready) {
    const value = result.ready;
}
```

## `volt.future.Context`

```zig
pub const Context = struct {
    waker: *const Waker,

    pub fn getWaker(self: *Context) Waker { ... }
};
```

Passed to `poll()`. Contains the waker that the future must use to signal when it can make progress.

## `volt.future.Waker`

The mechanism for waking a suspended task. When a future returns `.pending`, it must store the waker and call `waker.wakeByRef()` when the operation completes.

```zig
pub const Waker = struct {
    raw: RawWaker,

    pub fn wakeByRef(self: *const Waker) void { ... }
    pub fn clone(self: *const Waker) Waker { ... }
    pub fn deinit(self: *Waker) void { ... }
    pub fn willWakeSame(self: *const Waker, other: Waker) bool { ... }
};
```

## Future Trait

Any type `F` is a valid Future if it has:

```zig
pub const Output = T;
pub fn poll(self: *F, ctx: *Context) PollResult(T);
```

Check at comptime with `volt.future.isFuture(F)`.

Here is a complete example of implementing a custom Future. This one retries an operation up to a fixed number of times before giving up:

```zig
const volt = @import("volt");

/// A future that polls an inner future, retrying on failure up to
/// `max_retries` times. Each retry re-initializes the inner future.
fn RetryFuture(comptime Inner: type) type {
    return struct {
        const Self = @This();
        pub const Output = Inner.Output;

        inner: Inner,
        create_fn: *const fn () Inner,
        retries_left: u32,

        pub fn init(create: *const fn () Inner, max_retries: u32) Self {
            return .{
                .inner = create(),
                .create_fn = create,
                .retries_left = max_retries,
            };
        }

        pub fn poll(self: *Self, ctx: *volt.future.Context) volt.future.PollResult(Output) {
            const result = self.inner.poll(ctx);
            switch (result) {
                .ready => return result,
                .pending => {
                    // Inner future is not done yet. If it signals an
                    // error state internally, we could retry here.
                    // For now, just propagate pending.
                    if (self.retries_left > 0) {
                        self.retries_left -= 1;
                        self.inner = self.create_fn();
                        // Immediately re-poll the fresh future
                        return self.inner.poll(ctx);
                    }
                    return .pending;
                },
            }
        }
    };
}
```

## Built-in Futures

| Type | Description |
|------|-------------|
| `Ready(T)` | Immediately ready with a value |
| `Pending(T)` | Never completes |
| `Lazy(F)` | Computes on first poll |
| `FnFuture(func, Args)` | Wraps a plain function as a single-poll future |
| `MapFuture(F, G)` | Transform a future's output |
| `AndThenFuture(F, G)` | Chain two futures sequentially |

```zig
const volt = @import("volt");

// Ready: a future that resolves immediately.
// Useful as a return value when no async work is needed.
var immediate = volt.future.Ready(i32).init(42);

// Pending: a future that never completes.
// Useful as a placeholder or in select() where you want one branch
// to never fire.
var never: volt.future.Pending(i32) = .{};
_ = &never;
```

## Compose (Fluent API)

```zig
const result = volt.future.compose(my_future)
    .map(transformFn)
    .andThen(nextAsyncOp);
```

`Compose` wraps any future with fluent combinator methods: `.map()`, `.andThen()`.

Here is a longer example showing how to build a processing pipeline by chaining `.map()` and `.andThen()`. Each step transforms or extends the result of the previous one:

```zig
const std = @import("std");
const volt = @import("volt");

// Suppose we have a channel that delivers raw sensor readings.
// We want to: receive a reading -> convert units -> validate range ->
// produce a ValidatedReading future that stores the result.

const SensorReading = struct { raw_value: f64, sensor_id: u32 };
const CalibratedReading = struct { celsius: f64, sensor_id: u32 };

const ValidatedReading = struct {
    celsius: f64,
    sensor_id: u32,
    in_range: bool,
};

/// Step 1: Convert raw ADC counts to Celsius.
fn calibrate(reading: SensorReading) CalibratedReading {
    // Linear calibration: raw 0-4095 maps to -40..+125 C
    const celsius = (reading.raw_value / 4095.0) * 165.0 - 40.0;
    return .{ .celsius = celsius, .sensor_id = reading.sensor_id };
}

/// Step 2: Validate that the reading is within operational range.
/// Returns a Ready future so it can be used with .andThen().
fn validateRange(cal: CalibratedReading) volt.future.Ready(ValidatedReading) {
    const in_range = cal.celsius >= -20.0 and cal.celsius <= 80.0;
    return volt.future.Ready(ValidatedReading).init(.{
        .celsius = cal.celsius,
        .sensor_id = cal.sensor_id,
        .in_range = in_range,
    });
}

/// Build the full pipeline from a RecvFuture.
fn buildPipeline(recv_future: anytype) @TypeOf(volt.future.compose(recv_future)
    .map(calibrate)
    .andThen(validateRange)) {
    // compose() wraps the recv future, then we chain:
    //   .map(calibrate)    -- synchronous transform (SensorReading -> CalibratedReading)
    //   .andThen(validate) -- async step (CalibratedReading -> Ready(ValidatedReading))
    return volt.future.compose(recv_future)
        .map(calibrate)
        .andThen(validateRange);
}
```

:::note
`Compose(F)` is itself a Future, so composed pipelines can be used with other engine-level combinators or polled directly in custom Future implementations.
:::
