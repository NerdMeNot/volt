# Volt Streams — design

**Status:** Proposed, 2026-06-24
**Supersedes:** `api-design.md` §6 (the push/`emit`-based Flow sketch)
**Driver:** Streams are the one unshipped roadmap item (v0.6) and the
abstraction every consumer lib leans on — S3 list pagination, HTTP
chunked/SSE bodies, PG result-set cursors, DataFrame batch I/O are all
streaming. This is the highest-leverage ergonomics piece.

## 1. The core decision: pull, not push

`api-design.md` §6 sketched a Kotlin-faithful **push** model: a producer
fn takes an `emit` callback, operators wrap `emit`, a terminal `collect`
drives it. **We're reversing that to a pull model** — a `Stream(T)` with
`next() !?T` that suspends — for three reasons:

1. **Zig has no lambdas.** The push model inverts control through
   `emit`-callbacks and per-operator function pointers — exactly the
   "verbosity tax" §6 flags. Pull's `while (try s.next()) |v|` is the
   canonical Zig iterator shape; no inversion.
2. **I/O sources are pull-shaped.** S3/HTTP/PG/DataFrame all produce the
   *next* item by doing I/O on demand. `next()` *is* that I/O. Push
   forces a producer-drives-consumer model onto sources that are
   naturally consumer-drives-producer.
3. **Backpressure is free.** A pull source produces nothing until `next()`
   asks. No buffering, no suspending-`emit` dance.

Pull = Rust's `Stream`/`Iterator`, Zig std iterators, Go's
`iter.Seq` — the shape the ecosystem converged on for I/O.

## 2. The core type — erased, allocator-owned

The public type is **type-erased** (`*anyopaque` ctx + vtable), not
comptime-composed (`Filter(Map(Src))`). This is non-negotiable for the
consumer libs: a lib must be able to write `fn listObjects(...) Stream(Object)`
— one concrete return type that escapes the function. The per-`next`
indirect call is noise against the I/O it wraps.

```zig
pub fn Stream(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Item = T;

        ctx: *anyopaque,
        vtable: *const VTable,
        allocator: std.mem.Allocator, // carried from the source; operators reuse it

        pub const VTable = struct {
            /// Suspends if the source needs I/O. `null` = end of stream.
            /// Errors (I/O, Cancelled, OOM) flow through the error union.
            next: *const fn (ctx: *anyopaque) anyerror!?T,
            /// Frees this stage's box and recursively deinits upstream.
            deinit: *const fn (ctx: *anyopaque, a: std.mem.Allocator) void,
        };

        pub fn next(self: *Self) anyerror!?T { return self.vtable.next(self.ctx); }
        pub fn deinit(self: *Self) void { self.vtable.deinit(self.ctx, self.allocator); }

        // operators + terminals below…
    };
}
```

**Error set:** `anyerror` for v1. A parameterized `Stream(T, E)` is more
precise but viral and verbose; `anyerror` is a `u16` (free in the hot
path) and matches how `std.Io.Reader`/`Writer` settle the tradeoff. See
open question Q2.

**Ownership:** each operator allocates one small box (its state +
upstream `Stream`) via the carried allocator — **once, at construction,
not per item**. `deinit()` walks the chain freeing boxes. A 4-stage
pipeline is 4 small allocs total.

## 3. Cancellation, backpressure, errors — all inherited

- **Cancellation:** Volt cancellation is *per-op explicit* (`recvCancel(*Cancel)`,
  not ambient), so cancel is bound at the **blocking source**, not the
  stream wrapper. `fromChannelCancel(ch, c)` parks on `recvCancel(c)`; a
  fired cancel surfaces as `error.Cancelled` and **propagates up through
  every operator automatically** (each does `try upstream.next()`).
  `generate` sources thread their own cancel via `ctx` (call
  `readCancel(ctx.cancel)` inside the generator). There is no stream-level
  `.withCancel` — `fromSlice` has no suspend point to inject at, and the
  two blocking source kinds each have a natural place. *(Shipped, Slice 3.)*
- **Backpressure:** inherent in pull (§1.3). Operators that decouple
  producer/consumer (`buffered(n)`, `merge`) opt *into* buffering via an
  internal channel + coroutine; nothing else buffers.
- **Errors:** propagate through the `next()` error union. A fallible
  transform uses `mapTry`; the first error ends the stream for the
  collector (terminal returns it).

## 4. Sources + the channel bridge

Sources construct the head of a pipeline:

```zig
volt.streams.fromSlice(alloc, items)        // Stream(T) over a []const T (tests, in-mem)
volt.streams.fromChannel(alloc, &ch)        // Stream(T): next() = ch.recv(), ends on Closed
volt.streams.generate(alloc, ctx, genFn)    // next() = genFn(ctx) -> !?T  (the I/O-source hook)
```

`generate` is what the consumer libs build on: S3's `listObjects` stream
is `generate` over "fetch next page, hand out rows, refill on page
boundary." Bridge the other way with:

```zig
stream.intoChannel(&ch)   // spawns a coroutine pulling the stream into ch (fan-out / decouple)
```

So channels and streams interconvert cleanly — `select` etc. keep working
on the channel side.

## 5. Operators + terminals

Operators take **comptime** fn args, so the user's fn is monomorphized
into the stage's `next` (inlined; the only indirection is the one vtable
hop). They return `Allocator.Error!Stream(U)` (the box alloc can fail).

**v1 slice (implement first):**

| Lazy transforms | Terminals (drive the stream) |
|---|---|
| `map(f)` · `filter(pred)` · `mapTry(f)` | `forEach(f)` · `toList(alloc)` |
| `take(n)` · `drop(n)` | `count()` · `first()` · `fold(init, f)` |

**Later slices** (each its own PR): `flatMap`, `zip`, `combine`, `merge`
(needs internal coroutine+channel), `buffered(n)`, `onEach`, `catch`,
`retry(n)`, `timeout(d)`, `single`.

```zig
// Usage — intermediate consts because map changes the element type:
var src = try volt.streams.fromChannel(alloc, &ch);
const evens = try src.filter(isEven);     // Stream(u32)
const labels = try evens.map(toLabel);    // Stream([]const u8)
defer labels.deinit();                    // deinits the whole chain
while (try labels.next()) |label| use(label);
```

## 6. The verbosity tax (acknowledged, bounded)

No lambdas means `map(struct { fn f(v: u32) u64 { return v * 2; } }.f)`.
We accept it for v1 (it's the price of Zig). Mitigations, in order:
- comptime-fn args keep it zero-overhead (no runtime closure).
- A `mapWith(ctx, f)` variant carries state for the closure-over-state case.
- Named fns read fine; the anon-struct form is only for one-offs.
We do **not** ship a zoo of `mapMul`/`filterGt` shortcuts in v1 (open Q3).

## 7. Implementation plan (incremental — each a reviewable slice)

1. ✅ **Core + sources + terminals** — `Stream(T)`, vtable, `fromSlice`,
   `fromChannel`, `generate`, `forEach`/`toList`/`count`/`first`/`fold`.
   Leak-gated. Proves the model end-to-end.
2. ✅ **Lazy transforms** — `map`/`mapTry`/`filter`/`take`/`drop`.
3. ✅ **Cancellation** — `fromChannelCancel` + propagation through
   operators (test: cancel a coro parked on `recv`, assert
   `error.Cancelled` through a `map`, no leak).
4. ✅ **Combine / concurrency (partial)** — `zip` (sequential, no spawn),
   `buffered(n)` (single-producer SPSC prefetch), `intoChannel` (fan-out).
   The spawn-based ops carry a documented teardown contract (drain or
   finite upstream); tests are deadline-guarded so a teardown hang is a
   visible failure, and cover early-abandon.

Slice 1 validated the whole design; 2–4 followed. **Remaining (deferred):**
- `merge` (N-producer fan-in) — reuses `buffered`'s producer+channel
  pattern × N inputs + a done-counter; the obvious next step.
- `combine` (latest-of-both) — niche, **dropped from v1**.
- `flatMap` / `onEach` / `catch` / `retry` / `timeout` / `single` — add
  when a consumer needs them (most are small).

## 8. Open questions

- **Q1 — re-iteration semantics.** Pull streams are single-pass (like
  iterators), not Kotlin cold-Flow (re-runs each `collect`). Single-pass
  is simpler and Zig-idiomatic. Confirm we don't need cold re-run for any
  consumer (S3 re-list = build a new stream — fine).
- **Q2 — `anyerror` vs `Stream(T, E)`.** Start `anyerror`; revisit if a
  consumer needs compile-time-checked stream error sets.
- **Q3 — operator-arg ergonomics.** Ship only comptime-fn + `mapWith`
  for v1, or also the `mapMul`/`filterGt` shortcut zoo? Lean: no zoo.
- **Q4 — fluent chaining vs `try`-per-op.** v1 uses explicit `try`
  (alloc can fail) with intermediate consts. A deferred-error "poisoned
  stream" model (à la `std.Io`'s err slot) would allow
  `src.filter(p).map(f)` with one `try` at the terminal — more machinery,
  defer unless the `try`-per-op reads badly in practice.
- **Q5 — zero-alloc fast path.** Optionally offer a comptime-composed
  `Iter(T)` for hot in-function pipelines that never cross a boundary.
  Not v1; the erased `Stream(T)` is the one public type.
