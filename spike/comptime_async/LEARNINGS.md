# Spike Learnings — Day 1

**TL;DR: the spike cleared.** Linear stackless coroutines with captured locals work in pure Zig comptime. ~80% reduction in user code vs. hand-written state machines.

## What works

A spec struct with `step1`, `step2`, ... methods is introspected at comptime and turned into a fully working Future. All seven tests pass:

| Test | Suspensions | Pattern |
|---|---|---|
| One terminal step (no suspension) | 0 | Sync compute, just for completeness |
| One Future step | 1 | Single suspension |
| Two suspensions with captured local | 2 | **Flagship** — `self.b` accessed across suspensions |
| Three steps, mixed terminal | 2 | Future → Future → terminal value |
| Intermediate value stashed on self | 2 | Value computed in step1, used in step3, threaded through self |
| Shape sanity | — | Generated type satisfies `isFuture` |

## User-facing comparison

**Hand-rolled** (the baseline in `hand_written.zig` — same behavior as the flagship test):

```zig
pub const DoubleAndAddFuture = struct {
    a: i32,
    b: i32,
    state: State,
    pub const Output = i32;
    const State = union(enum) {
        start: void,
        awaiting_first: Delayed(i32),
        awaiting_second: Delayed(i32),
        done: void,
    };
    pub fn init(a: i32, b: i32) ...
    pub fn poll(self: *Self, ctx: *Context) PollResult(i32) {
        while (true) switch (self.state) {
            .start => self.state = .{ .awaiting_first = Delayed(i32).init(self.a * 2) },
            .awaiting_first => |*inner| {
                const r = inner.poll(ctx);
                if (r.isPending()) return .pending;
                const doubled = r.unwrap();
                const sum = doubled + self.b;
                self.state = .{ .awaiting_second = Delayed(i32).init(sum * 2) };
            },
            .awaiting_second => |*inner| {
                const r = inner.poll(ctx);
                if (r.isPending()) return .pending;
                const final = r.unwrap();
                self.state = .done;
                return .{ .ready = final };
            },
            .done => @panic("polled after ready"),
        };
    }
};
```

~50 lines. Cognitive load: state machine, switch dispatch, manual transitions, ownership of inner futures.

**Linear-spec** (same behavior):

```zig
const Spec = struct {
    a: i32,
    b: i32,
    pub const Output = i32;

    pub fn step1(self: *@This()) Delayed(i32) {
        return Delayed(i32).init(self.a * 2);
    }
    pub fn step2(self: *@This(), doubled: i32) Delayed(i32) {
        return Delayed(i32).init((doubled + self.b) * 2);
    }
};
const DoubleAndAddFuture = linear(Spec);
```

~10 lines. Cognitive load: each step says what it does; comptime generates the rest.

**~80% reduction in user code.** The state machine, the dispatch, the inner-future ownership, the transition logic — all generated.

## What we hit & worked through

| Issue | Fix |
|---|---|
| `proto.isFuture` `@hasDecl` failed on non-aggregate types like `i32` | Added `@typeInfo(T)` guard to short-circuit |
| `@Type(.{ .@"union" = ... })` removed in 0.16 | Use new `@Union(.auto, Tag, names, types, attrs)` |
| `@Type(.{ .@"enum" = ... })` removed in 0.16 | Use new `@Enum(TagInt, .exhaustive, names, values)` |
| `@Enum`/`@Union` want `*const [N]T` not `[]const T` | Build fixed-size arrays at comptime, pass with `&names_arr` |
| `comptime var` redundant in comptime scope | Drop `comptime` qualifier; we're already in `blk: { ... }` |
| `inline for` with `continue` mixed with runtime control flow → "comptime control flow inside runtime block" | Restructure: hoist runtime tag check via `@as(TagType, self.state) == @field(TagType, name)`, no `continue` inside the inline for |
| Returning pending from a sub-function alongside `?Output` needs an extra channel | Use `error{Pending}!?Output` — clean way to thread three states (pending / advanced / terminal) |

None of these were fundamental walls. All resolved within the comptime mental model.

## Limits identified (Phase 2 candidates)

The spike covers Phase 1 (linear control flow with captures). These remain:

1. **Branching**: a step that returns one of N possible Future types. Current spec assumes each step has a single return type. Workaround: union the variants and have one downstream step that switches on the tag. Could be smoothed by allowing step return types to be `union(enum) { Future1, Future2 }`. **Probably tractable.**

2. **Loops**: a step that re-invokes itself or an earlier step. Would need either an explicit loop construct (`loopStep1`) or a way for a step to return "go back to step N". **Less obvious how to do cleanly.**

3. **Locals across non-adjacent suspensions**: Currently solved by stashing on `self.*` (test 5 demonstrates). Works but doesn't autodetect — user has to remember to stash. **Acceptable for Phase 1.**

4. **Error propagation**: All current tests use infallible types. Real async code returns `!T`. Need to thread error unions through step types. **Tractable, just needs implementation.**

## What this means for Volt's bet

The spike answered the strategic question: **yes, Zig comptime can produce stackless coroutines with stackful-feeling ergonomics for the linear control-flow case.** That alone covers the majority of real async code (request → response, connect → read → process).

The compounding effect: every dependent NerdMeNot lib (S3, HTTP, PG, DataFrame I/O) gets this ergonomic improvement automatically once `linear` lands as a real Volt feature. A 30% improvement here propagates everywhere.

## Recommended next moves

1. **Land `linear` as `volt.future.linear`** in production. Wire it up to `src/future/Poll.zig` instead of `proto.zig`. Keep additive — doesn't replace existing Future API.
2. **Document patterns** for the limits identified — branching via union, stashing across non-adjacent suspensions.
3. **Phase 2 spike (separate)**: branching and error propagation. Both are likely tractable; do them when the post-port ergonomic roadmap reaches that point.

## Time spent

Day 1 of the spike: ~2 hours from "what's the API surface?" to "all tests pass." Significantly faster than expected because:
- Volt's Future protocol was already well-defined (not redesigning anything)
- Zig comptime introspection (despite no AST walking) handles step methods cleanly via `@hasDecl` + `@TypeOf` + `@field`
- The `@Union`/`@Enum` builtins in 0.16 are powerful enough for runtime-dispatched union construction
