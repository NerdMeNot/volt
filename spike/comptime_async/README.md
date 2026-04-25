# Spike: Comptime State Machine Generation

**Status:** Active experiment. NOT part of `zig build`. Files here are isolated from production Volt code.

**Question:** can Zig 0.16 comptime produce stackless coroutines from linear-style code well enough that users don't notice they're writing state machines?

**Boundary:** pure Zig + comptime + stdlib. No C interop, no inline asm. The spike answers a comptime introspection question; foreign tools don't help.

## Files

- `delayed.zig` — A test Future that suspends once before becoming ready. Used as a synthetic suspension point so we can verify state machines handle pending→ready transitions, not just degenerate "everything ready immediately" cases.
- `hand_written.zig` — Baseline: what a Future for a 2-suspension async fn looks like when written by hand. The boilerplate to reduce.
- `linear.zig` — The experimental helper. Takes a struct with step methods, generates the state machine.
- `test_*.zig` — Iterative tests that drive the helper from simple to harder cases.

## Run

```sh
cd spike/comptime_async
zig test test_iter1.zig   # one suspension
zig test test_iter2.zig   # two suspensions with captured locals
```

## Iteration plan

| # | Goal | Complexity |
|---|---|---|
| 1 | One suspension: step1 returns Future, step2 (terminal) takes the result and returns Output. | Easy — degenerates to MapFuture-with-state |
| 2 | Two suspensions: step1 returns Future, step2 takes result and returns Future, step3 takes result and returns Output. Crucially, step2 can read `self.*` to access outer-scope captures. | Real test: composition |
| 3 | Branching: a step returns one of N possible future types depending on a condition. | Probably needs union types — comptime test |
| 4 | Loops: a step that re-invokes itself. | Would prove the model is fully general |

If 1+2 work, the spike clears Phase 1 (linear control flow with captures) and the broader bet is alive. If 3+4 stall, we know the boundary — and "linear async with captures" alone is still a massive ergonomic win over hand-rolled state machines.

## Decision criteria

After each iteration, decide:
- Did Zig comptime support the introspection cleanly? (vs. hacks/workarounds)
- Is the generated state machine correct under contention? (test it through the real scheduler)
- Is the user-facing syntax actually nicer than hand-written? (line-count + cognitive-load comparison)

If the answer to all three is "yes" through iteration 2, escalate to a proper Volt feature. If any is "no," document what failed and stop.
