# POC-B — Direct dispatch (no EventSource vtable)

## Hypothesis

`pending_event.subscribe_fn(coro)` is an indirect call through a fn
pointer. Replacing with a comptime-resolved enum + switch shaves
10–30 ns per dispatch (one less load, one fewer branch-target buffer
miss).

## Success criterion

Enum dispatch ≤ 100 ns/full dispatch cycle (where a cycle = 2 ctx
swaps + 1 dispatch decision).

## Result

- **Status:** PASS on target, **hypothesis essentially INVALIDATED**
- **Achieved:** vtable 14 ns/cycle, enum 13 ns/cycle (median of 11 × 1 M)
- After subtracting 2× ctx swap (~12 ns), the dispatch overhead itself
  is **1–2 ns** in both cases — within noise.

## Interpretation

The vtable's indirect call is essentially **free** on modern Apple
Silicon. The branch predictor's BTB sees a stable single-target
indirect call (`yield_singleton`'s subscribe is the hot path 99 % of
the time) and predicts it correctly. No measurable cost vs the direct
enum dispatch.

**Implication for the rewrite:** keep the vtable shape if it has
ergonomic value (extensibility, per-EventSource state); otherwise drop
it for code clarity. Not for perf.

## Pattern emerging across A / B / D

Three POCs in a row have ruled out their primitive-level hypotheses:

| POC | Hypothesis | Reality |
|---|---|---|
| A | Narrow ctx saves faster | No measurable difference |
| B | Enum dispatch beats vtable | Saves 1 ns (within noise) |
| D | Native ulock beats pthread | Saves 15 % — useful but not 10× |

The 33× spawn+join gap to Go is **not in any single primitive**. It's
in **how those primitives are composed by the scheduler.** This shifts
the priority order in the plan: POC-G (scheduler architecture) is now
the highest-leverage POC.
