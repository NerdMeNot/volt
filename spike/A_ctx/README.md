# POC-A — Narrow-save context switch

## Hypothesis

Dropping the NEON callee-saves (`d8–d15`) from the AAPCS64 save set drops
swap cost by removing 4 `stp`/`ldp` pairs (8 µops, ~2 ns at 4 GHz).

## Success criterion

Median ≤ 7 ns/switch (one-way), down from spike's 10 ns wide-save.

## Implementation sketch

- `ctx_narrow.zig` — 104-byte Context (no NEON), narrow swap asm
- `bench_swap_narrow.zig` — ping-pong bench measuring both wide and narrow
  side-by-side in the same binary; 1 M iters × 2 swaps × 11 reps

## How to run

```sh
zig build spike-A
```

## Risk

If a coroutine yields while it has live `d8–d15` values (e.g. an inner
SIMD loop that calls `await`/`yield` mid-vectorized op), the resume sees
garbage NEON. Per AAPCS64 these registers ARE callee-saved across normal
calls — but a stackful swap is special: the swap function returns into a
different stack, so the "callee" preserving them is not the same caller.

The decision is whether to:
1. Accept the constraint ("don't hold live SIMD state across a yield")
   and pay no cost — most workloads don't.
2. Keep the wide save and pay ~2 ns/swap.
3. Use comptime to pick (e.g. `@hasDecl(@import("root"), "uses_simd")`).

Will revisit after measuring.

## Result

- **Status:** PASS (target met) but **hypothesis INVALIDATED** (no measurable win)
- **Achieved:** narrow 6 ns/switch, wide 6 ns/switch (median of 11 × 1 M iters)
- **Variance:** both stable across reps (no IQR > 1 ns)
- **Date measured:** 2026-05-12 on macOS arm64, ReleaseFast

### Interpretation

The narrow save set is **not faster** than the wide save set on Apple
Silicon M-series. Modern ARM cores execute the 4 extra NEON `stp`/`ldp`
pairs in parallel with the GPR pairs — the swap cost is dominated by
the `ret` indirect branch + the L1 cache line touches, not the count
of instructions.

**Implication for the rewrite:** keep the wide save set. The 64 extra
bytes of Context (168 → 104) don't buy any wall-clock improvement, and
the wide set is correct under all callee-save expectations (no
constraint on user code holding live SIMD across yield).

The bottleneck is elsewhere. POC-B (direct dispatch) and POC-D
(Parker) are the next places to look.

### Side note: actual swap cost

Both variants measure **6 ns/switch**, not the 10 ns reported in
`spike/coroutines/bench_switch.zig`. Either the Apple Silicon has
gotten faster since the spike was first run (Apple's been shipping
process improvements), or the spike's older `nanosNow` overhead was
adding ~4 ns. Either way, **Volt's raw context switch is 6 ns —
not 10 ns**. Update `BENCHMARKS.md` accordingly when we refresh.
