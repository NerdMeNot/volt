# Spike — POC sprint to validate world-class architecture

This directory holds **proof-of-concept spikes** that validate whether Volt can
match Go's performance (within 5 %) on three headline workloads before we
commit to rewriting the public APIs.

Plan reference: `/Users/srini/.claude/plans/create-a-proper-plan-giggly-cherny.md`
(POC-validate before reshaping APIs).

## Layout

```
spike/
├── README.md              # this file
├── poc_baselines.md       # Phase 0: locked-in pre-POC numbers
├── POC_RESULTS.md         # Phase 1 aggregate (filled as POCs land)
├── SYNTHESIS.md           # Phase 2 decision branch
├── _template/             # copy this when starting a new POC
├── coroutines/            # pre-existing v0.x spike — DO NOT DELETE
├── comptime_async/        # pre-existing stackless attempt — DO NOT DELETE
│
├── A_ctx/                 # narrow-save context switch
├── B_dispatch/            # direct dispatch (no EventSource vtable)
├── C_spawn_floor/         # bare-floor spawn+join (consumes A, B, E winners)
├── D_parker/              # Parker without pthread_cond
├── E_stack/               # stack model bake-off (mmap / fixed / heap / segment)
├── F_spsc/                # SPSC channel fast path
├── G_sched/               # scheduler architecture bake-off
├── H_reactor/             # TCP echo with tight reactor
└── I_wg/                  # batched completion (kill per-coro Park)
```

## How a POC is structured

Each `spike/<letter>_<name>/` directory contains:

1. **`README.md`** with these sections, in order:
   - **Hypothesis** — one sentence: "I think X is faster than Y because Z"
   - **Success criterion** — one specific number from `poc_baselines.md` ±5 %
   - **Implementation sketch** — what's in each `.zig` file in this dir
   - **Result** — filled in after benching: PASS / CLOSE / FAIL with the achieved number
   - **Notes** — anything surprising; profile excerpts; failed branches

2. **Standalone Zig program(s)** — does ONE thing, measures it. Avoid pulling
   in the full `src/volt` module; reuse only the `internal/*` helpers when
   absolutely needed. Direct copy-and-modify from `src/` is preferred over
   import.

3. **A `bench_*.zig` that emits ns/op via `volt.time.nanoTimestamp` or
   `std.time.Timer`.** Minimum 11 iterations after ≥ 3 warmup rounds.
   Report median + IQR.

## How to run a POC

Each POC has a build target in `build.zig` of the form `spike-<letter>`.
For example: `zig build spike-A` runs POC-A's bench.

For sampling profiles, capture with samply:

```sh
zig build spike-<letter>          # build + run, get ns/op
samply record --save-only --no-open --unstable-presymbolicate \
  -o /tmp/spike_<letter>.json.gz \
  ./zig-out/bin/spike-<letter>
```

## Phase gates

- **Phase 0** (1–2 days): `poc_baselines.md` is locked. Every POC compares
  against these numbers, NOT against `BENCHMARKS.md` (which is a snapshot,
  not a band).
- **Phase 1** (3–4 weeks): each POC reports PASS / CLOSE / FAIL. Aggregated
  in `POC_RESULTS.md`.
- **Phase 2** (3–5 days): `SYNTHESIS.md` answers "can we commit to a v2
  architecture rewrite?" Branch 1 (PASS), Branch 2 (CLOSE), Branch 3 (FAIL).

## What POCs are NOT

- **Not production code.** Code quality is "good enough to bench, not
  good enough to ship." Don't worry about error handling beyond what the
  bench needs.
- **Not API design.** API shape is Phase 3 work; POCs use whatever shape
  is cheapest to write.
- **Not portable.** All POCs target macOS arm64 first. POC-D's `futex`
  variant cross-compiles for Linux but isn't run.
- **Not exhaustive.** Each POC measures ONE thing. Don't bundle measurements.

## What was already validated (don't redo)

From `spike/coroutines/bench_switch.zig` (committed earlier):
- ARM64 context switch: **10 ns/swap**
- Single-threaded spawn cost (stackful): **622 ns**

POC-A starts from this baseline and asks "can we get below 10 ns by
narrowing the save set?" The original 10 ns saves all 14 callee-saves;
POC-A asks what the minimum is.
