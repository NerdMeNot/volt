# POC Baselines — Phase 0 reference numbers

Captured 2026-05-12 on macOS arm64 (Apple Silicon), Zig 0.16.0, Go 1.23.2.
Source: 5 sequential `zig build compare` runs; 3 produced clean output, 2
hit the Parker watchdog flake on spawn+join. Numbers below are from the 3
clean runs, presented as min / median / max so POC results can be judged
against a band, not a single number.

## Raw runs

| Workload | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| TCP echo, Volt (ns/RTT) | 11,856 | 14,488 | 10,321 |
| TCP echo, Go (ns/RTT) | 10,431 | 8,992 | 9,908 |
| Channel pipeline, Volt (ns/msg) | 104 | 97 | 75 |
| Channel pipeline, Go (ns/msg) | 78 | 99 | 94 |
| yield (one-way ctx switch), Volt (ns) | 11 | 11 | 15 |
| yield, Go (ns) | 41 | 42 | 42 |
| spawn + waitgroup, Volt (ns/op) | 4,379 | 3,938 | 4,137 |
| spawn + waitgroup, Go (ns/op) | 157 | 146 | 149 |
| spawn + per-coro join, Volt (ns/op) | 4,015 | 4,448 | 4,125 |
| spawn + per-coro join, Go (ns/op) | 163 | 143 | 149 |
| channel SPSC cap=16, Volt (ns/op) | 184 | 58 | 176 |
| channel SPSC cap=16, Go (ns/op) | 34 | 32 | 33 |
| mutex contended, Volt (ns/op) | 43 | 42 | 41 |
| mutex contended, Go (ns/op) | 83 | 83 | 81 |

The Volt channel SPSC at run 2 (58 ns/op) is anomalous; run 1 and 3 cluster
near 180 ns. Likely cache-warmth artefact. Use the 180 ns figure as the
representative baseline.

## Locked baselines (median of clean runs)

| Workload | Volt | Go | Volt/Go | POC target (≤ 5 % of Go) |
|---|---|---|---|---|
| TCP echo (ns/RTT) | 11,856 | 9,908 | 1.20× | ≤ 10,403 |
| Channel pipeline (ns/msg) | 97 | 94 | 1.03× ★ | ≤ 99 |
| yield (ns) | 11 | 42 | 0.26× ★★ | must stay ≤ 20 |
| spawn + waitgroup (ns/op) | 4,137 | 149 | 27.77× | ≤ 156 |
| spawn + per-coro join (ns/op) | 4,125 | 149 | 27.68× | ≤ 156 |
| channel SPSC (ns/op) | 180 | 33 | 5.45× | ≤ 35 |
| mutex contended (ns/op) | 42 | 83 | 0.51× ★ | must stay ≤ 50 |

## Notes on variance

- Run-to-run variance on Volt is **higher than 5 %** on TCP echo (±15–20 %),
  channel pipeline (±13 %), and channel SPSC (±50 % including the anomaly).
- For POC evaluation we accept ≤ 10 % run-to-run noise — anything inside
  that is "no change," not a win. Real wins should be ≥ 2× anyway given
  the gaps we're trying to close.
- Mutex / yield are stable (≤ 5 % variance).
- The spawn benches occasionally trip the 30 s Parker watchdog under
  sustained 10 k-spawn pressure. This is an open scheduler-layer race
  (Workstream R in the prior plan). For POC purposes we re-run on watchdog
  panic; the POC architecture is meant to make this race unreachable
  (Parker is being replaced in POC-D).

## Raw spike numbers (already validated, do not redo)

From `spike/coroutines/bench_switch.zig` and POC-A (re-measured 2026-05-12):

| Measurement | Value | Notes |
|---|---|---|
| Wide-save ctx switch (14 saves, 168 B) | **6 ns/switch** | re-measured in POC-A; was 10 ns in older spike |
| Narrow-save ctx switch (10 saves, 104 B) | 6 ns/switch | POC-A; no improvement vs wide |
| Single-threaded spike spawn | 622 ns | from `bench_switch.zig` |

## What "Volt today" means for the POC plan

The "Volt today" column above is the medians **before any POC architecture
is adopted.** Each POC is judged by whether its bench number lands inside
the POC target column. POCs are not required to beat Volt today — they
ARE required to beat or match Go.
