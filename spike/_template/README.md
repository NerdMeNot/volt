# POC-? — short descriptive title

## Hypothesis

One sentence: "I think X is faster than Y because Z."

## Success criterion

One specific number from `spike/poc_baselines.md` ±5 %.

Example: "≤ 7 ns/swap (current 10 ns, Go 42 ns)."

## Implementation sketch

- `foo.zig` — minimal implementation under test
- `bench_foo.zig` — bench driver, 11 iterations after 3 warmup, emits median ns/op

## How to run

```sh
zig build spike-?
```

Optional: samply profile

```sh
samply record --save-only --no-open --unstable-presymbolicate \
  -o /tmp/spike_?.json.gz \
  ./zig-out/bin/spike-?
```

## Result

To be filled in after benching:

- **Status:** PASS / CLOSE / FAIL
- **Achieved:** X ns/op (target: Y ns/op)
- **Variance:** IQR = ±Z %
- **Date measured:** YYYY-MM-DD on macOS arm64

## Notes

Anything surprising; profile excerpts; failed branches; cross-checks.
