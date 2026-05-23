#!/usr/bin/env python3
"""Reactor-hardening bench regression harness.

Runs the required bench suite from CLAUDE.md, extracts the headline ns/op
(or analogue) from each bench's stderr output, and either:
  - captures a JSON baseline (`capture --output <file>`)
  - compares current numbers to a baseline (`compare --baseline <file>`)

Compare exits non-zero on regression beyond the per-bench threshold. Spawn-heavy
benches use a wider threshold per BENCHMARKS.md's "5-run medians, 30–50% variance"
guidance; micros use a tight threshold because they're stable.

Baselines are per-platform — `bench-baseline-<os>-<arch>.json`. The platform tag
comes from platform.system() + platform.machine(). Compare against a baseline
captured on a different platform emits a warning but proceeds, since the user
may explicitly want to spot-check.
"""

from __future__ import annotations

import argparse
import json
import platform
import re
import statistics
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class Bench:
    name: str                       # bench-<name> as `zig build` invokes it
    runs: int                       # how many times to invoke for variance averaging
    patterns: Dict[str, str]        # metric_key -> regex with one integer capture
    spawn_heavy: bool = False       # widens regression threshold
    timeout_s: int = 600            # per-invocation timeout


# Threshold notes:
#   Micros (yield/spsc/mpmc/mutex): single-process, no cross-thread variance →
#     5-10% gate catches real regressions without flaking.
#   Reactor (tcp-echo/reactor-*): kernel scheduling adds noise → 15%.
#   Spawn-heavy (spawn-hot/scaling/fanout-scaling/parallel-compute): per
#     BENCHMARKS.md, run-to-run variance can be 30-50% → 25% gate is correct
#     for catching regressions without catching noise.
THRESHOLD_MICRO = 0.10
THRESHOLD_REACTOR = 0.15
THRESHOLD_SPAWN = 0.25


BENCHES: List[Bench] = [
    # Pure scheduler/sync — reactor changes should NOT move these.
    Bench("yield", runs=3, patterns={
        "yield_ns_per_op": r"yield:\s+(\d+)\s+ns/op",
    }),
    Bench("spsc", runs=3, patterns={
        "spsc_ns_per_op": r"Spsc send\+recv:\s+(\d+)\s+ns/op",
    }),
    Bench("mpmc", runs=3, patterns={
        "mpmc_1p1c_ns_per_op": r"1P\s*[xX×]\s*1C:\s+(\d+)\s+ns/op",
        "mpmc_2p2c_ns_per_op": r"2P\s*[xX×]\s*2C:\s+(\d+)\s+ns/op",
        "mpmc_4p4c_ns_per_op": r"4P\s*[xX×]\s*4C:\s+(\d+)\s+ns/op",
    }),
    Bench("mutex", runs=3, patterns={
        "mutex_ns_per_op": r"Mutex \(contended\):\s+(\d+)\s+ns/op",
    }),
    # Spawn-heavy — wider threshold.
    Bench("spawn-hot", runs=5, spawn_heavy=True, patterns={
        "spawn_hot_ns_per_op": r"ns/op:\s+(\d+)",
    }),
    Bench("scaling", runs=3, spawn_heavy=True, patterns={
        # Format: `  workers=  1  median    +89 ns/op   +11207674 ops/sec`
        # The `+` prefix comes from Zig's `{d:>6}` right-alignment formatting.
        "_multi_scaling": r"workers=\s*(\d+)\s+median\s+\+?(\d+)\s+ns/op",
    }),
    Bench("fanout-scaling", runs=3, spawn_heavy=True, patterns={
        # Format (matched-shape sweep, drivers=workers):
        #   `        1          1       48497800       82.5      1.00×`
        # Columns: workers, drivers, total_ops, ns/op, ratio×
        # We key by workers (first group). Last group is the ns/op value.
        "_multi_fanout": r"^\s*(\d+)\s+\d+\s+\d+\s+(\d+(?:\.\d+)?)\s+\d+(?:\.\d+)?×",
    }),
    Bench("parallel-compute", runs=3, spawn_heavy=True, patterns={
        # Format: `  workers= 1  wall +25090 us  per-task  +98 us  speedup 1.00x`
        # Key by workers, value = per-task us (lower=better, same direction as ns/op).
        "_multi_parallel": r"workers=\s*(\d+)\s+wall\s+\+?\d+\s+us\s+per-task\s+\+?(\d+)\s+us",
    }),
    # Reactor-heavy.
    Bench("tcp-echo", runs=3, patterns={
        "tcp_echo_ns_per_rtt": r"TCP echo: median\s+(\d+)\s+ns/RTT",
    }),
    Bench("reactor-throughput", runs=3, patterns={
        "reactor_wake_ns": r"reactor wake: median\s+(\d+)\s+ns/wake",
    }),
    Bench("reactor-fanout", runs=3, patterns={
        "reactor_fanout_ns_per_rtt": r"per-RTT: median\s+(\d+)\s+ns",
    }),
    # RSS — captures per-coro bytes at the largest N. Single run; deterministic.
    Bench("rss", runs=1, patterns={
        "_multi_rss": r"N=\s*(\d+)\s+peak RSS\s*=\s*\d+\s+B\s+\(\s*\d+\s+KiB\)\s+→\s+per-coro\s*=\s*(\d+)\s+B",
    }),
]


def platform_tag() -> str:
    sys_name = platform.system().lower()  # darwin / linux / windows
    arch = platform.machine().lower()
    if sys_name == "darwin":
        sys_name = "macos"
    if arch in ("aarch64", "arm64"):
        arch = "arm64"
    elif arch in ("x86_64", "amd64", "x64"):
        arch = "x86_64"
    return f"{sys_name}-{arch}"


def run_bench(bench: Bench) -> List[str]:
    """Build + run the bench `bench.runs` times. Each invocation's stderr returned."""
    outputs: List[str] = []
    for i in range(bench.runs):
        sys.stderr.write(f"  run {i + 1}/{bench.runs} ... ")
        sys.stderr.flush()
        try:
            r = subprocess.run(
                ["zig", "build", f"bench-{bench.name}", "-Doptimize=ReleaseFast"],
                capture_output=True,
                text=True,
                timeout=bench.timeout_s,
            )
        except subprocess.TimeoutExpired:
            sys.stderr.write(f"TIMEOUT after {bench.timeout_s}s\n")
            sys.exit(2)
        if r.returncode != 0:
            sys.stderr.write(f"FAILED (exit {r.returncode})\n")
            sys.stderr.write(r.stderr[-2000:])
            sys.exit(2)
        sys.stderr.write("done\n")
        # std.debug.print writes to stderr.
        outputs.append(r.stderr)
    return outputs


def parse(outputs: List[str], patterns: Dict[str, str]) -> Dict[str, object]:
    """Extract metrics. Multi-shape patterns prefixed with `_multi_` expand
    into per-shape keys derived from the first capture group."""
    metrics: Dict[str, object] = {}
    for key, pat in patterns.items():
        if key.startswith("_multi_"):
            # Multi-shape: scan all matches across all runs, collapse by shape.
            # Convention: group(1) = shape key, group(<last>) = value.
            shape_samples: Dict[str, List[float]] = {}
            for out in outputs:
                for m in re.finditer(pat, out, re.MULTILINE):
                    shape = m.group(1)
                    raw = m.group(m.lastindex)
                    val = float(raw) if "." in raw else int(raw)
                    sub = key[len("_multi_"):]
                    shape_key = f"{sub}_w{shape}"
                    shape_samples.setdefault(shape_key, []).append(float(val))
            for sk, samples in shape_samples.items():
                metrics[sk] = int(statistics.median(samples))
                metrics[f"{sk}_samples"] = [int(s) for s in samples]
        else:
            samples: List[int] = []
            for out in outputs:
                m = re.search(pat, out)
                if not m:
                    sys.stderr.write(
                        f"  WARN: pattern {key!r}={pat!r} not matched\n"
                    )
                    continue
                samples.append(int(m.group(1)))
            if samples:
                metrics[key] = int(statistics.median(samples))
                metrics[f"{key}_samples"] = samples
    return metrics


def threshold_for(bench: Bench) -> float:
    if bench.spawn_heavy:
        return THRESHOLD_SPAWN
    if bench.name in ("tcp-echo", "reactor-throughput", "reactor-fanout"):
        return THRESHOLD_REACTOR
    return THRESHOLD_MICRO


def capture(output: Path, only: Optional[List[str]] = None) -> None:
    tag = platform_tag()
    result: Dict[str, object] = {
        "platform": tag,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "benches": {},
    }
    for bench in BENCHES:
        if only and bench.name not in only:
            continue
        sys.stderr.write(f"\n[bench-{bench.name}] {bench.runs} run(s)\n")
        outputs = run_bench(bench)
        metrics = parse(outputs, bench.patterns)
        result["benches"][bench.name] = metrics  # type: ignore[index]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n")
    sys.stderr.write(f"\nWrote baseline → {output}\n")


def compare(baseline: Path, only: Optional[List[str]] = None) -> None:
    base_data = json.loads(baseline.read_text())
    sys.stderr.write(
        f"Comparing against {baseline} (captured {base_data.get('captured_at', '?')} on {base_data.get('platform', '?')})\n"
    )
    if base_data.get("platform") != platform_tag():
        sys.stderr.write(
            f"WARNING: baseline platform {base_data.get('platform')} != current {platform_tag()}; numbers may not be directly comparable\n"
        )
    regressions: List[tuple] = []
    for bench in BENCHES:
        if only and bench.name not in only:
            continue
        thr = threshold_for(bench)
        sys.stderr.write(f"\n[bench-{bench.name}] {bench.runs} run(s), threshold {thr * 100:.0f}%\n")
        outputs = run_bench(bench)
        current = parse(outputs, bench.patterns)
        base_bench = base_data.get("benches", {}).get(bench.name, {})
        for key, cur_val in current.items():
            if key.endswith("_samples"):
                continue
            base_val = base_bench.get(key)
            if base_val is None:
                sys.stderr.write(f"  {key}: {cur_val} (no baseline)\n")
                continue
            ratio = cur_val / base_val if base_val else float("inf")
            delta = (ratio - 1.0) * 100.0
            tag = "OK   "
            if ratio > 1.0 + thr:
                tag = "REGRESS"
                regressions.append((bench.name, key, base_val, cur_val, delta, thr))
            sys.stderr.write(f"  {tag}  {key}: {cur_val} (baseline {base_val}, {delta:+.1f}%)\n")
    if regressions:
        sys.stderr.write(f"\n{len(regressions)} regression(s):\n")
        for name, key, b, c, d, t in regressions:
            sys.stderr.write(
                f"  bench-{name} {key}: {b} → {c} ({d:+.1f}%, threshold {t * 100:.0f}%)\n"
            )
        sys.exit(1)
    sys.stderr.write("\nAll benches within threshold.\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    cap = sub.add_parser("capture", help="capture a baseline JSON")
    cap.add_argument("--output", type=Path, required=True)
    cap.add_argument("--only", nargs="+", help="restrict to named benches")
    cmp = sub.add_parser("compare", help="compare current against a baseline")
    cmp.add_argument("--baseline", type=Path, required=True)
    cmp.add_argument("--only", nargs="+", help="restrict to named benches")
    args = ap.parse_args()
    if args.cmd == "capture":
        capture(args.output, only=args.only)
    else:
        compare(args.baseline, only=args.only)


if __name__ == "__main__":
    main()
