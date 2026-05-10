#!/usr/bin/env bash
# Bench gate — fails if any core bench exceeds its ceiling threshold.
#
# Volt commits to the runtime's hot-loop performance budget. This
# script enforces that budget on every PR. Thresholds are *ceilings*
# — production numbers should sit well below them. The gate trips
# only on real regressions (hardware variance is well under the
# margin baked into each ceiling).
#
# Thresholds picked at ~3x current production numbers on a typical
# GH-hosted Linux runner (which is ~2x slower than an Apple Silicon
# dev machine). That gives ~30% headroom over hot-runner-jitter
# without letting a real perf regression slip through.
#
# Usage:
#   scripts/bench_gate.sh                           # default thresholds
#   YIELD_NS_MAX=200 scripts/bench_gate.sh          # override per-metric
#   BENCH_ARGS="-Dreactor=iouring" scripts/bench_gate.sh
#
# Exit codes:
#   0 — all metrics under threshold
#   1 — at least one metric exceeded; PR should not merge
#   2 — bench itself failed to run

set -eu

# Ceilings (ns/op). Override via env var of the same name.
YIELD_NS_MAX="${YIELD_NS_MAX:-200}"
CHANNEL_NS_MAX="${CHANNEL_NS_MAX:-1000}"
MUTEX_NS_MAX="${MUTEX_NS_MAX:-5000}"
# spawn+join is the highest-variance metric — local runs 6-22µs,
# GH-hosted runners can spike to 60µs+ on cold cache. 100µs ceiling
# gives 5x headroom over local steady-state without hiding real
# regressions.
SPAWN_JOIN_NS_MAX="${SPAWN_JOIN_NS_MAX:-100000}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "Volt bench gate"
echo "  yield ≤ ${YIELD_NS_MAX} ns/op"
echo "  channel ≤ ${CHANNEL_NS_MAX} ns/op"
echo "  mutex ≤ ${MUTEX_NS_MAX} ns/op"
echo "  spawn+join ≤ ${SPAWN_JOIN_NS_MAX} ns/op"
echo

BENCH_OUT=$(mktemp)
trap 'rm -f "$BENCH_OUT"' EXIT

if ! zig build bench --summary none ${BENCH_ARGS:-} > "$BENCH_OUT" 2>&1; then
    echo "FAIL: zig build bench errored:" >&2
    cat "$BENCH_OUT" >&2
    exit 2
fi

cat "$BENCH_OUT"
echo

# Extract "{name}: {N} ns/op" lines.
extract_ns() {
    local name="$1"
    grep -F "$name" "$BENCH_OUT" | head -1 | sed -E 's/^.*: ([0-9]+) ns\/op.*/\1/'
}

YIELD_NS=$(extract_ns "yield ping-pong")
CHANNEL_NS=$(extract_ns "channel SPSC")
MUTEX_NS=$(extract_ns "mutex lock/unlock")
SPAWN_JOIN_NS=$(extract_ns "spawn+join")

# Mutex bench is opt-in (VOLT_BENCH_MUTEX=1). Default is SKIPPED,
# in which case there's no ns/op to gate.
if grep -q "mutex lock/unlock: SKIPPED" "$BENCH_OUT"; then
    MUTEX_NS=""
fi

if [ -z "$YIELD_NS" ] || [ -z "$CHANNEL_NS" ] || [ -z "$SPAWN_JOIN_NS" ]; then
    echo "FAIL: couldn't parse bench output" >&2
    exit 2
fi

fail=0
check() {
    local name="$1" actual="$2" max="$3"
    if [ "$actual" -gt "$max" ]; then
        printf "  ✗ %-22s %s ns/op  (limit %s)\n" "$name" "$actual" "$max"
        fail=1
    else
        printf "  ✓ %-22s %s ns/op  (limit %s)\n" "$name" "$actual" "$max"
    fi
}

echo "Result:"
check "yield ping-pong"  "$YIELD_NS"      "$YIELD_NS_MAX"
check "channel SPSC"     "$CHANNEL_NS"    "$CHANNEL_NS_MAX"
if [ -n "$MUTEX_NS" ]; then
    check "mutex lock/unlock" "$MUTEX_NS"    "$MUTEX_NS_MAX"
else
    printf "  ⚠ %-22s skipped (opt-in via VOLT_BENCH_MUTEX=1)\n" "mutex lock/unlock"
fi
check "spawn+join"       "$SPAWN_JOIN_NS" "$SPAWN_JOIN_NS_MAX"

if [ "$fail" -eq 0 ]; then
    echo
    echo "All metrics under their ceilings."
    exit 0
fi

echo
echo "FAIL: one or more metrics exceeded ceiling. Review the regression before merging." >&2
exit 1
