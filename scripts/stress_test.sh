#!/usr/bin/env sh
# Stress runner — runs the test suite N times with different seeds
# to surface flaky / timing-sensitive tests that pass in isolation
# but fail under load.
#
# Each iteration runs under the timeout wrapper so a deadlock in any
# iteration kills the run and captures stack traces.
#
# Output:
#   - Per-iteration pass/fail summary
#   - Aggregated flake-rate per failing test
#
# Usage:
#   scripts/stress_test.sh                    # default: 50 iterations
#   N=200 scripts/stress_test.sh              # custom iteration count
#   N=200 TIMEOUT_SECS=60 scripts/stress_test.sh
#   scripts/stress_test.sh -Dreactor=iouring  # forwards flags

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
N="${N:-50}"
export TIMEOUT_SECS="${TIMEOUT_SECS:-90}"
LOG_DIR="${REPO_ROOT}/tmp/stress"
mkdir -p "$LOG_DIR"

cd "$REPO_ROOT"

start_ts=$(date +%s)
results_file=$(mktemp)
flakes_file=$(mktemp)
trap 'rm -f "$results_file" "$flakes_file"' EXIT

printf "Stress runner: %d iterations, %ds timeout each\n" "$N" "$TIMEOUT_SECS"
printf "Args forwarded to zig build test: %s\n\n" "$*"

passes=0
failures=0
hangs=0

i=1
while [ "$i" -le "$N" ]; do
    seed=$(printf "0x%x" "$((RANDOM * 65537 + i))")
    iter_log="${LOG_DIR}/iter-$(printf "%04d" "$i").log"

    # Run with a fresh seed so we hit different schedule orderings.
    if "${REPO_ROOT}/scripts/test_with_timeout.sh" --seed "$seed" "$@" > "$iter_log" 2>&1; then
        rc=0
    else
        rc=$?
    fi

    case "$rc" in
        0)
            printf "  [%3d/%d] PASS  seed=%s\n" "$i" "$N" "$seed"
            passes=$((passes + 1))
            # Drop the log on pass to keep the dir small.
            rm -f "$iter_log"
            ;;
        124)
            printf "  [%3d/%d] HANG  seed=%s  log=%s\n" "$i" "$N" "$seed" "$iter_log"
            hangs=$((hangs + 1))
            echo "HANG seed=$seed log=$iter_log" >> "$results_file"
            ;;
        *)
            printf "  [%3d/%d] FAIL  seed=%s  log=%s\n" "$i" "$N" "$seed" "$iter_log"
            failures=$((failures + 1))
            # Extract failing test names for the flake-rate roll-up.
            grep -E "^error: '" "$iter_log" \
                | sed -E "s/^error: '([^']+)'.*/\1/" \
                >> "$flakes_file" 2>/dev/null || true
            echo "FAIL seed=$seed log=$iter_log" >> "$results_file"
            ;;
    esac

    i=$((i + 1))
done

end_ts=$(date +%s)
duration=$((end_ts - start_ts))

printf "\n═══════════════════════════════════════════\n"
printf "Iterations: %d  duration: %ds  avg: %ds/iter\n" "$N" "$duration" "$((duration / N))"
printf "Passes:   %d\n" "$passes"
printf "Failures: %d\n" "$failures"
printf "Hangs:    %d\n" "$hangs"

if [ -s "$flakes_file" ]; then
    printf "\nFlake rate by test (failures / total):\n"
    sort "$flakes_file" | uniq -c | sort -rn | while read -r count test; do
        printf "  %d/%d  %s\n" "$count" "$N" "$test"
    done
fi

# Exit non-zero if any iteration failed/hung — useful for CI.
if [ "$failures" -gt 0 ] || [ "$hangs" -gt 0 ]; then
    exit 1
fi
exit 0
