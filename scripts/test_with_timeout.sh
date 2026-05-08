#!/usr/bin/env sh
# Run `zig build test` with a hard wall-clock timeout, capturing
# stack traces if the test process hangs.
#
# Why: an unbounded `zig build test` can sit forever on a deadlocked
# coroutine or a parked-but-unwakeable worker. CI then waits the full
# job timeout (often 30 min) before killing it. Local devs leave the
# terminal "running" and walk away.
#
# This wrapper:
#   1. Runs `zig build test` under `timeout` (default 90s).
#   2. On timeout: locates the test process, runs `sample` (Darwin)
#      or attempts `gdb` (Linux) to capture stack traces of all
#      threads, writes to tmp/hang-<timestamp>.txt.
#   3. Kills the test process tree.
#   4. Exits with the timeout's status (124).
#
# Usage:
#   scripts/test_with_timeout.sh                  # 90s default
#   TIMEOUT_SECS=30 scripts/test_with_timeout.sh  # custom
#   scripts/test_with_timeout.sh -Dreactor=iouring   # forwards flags

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TIMEOUT_SECS="${TIMEOUT_SECS:-90}"
HANG_DIR="${REPO_ROOT}/tmp"
mkdir -p "$HANG_DIR"

cd "$REPO_ROOT"

start_ts=$(date +%s)
log_file=$(mktemp)
trap 'rm -f "$log_file"' EXIT

# Run `zig build test "$@"` with a wall-clock budget. We use
# `timeout` if available (GNU coreutils on Linux + Homebrew on
# macOS), otherwise fall back to a background-pid + sleep guard.
if command -v timeout >/dev/null 2>&1; then
    set +e
    timeout --kill-after=5s "${TIMEOUT_SECS}s" zig build test "$@" 2>&1 | tee "$log_file"
    rc=$?
    set -e
else
    # Fallback: launch in background, sleep, kill if still running.
    zig build test "$@" 2>&1 | tee "$log_file" &
    bg_pid=$!
    elapsed=0
    while [ $elapsed -lt $TIMEOUT_SECS ]; do
        if ! kill -0 "$bg_pid" 2>/dev/null; then
            wait "$bg_pid" 2>/dev/null
            rc=$?
            break
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    if kill -0 "$bg_pid" 2>/dev/null; then
        rc=124  # timeout
        kill -TERM "$bg_pid" 2>/dev/null || true
        sleep 2
        kill -KILL "$bg_pid" 2>/dev/null || true
    fi
fi

end_ts=$(date +%s)
duration=$((end_ts - start_ts))

if [ "$rc" -eq 124 ]; then
    ts=$(date +%Y%m%d_%H%M%S)
    hang_log="${HANG_DIR}/hang-${ts}.txt"
    {
        printf "Test hang detected at %s\n" "$(date)"
        printf "Duration: %ds (budget: %ds)\n" "$duration" "$TIMEOUT_SECS"
        printf "Args: %s\n\n" "$*"
        printf "── Test process stack traces ──\n\n"

        # Find the actual test binary processes.
        for pid in $(ps -ax -o pid,command | grep -E "test --cache-dir|\.zig-cache.*test" | grep -v grep | awk '{print $1}'); do
            printf "── PID %s ──\n" "$pid"
            if [ "$(uname)" = "Darwin" ]; then
                sample "$pid" 1 2>/dev/null || printf "(sample failed)\n"
            else
                gdb -batch -p "$pid" -ex "thread apply all bt" 2>/dev/null \
                    || printf "(gdb failed; install for stack traces)\n"
            fi
            printf "\n"
        done

        # Kill the offenders.
        printf "── Killing zombie test processes ──\n"
        for pid in $(ps -ax -o pid,command | grep -E "test --cache-dir|\.zig-cache.*test" | grep -v grep | awk '{print $1}'); do
            kill -KILL "$pid" 2>/dev/null && printf "killed pid %s\n" "$pid"
        done
    } > "$hang_log" 2>&1

    printf "\n"
    printf "═══════════════════════════════════════════\n"
    printf "TEST HUNG. Stack traces + kill log: %s\n" "$hang_log"
    printf "═══════════════════════════════════════════\n"
fi

exit "$rc"
