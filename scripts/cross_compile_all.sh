#!/usr/bin/env sh
# Cross-compile sweep — compile-check every supported target × every
# reactor backend, summarize errors per (target, backend).
#
# Today (Phase 2 incomplete) we expect:
#   - Darwin / Linux native (default backend): clean
#   - Linux × iouring backend (`-Dreactor=iouring`): clean
#   - Windows: ~60 errors (Phase 2c-2g pending)
#
# This script exists so that as Windows arms land, we can watch the
# error count drop and surface regressions immediately.
#
# Usage:
#   scripts/cross_compile_all.sh           # build-lib (fastest)
#   scripts/cross_compile_all.sh --tests   # build the test binary too
#                                          # (catches more errors)

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-build-lib}"
LOG_DIR="${REPO_ROOT}/tmp/cross"
mkdir -p "$LOG_DIR"

cd "$REPO_ROOT"

# (target, reactor flag) matrix
sweep="
x86_64-linux-gnu     :default
x86_64-linux-gnu     :-Dreactor=epoll
x86_64-linux-gnu     :-Dreactor=iouring
aarch64-linux-gnu    :default
aarch64-linux-gnu    :-Dreactor=iouring
x86_64-macos         :default
aarch64-macos        :default
x86_64-windows-gnu   :default
aarch64-windows-gnu  :default
"

printf "Cross-compile sweep (mode=%s)\n" "$MODE"
printf "═══════════════════════════════════════════\n\n"

failed=0
passed=0
total=0

# bash 3.2 portable
OLD_IFS="$IFS"
IFS='
'
for line in $sweep; do
    IFS="$OLD_IFS"
    line=$(printf "%s\n" "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    [ -z "$line" ] && { IFS='
'; continue; }

    target=$(printf "%s\n" "$line" | cut -d: -f1 | sed -e 's/[[:space:]]*$//')
    reactor=$(printf "%s\n" "$line" | cut -d: -f2 | sed -e 's/^[[:space:]]*//')
    [ "$reactor" = "default" ] && reactor=""

    # Slug the (target, reactor) for a log filename.
    slug="${target}"
    [ -n "$reactor" ] && slug="${slug}_$(printf "%s" "$reactor" | tr -d '=' | tr -d '-' | tr -d ' ')"
    log_file="${LOG_DIR}/${slug}.log"

    total=$((total + 1))

    if [ "$MODE" = "--tests" ]; then
        cmd="zig build test -Dtarget=$target $reactor"
    else
        cmd="zig build-lib src/lib.zig -target $target -lc -fno-emit-bin $reactor"
    fi

    printf "── %s + %s\n" "$target" "${reactor:-default}"
    if eval "$cmd" > "$log_file" 2>&1; then
        printf "   PASS\n"
        passed=$((passed + 1))
        rm -f "$log_file"  # keep log dir small on success
    else
        err_count=$(grep -cE "^([a-z/].*\.(zig|S):|error:)" "$log_file" || true)
        printf "   FAIL — %d errors  log=%s\n" "$err_count" "$log_file"
        failed=$((failed + 1))
    fi

    IFS='
'
done
IFS="$OLD_IFS"

printf "\n═══════════════════════════════════════════\n"
printf "Total:   %d\n" "$total"
printf "Passed:  %d\n" "$passed"
printf "Failed:  %d\n" "$failed"

if [ "$failed" -gt 0 ]; then
    printf "\nFailing logs in %s/\n" "$LOG_DIR"
    exit 1
fi
exit 0
