#!/usr/bin/env sh
# Yield-as-synchronization detector for tests.
#
# Pattern (anti-pattern): a test launches N coroutines, then loops
# `try volt.yield()` M times "to give them a chance to park," then
# inspects state set by those coroutines. This works on quiet systems
# but fails under load — yields are best-effort reschedules, not a
# barrier. The work-stealing scheduler may not actually run the
# launched coros before the parent's assertions fire.
#
# Real fix: use a synchronization primitive (Barrier, Latch,
# WaitGroup) so the parent deterministically waits for the children to
# reach the expected state.
#
# This script:
#   1. Searches test files for `while (... < N) ... volt.yield()` or
#      similar yield-loop patterns.
#   2. Prints each match with surrounding context so reviewers can
#      tell whether it's actually being used as a sync barrier.
#
# Output is informational; does not exit non-zero by default. Real
# enforcement comes from rewriting the offenders to use proper
# synchronization (Phase B of the audit).

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${REPO_ROOT}/src"

printf "Yield-as-sync detector\n"
printf "======================\n\n"

# Pattern: a `while (... < N) ... volt.yield()` loop where the next
# significant lines are assertions about state owned by other
# coroutines. We approximate by looking for `volt.yield()` inside a
# `while` loop with a comment or surrounding code that hints at
# synchronization intent.

# Heuristic 1: yield loops in test files.
printf "── Heuristic 1: yield loops in src/test/ and *_test.zig\n\n"

found=0
hits_file=$(mktemp)
trap 'rm -f "$hits_file"' EXIT

# Walk all test files. Find lines containing `volt.yield()` inside a
# while / for loop.
find "${SRC_DIR}" -name "*_test.zig" -o -name "*test*.zig" 2>/dev/null \
    | grep -E "/test/|_test\.zig" \
    | sort -u \
    | while IFS= read -r f; do
    relpath="${f#${REPO_ROOT}/}"
    grep -nE "volt\.yield\(\)" "$f" 2>/dev/null \
        | grep -v "^[[:space:]]*//" \
        | while IFS=: read -r line_no rest; do
            # Look at the 5 preceding lines for a `while` or `for` opener.
            start=$((line_no > 5 ? line_no - 5 : 1))
            preceding=$(sed -n "${start},${line_no}p" "$f")
            if printf "%s" "$preceding" | grep -qE "while|for "; then
                # Look at the next 5 lines for an assertion that hints
                # this is a "wait for state" pattern.
                end=$((line_no + 5))
                following=$(sed -n "${line_no},${end}p" "$f")

                printf "─── %s:%s ───\n" "$relpath" "$line_no"
                sed -n "${start},${end}p" "$f"
                printf "\n"
                echo "1" >> "$hits_file"
            fi
        done
done

found=$(wc -l < "$hits_file" | tr -d ' ')

printf "═══════════════════════════════════════════\n"
printf "Yield-as-sync candidates found: %d\n" "$found"
printf "\n"
printf "These are flagged for review — volt.yield() is a reschedule\n"
printf "hint, NOT a synchronization barrier. Tests using yield-loops as\n"
printf "synchronization should migrate to Barrier/Latch/WaitGroup.\n"
exit 0
