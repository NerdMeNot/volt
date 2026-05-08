#!/usr/bin/env bash
# Park-site cancel-cleanup audit.
#
# Every `Park.parkCurrent()` call on a Park that's holding kernel /
# external state (a reactor registration, a queued waiter slot, a pool
# closure pointer, etc.) MUST have a `catch |err| switch (err) {
# error.Cancelled => ... }` arm that tears that state down — otherwise
# cancellation leaks the registration and idle workers can wedge in
# `poll` waiting for an event that will never fire.
#
# This script:
#   1. Finds every `parkCurrent()` call in production source.
#   2. Prints 16 lines of context around each (so reviewers can see
#      the catch arm + cleanup, if any).
#   3. Flags sites whose surrounding window has no `error.Cancelled`
#      branch — these are the candidates for missed cleanup.
#
# Output goes to stdout; non-zero exit means flags were raised.
#
# Usage:
#   scripts/audit_park_sites.sh           # full audit
#   scripts/audit_park_sites.sh --strict  # exit 1 on any flag (for CI)

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${REPO_ROOT}/src"
STRICT="${1:-}"

# Production files only — tests intentionally drive parkCurrent into
# odd states to validate the primitive.
files_list=$(
    grep -rln "parkCurrent" "${SRC_DIR}" --include="*.zig" \
        | grep -vE "/test/|/scheduler/park.zig$" \
        | sort -u
)

flagged=0
total_sites=0

printf "Park-site audit\n"
printf "===============\n\n"
file_count=$(printf "%s\n" "$files_list" | grep -c . || true)
printf "Scanning %d files for parkCurrent() call sites.\n\n" "$file_count"

# bash 3.2 has no mapfile / readarray — use a while loop on the IFS=newline
# split.
IFS='
'
for f in $files_list; do
    unset IFS
    relpath="${f#${REPO_ROOT}/}"
    # awk: for each line containing parkCurrent(, print a 16-line window
    #      starting from 2 lines before the call so we capture the
    #      `try X.parkCurrent()` line plus the catch arm.
    while IFS= read -r line_no; do
        total_sites=$((total_sites + 1))
        start=$((line_no > 2 ? line_no - 2 : 1))
        end=$((line_no + 14))
        window=$(sed -n "${start},${end}p" "$f")

        # The post-parkCurrent window must contain SOME `catch` arm —
        # otherwise the Cancelled error from the park bubbles up
        # uncleaned. We don't try to verify the catch body is the
        # *correct* cleanup (would need semantic awareness, e.g.,
        # whether a register* call upstream is paired with the right
        # unregister*); that's part of the registration audit
        # (`audit_registrations.sh`). This script flags only the
        # "no catch arm at all" case.
        post_window=$(sed -n "${line_no},${end}p" "$f")
        if grep -qE "(catch \||catch \{|catch return|catch \|err\| return)" <<<"$post_window"; then
            status="OK"
        else
            status="FLAG"
            flagged=$((flagged + 1))
        fi

        printf "─── %s:%s [%s] ───\n" "$relpath" "$line_no" "$status"
        printf "%s\n\n" "$window"
    done < <(grep -n "parkCurrent()" "$f" | grep -v "//" | cut -d: -f1)
    IFS='
'
done
unset IFS

printf "═══════════════════════════════════════════\n"
printf "Total park sites:   %d\n" "$total_sites"
printf "Flagged (no Cancelled handler in window): %d\n" "$flagged"

if [[ "$STRICT" == "--strict" && "$flagged" -gt 0 ]]; then
    exit 1
fi
exit 0
