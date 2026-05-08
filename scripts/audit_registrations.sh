#!/usr/bin/env sh
# Reactor-registration pairing audit.
#
# Every `reactor.registerWait(fd, kind, target)` call site must be
# paired with a `reactor.unregisterWait(fd, kind)` on the cancellation
# path. Same for `registerTimer`/`unregisterTimer`. Without the pair,
# a cancelled coroutine leaves its kernel registration armed AND the
# reactor's pending counter inflated — idle workers then block in
# `poll` waiting for an event that no live park will consume.
#
# This was the v1.0 bug class: kqueue timer leak that was masked as
# v1.2 follow-up but was actually a 100-LoC fix.
#
# This script:
#   1. Finds every `reactor.register*(...)` call in production code
#      (excluding the reactor backends themselves and tests).
#   2. Verifies the same function/scope contains a corresponding
#      `reactor.unregister*` — typically inside a `catch |err| switch
#      (err) { error.Cancelled => ... }` arm.
#   3. Prints a structured report with OK/FLAG per site.
#
# Output goes to stdout; non-zero exit means flags were raised.
#
# Usage:
#   scripts/audit_registrations.sh           # full audit
#   scripts/audit_registrations.sh --strict  # exit 1 on any flag

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${REPO_ROOT}/src"
STRICT="${1:-}"

# Production files that call reactor.register* (excluding the reactor
# backends, which implement these calls).
files_list=$(
    grep -rln "reactor\.register\(Wait\|Timer\)" "${SRC_DIR}" --include="*.zig" \
        | grep -vE "/io/reactor_(kqueue|epoll|iouring|iocp)\.zig$" \
        | grep -vE "/test/|reactor_test\.zig$" \
        | sort -u
)

flagged_file=$(mktemp)
total_file=$(mktemp)
echo 0 > "$flagged_file"
echo 0 > "$total_file"
trap 'rm -f "$flagged_file" "$total_file"' EXIT

printf "Reactor-registration pairing audit\n"
printf "==================================\n\n"

# bash 3.2 portable: walk lines via a here-string + while
file_count=$(printf "%s\n" "$files_list" | grep -c . || true)
printf "Scanning %s files for reactor.register* call sites.\n\n" "$file_count"

OLD_IFS="$IFS"
IFS='
'

for f in $files_list; do
    IFS="$OLD_IFS"
    relpath="${f#${REPO_ROOT}/}"

    # For each register call, find the enclosing function scope and
    # verify the matching unregister is reachable from the cancel arm.
    # POSIX-portable approach: print 30 lines starting from each
    # register* call, then check whether that window contains the
    # corresponding unregister*. Crude but correct in practice — the
    # cancel arm typically lives within 30 lines.
    grep -nE "reactor\.register(Wait|Timer)\(" "$f" | grep -v "//" | while IFS=: read -r line_no _; do
        # Increment via temp file because we're in a subshell pipeline.
        echo $(($(cat "$total_file") + 1)) > "$total_file"
        end=$((line_no + 30))
        window=$(sed -n "${line_no},${end}p" "$f")

        # Detect which kind we're auditing.
        if printf "%s" "$window" | head -1 | grep -q "registerWait"; then
            counterpart="unregisterWait"
        else
            counterpart="unregisterTimer"
        fi

        if printf "%s" "$window" | grep -q "$counterpart"; then
            status="OK"
        else
            status="FLAG (no $counterpart in next 30 lines)"
            echo $(($(cat "$flagged_file") + 1)) > "$flagged_file"
        fi

        printf "─── %s:%s [%s] ───\n" "$relpath" "$line_no" "$status"
        # Show 12 lines so reviewers can see the call + cancel arm.
        sed -n "${line_no},$((line_no + 11))p" "$f"
        printf "\n"
    done

    IFS='
'
done
IFS="$OLD_IFS"

total_sites=$(cat "$total_file")
flagged=$(cat "$flagged_file")

printf "═══════════════════════════════════════════\n"
printf "Total register sites:  %d\n" "$total_sites"
printf "Flagged (no paired unregister): %d\n" "$flagged"

if [ "$STRICT" = "--strict" ] && [ "$flagged" -gt 0 ]; then
    exit 1
fi
exit 0
