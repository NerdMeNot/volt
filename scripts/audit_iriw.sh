#!/usr/bin/env sh
# IRIW (Independent Reads of Independent Writes) pattern detector.
#
# The bug class: two threads each have a "set flag, read pointer" or
# "set pointer, read flag" pair, and the pairs are coordinating. With
# release/acquire memory ordering, both threads can observe their
# pre-state of the other (canceller reads ptr=0, parker reads flag=
# false) — and miss each other's update.
#
# Volt hit this in `Coroutine.cancel` + `Park.parkCurrent`: cancel did
# `cancel_flag.store(.release) + current_park.load(.acquire)`;
# parkCurrent did `current_park.store(.release)` and read cancel_flag
# only at entry. Result: parked coro with cancel_flag set and no
# unpark scheduled — permanent park leak. Fix: seq_cst on both stores.
#
# This script finds candidate sites: any function that does both an
# atomic.store and atomic.load on DIFFERENT atomic fields, where at
# least one is non-seq_cst. The pair MAY be a coordination dance
# (false positives exist when the two atomics aren't actually paired
# across threads). Reviewer triage is required.
#
# Usage:
#   scripts/audit_iriw.sh           # full audit
#   scripts/audit_iriw.sh --strict  # exit 1 on any flag

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${REPO_ROOT}/src"
STRICT="${1:-}"

flagged_file=$(mktemp)
echo 0 > "$flagged_file"
trap 'rm -f "$flagged_file"' EXIT

printf "IRIW pattern audit\n"
printf "==================\n\n"
printf "Searches for functions that do BOTH atomic.store and atomic.load\n"
printf "on different fields without seq_cst — candidate sites for the\n"
printf "IRIW litmus race that bit Coroutine.cancel + Park.parkCurrent.\n\n"
printf "Reviewer triage required: not every (.store + .load) pair is a\n"
printf "cross-thread coordination — many are within-thread state.\n\n"

find "${SRC_DIR}" -name "*.zig" -not -path "*/test/*" 2>/dev/null \
    | sort -u \
    | while IFS= read -r f; do
    relpath="${f#${REPO_ROOT}/}"

    # Find functions with at least one .store(...) AND at least one .load(...).
    # Heuristic: scan the file for `pub fn|fn ` blocks; for each, check
    # if it contains both .store and .load on different field names.
    # POSIX awk to keep it portable.
    awk -v file="$relpath" '
    BEGIN {
        in_fn = 0; brace_depth = 0; fn_name = ""; fn_start = 0;
        store_field = ""; load_field = "";
        store_line = 0; load_line = 0;
        store_ord = ""; load_ord = "";
    }
    /^[[:space:]]*((pub )?fn )/ {
        # Function header
        in_fn = 1; brace_depth = 0; fn_start = NR;
        match($0, /(pub )?fn [a-zA-Z0-9_]+/);
        fn_name = substr($0, RSTART, RLENGTH);
        store_field = ""; load_field = "";
        store_line = 0; load_line = 0;
    }
    in_fn {
        n = gsub(/\{/, "&", $0);
        brace_depth += n;
        n = gsub(/\}/, "&", $0);
        brace_depth -= n;

        # Capture the first .store and .load with their fields.
        if (match($0, /[a-zA-Z0-9_.]+\.store\(/)) {
            line = $0;
            sub(/.*[ ,(]/, "", line); sub(/\.store\(.*/, "", line);
            if (store_field == "") {
                store_field = line; store_line = NR;
                if (match($0, /\.seq_cst\)/)) store_ord = "seq_cst";
                else if (match($0, /\.release\)/)) store_ord = "release";
                else if (match($0, /\.monotonic\)/)) store_ord = "monotonic";
                else store_ord = "?";
            }
        }
        if (match($0, /[a-zA-Z0-9_.]+\.load\(/)) {
            line = $0;
            sub(/.*[ ,(]/, "", line); sub(/\.load\(.*/, "", line);
            if (load_field == "" && line != store_field) {
                load_field = line; load_line = NR;
                if (match($0, /\.seq_cst\)/)) load_ord = "seq_cst";
                else if (match($0, /\.acquire\)/)) load_ord = "acquire";
                else if (match($0, /\.monotonic\)/)) load_ord = "monotonic";
                else load_ord = "?";
            }
        }

        if (brace_depth == 0 && NR > fn_start) {
            # End of function. Report if both store and load present
            # AND at least one is non-seq_cst.
            if (store_field != "" && load_field != "" &&
                (store_ord != "seq_cst" || load_ord != "seq_cst")) {
                printf "─── %s:%d–%d  %s\n", file, fn_start, NR, fn_name;
                printf "    store .%s @ line %d (.%s)\n", store_field, store_line, store_ord;
                printf "    load  .%s @ line %d (.%s)\n", load_field, load_line, load_ord;
                printf "\n";
            }
            in_fn = 0;
        }
    }
    ' "$f" >> "$flagged_file.out" 2>/dev/null || true
done

cat "$flagged_file.out" 2>/dev/null || true
flagged=$(grep -c "^─── " "$flagged_file.out" 2>/dev/null || echo 0)
rm -f "$flagged_file.out"

printf "═══════════════════════════════════════════\n"
printf "Candidate sites:  %d\n" "$flagged"
printf "\n"
printf "Each site needs human triage:\n"
printf "  - Are the two atomics paired across threads as a coordinator?\n"
printf "  - If yes: do BOTH sides use seq_cst on the corresponding ops?\n"
printf "  - If yes to both: site is correct; mark with a code comment.\n"
printf "  - If no: fix to seq_cst (canonical IRIW resolution).\n"

if [ "$STRICT" = "--strict" ] && [ "$flagged" -gt 0 ]; then
    exit 1
fi
exit 0
