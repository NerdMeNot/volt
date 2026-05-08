#!/usr/bin/env sh
# Run the full Volt audit suite — every individual script in scripts/
# concatenated with section headers, exit non-zero if any flag.
#
# Usage:
#   scripts/audit_all.sh           # full audit
#   scripts/audit_all.sh --strict  # propagate per-script --strict

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STRICT="${1:-}"

cd "$REPO_ROOT"

run_section() {
    title="$1"
    shift
    printf "\n"
    printf "╔════════════════════════════════════════════════════════════╗\n"
    printf "║ %s\n" "$title"
    printf "╚════════════════════════════════════════════════════════════╝\n"
    "$@"
}

overall_rc=0

run_section "1/5 — Park-site cancel-cleanup audit" \
    "${REPO_ROOT}/scripts/audit_park_sites.sh" $STRICT || overall_rc=$?

run_section "2/5 — Reactor registration pairing audit" \
    "${REPO_ROOT}/scripts/audit_registrations.sh" $STRICT || overall_rc=$?

run_section "3/5 — Yield-as-sync detector (informational)" \
    "${REPO_ROOT}/scripts/audit_yield_sync.sh" || true

run_section "4/5 — Zombie zig-test process check" \
    "${REPO_ROOT}/scripts/zombie_check.sh" $STRICT || overall_rc=$?

run_section "5/5 — Cross-compile sweep (build-lib)" \
    "${REPO_ROOT}/scripts/cross_compile_all.sh" || overall_rc=$?

printf "\n═══════════════════════════════════════════\n"
if [ "$overall_rc" -eq 0 ]; then
    printf "All audits passed.\n"
else
    printf "Audits flagged issues. Review section output above.\n"
fi
exit "$overall_rc"
