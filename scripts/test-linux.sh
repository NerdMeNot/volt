#!/usr/bin/env bash
#
# Volt — run the unit test suite on Linux inside a podman container.
#
# Why: we develop on macOS arm64 (kqueue + AAPCS64) but Volt also
# targets Linux arm64/x86_64 (epoll + io_uring, SysV ctx switch on
# x86_64). Bugs in those paths used to only show up after pushing
# to GitHub Actions. This script closes that loop locally.
#
# Usage:
#   scripts/test-linux.sh                # arm64 native (fast)
#   scripts/test-linux.sh --arch x86_64  # amd64 via Rosetta (slower, +ctx asm)
#   scripts/test-linux.sh --rebuild      # force image rebuild
#   scripts/test-linux.sh -- <args...>   # pass extra args to `zig test`
#
# First run pulls debian-slim + downloads Zig (~50 MB) — budget ~2 min.
# Warm runs reuse the image and a named `.zig-cache` volume.

set -euo pipefail

# Repo root = parent of this script.
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

ARCH="arm64"
REBUILD=0
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch)
            ARCH="$2"
            shift 2
            ;;
        --rebuild)
            REBUILD=1
            shift
            ;;
        --)
            shift
            EXTRA_ARGS=("$@")
            break
            ;;
        -h|--help)
            sed -n '3,18p' "$0"
            exit 0
            ;;
        *)
            echo "Unknown arg: $1 (use --help)" >&2
            exit 2
            ;;
    esac
done

# Normalise arch aliases.
case "$ARCH" in
    arm64|aarch64) ARCH=arm64; ZIG_ASM=""; PLATFORM="linux/arm64" ;;
    x86_64|amd64)  ARCH=amd64; ZIG_ASM="src/context_x86_64_sysv.S"; PLATFORM="linux/amd64" ;;
    *) echo "Unknown --arch: $ARCH (want arm64 or x86_64)" >&2; exit 2 ;;
esac

IMAGE="volt-zig:0.16.0-${ARCH}"
# Per-arch cache so amd64-emitted artifacts don't poison the arm64
# cache (different target triples).
CACHE_VOLUME="volt-zig-cache-${ARCH}"

# Pick a podman binary. The user's install lives in /opt/podman.
PODMAN="${PODMAN:-$(command -v podman || echo /opt/podman/bin/podman)}"
if [[ ! -x "$PODMAN" ]]; then
    echo "podman not found; set PODMAN=/path/to/podman" >&2
    exit 1
fi

# Build if missing or --rebuild.
if [[ "$REBUILD" -eq 1 ]] || ! "$PODMAN" image exists "$IMAGE"; then
    echo "==> Building $IMAGE ($PLATFORM)"
    "$PODMAN" build \
        --platform "$PLATFORM" \
        -t "$IMAGE" \
        -f "$REPO_ROOT/scripts/Containerfile.linux" \
        "$REPO_ROOT/scripts"
fi

# Match CI's invocation: bypass `zig build` so we see streaming
# per-test progress (see ci.yml comment for the rationale).
# The ${EXTRA_ARGS[@]+...} dance avoids `set -u`'s "unbound variable"
# error when the array is empty (a bash 4+ quirk).
ZIG_CMD=(zig test src/lib.zig -lc)
[[ -n "$ZIG_ASM" ]] && ZIG_CMD+=("$ZIG_ASM")
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    ZIG_CMD+=("${EXTRA_ARGS[@]}")
fi

echo "==> Running: ${ZIG_CMD[*]} (in $IMAGE)"

# -t only when stdout is a TTY, so the script also runs cleanly
# from CI / wrapper tools. :Z is a no-op on macOS but keeps the
# script portable to SELinux Linux hosts.
TTY_ARG=""
[[ -t 1 ]] && TTY_ARG="-t"

exec "$PODMAN" run --rm -i ${TTY_ARG} \
    --platform "$PLATFORM" \
    -v "$REPO_ROOT":/work:Z \
    -v "$CACHE_VOLUME":/work/.zig-cache \
    -w /work \
    "$IMAGE" \
    "${ZIG_CMD[@]}"
