#!/usr/bin/env bash
#
# Volt — run scripts/probe_io_uring.zig inside the Linux dev
# container to verify io_uring is usable before committing to the
# Phase 2 (per-worker io_uring rings) work — see
# docs/internals/async-fs-io.md and issue #17.
#
# Mirrors scripts/test-linux.sh's build/cache pattern; differs only
# in the final command (`zig run` of the probe vs `zig test` of the
# suite). One-shot diagnostic; not part of the regular dev loop.
#
# Usage:
#   scripts/probe-linux.sh                # arm64 (matches arm64 Mac default)
#   scripts/probe-linux.sh --arch x86_64  # amd64 via Rosetta
#
# Exit codes:
#   0  — io_uring usable in this container
#   2  — io_uring blocked (sysctl / seccomp / kernel too old)
#   other — script / image build error

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ARCH="arm64"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch) ARCH="$2"; shift 2 ;;
        -h|--help) sed -n '3,18p' "$0"; exit 0 ;;
        *) echo "Unknown arg: $1 (use --help)" >&2; exit 2 ;;
    esac
done

case "$ARCH" in
    arm64|aarch64) ARCH=arm64; PLATFORM="linux/arm64" ;;
    x86_64|amd64)  ARCH=amd64; PLATFORM="linux/amd64" ;;
    *) echo "Unknown --arch: $ARCH (want arm64 or x86_64)" >&2; exit 2 ;;
esac

IMAGE="volt-zig:0.16.0-${ARCH}"
PODMAN="${PODMAN:-$(command -v podman || echo /opt/podman/bin/podman)}"
if [[ ! -x "$PODMAN" ]]; then
    echo "podman not found; set PODMAN=/path/to/podman" >&2
    exit 1
fi

# Build the image if it doesn't exist yet. Same image as test-linux.sh;
# no extra rebuild if you've already run that script.
if ! "$PODMAN" image exists "$IMAGE"; then
    echo "==> Building $IMAGE ($PLATFORM)"
    "$PODMAN" build \
        --platform "$PLATFORM" \
        -t "$IMAGE" \
        -f "$REPO_ROOT/scripts/Containerfile.linux" \
        "$REPO_ROOT/scripts"
fi

echo "==> Running io_uring probe in $IMAGE"
TTY_ARG=""
[[ -t 1 ]] && TTY_ARG="-t"

# `--security-opt seccomp=unconfined`: Fedora 41's default podman
#   seccomp profile blocks the io_uring syscalls and returns
#   ENOSYS to disguise the rejection. Disabling seccomp for the
#   probe is acceptable — this is a local dev container, not an
#   exposed surface. (Phase 2 will need the same flag for any
#   container that runs the io_uring tests / benchmarks.)
# `--security-opt label=disable`: with seccomp out of the way,
#   SELinux still refuses io_uring_setup for the unconfined
#   container_t context (errno EACCES). label=disable opts this
#   container out of SELinux relabelling so the call succeeds.
exec "$PODMAN" run --rm -i ${TTY_ARG} \
    --platform "$PLATFORM" \
    --security-opt seccomp=unconfined \
    --security-opt label=disable \
    -v "$REPO_ROOT":/work:Z \
    -w /work \
    "$IMAGE" \
    zig run scripts/probe_io_uring.zig
