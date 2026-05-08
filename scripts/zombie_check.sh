#!/usr/bin/env sh
# Zombie-process check.
#
# Lists (and optionally kills) any orphan `zig build test` or
# `.zig-cache/o/.../test` processes from previous runs that are still
# alive. The fingerprint of a stuck test process: alive for many
# minutes / hours but with very low CPU time — it's blocked on a
# futex / select that will never wake.
#
# Usage:
#   scripts/zombie_check.sh           # report only
#   scripts/zombie_check.sh --kill    # send SIGKILL to all zombies
#   scripts/zombie_check.sh --strict  # exit 1 if any zombies found

set -eu

ACTION="${1:-}"

# `ps -o etime` gives DD-HH:MM:SS or HH:MM:SS or MM:SS
# Convert to seconds for filtering.
to_secs() {
    e="$1"
    case "$e" in
        *-*) # has days
            d=$(printf "%s\n" "$e" | cut -d- -f1)
            rest=$(printf "%s\n" "$e" | cut -d- -f2)
            ;;
        *)
            d=0
            rest="$e"
            ;;
    esac
    # rest is HH:MM:SS or MM:SS or SS
    case "$rest" in
        *:*:*)
            h=$(printf "%s\n" "$rest" | cut -d: -f1)
            m=$(printf "%s\n" "$rest" | cut -d: -f2)
            s=$(printf "%s\n" "$rest" | cut -d: -f3)
            ;;
        *:*)
            h=0
            m=$(printf "%s\n" "$rest" | cut -d: -f1)
            s=$(printf "%s\n" "$rest" | cut -d: -f2)
            ;;
        *)
            h=0; m=0; s="$rest"
            ;;
    esac
    echo $((d * 86400 + h * 3600 + m * 60 + s))
}

# Threshold: if a zig-test process has been alive > 60s with < 10s
# CPU, it's almost certainly hung.
ELAPSED_THRESHOLD=60

found=0

ps -ax -o pid,etime,time,command \
    | grep -E "zig build test|test --cache-dir|\.zig-cache.*test " \
    | grep -v grep \
    | grep -v "$0" \
    | while read -r pid etime cputime cmd; do
        # Strip "0:09.02" CPU time → seconds (rough)
        cpu_s=$(printf "%s" "$cputime" | awk -F: 'NF==2 { print int($1*60 + $2) } NF==3 { print int($1*3600 + $2*60 + $3) }')
        elapsed_s=$(to_secs "$etime")
        if [ "$elapsed_s" -gt "$ELAPSED_THRESHOLD" ] && [ "${cpu_s:-0}" -lt 10 ]; then
            printf "ZOMBIE pid=%s etime=%s cpu=%s cmd=%s\n" "$pid" "$etime" "$cputime" "$cmd"
            found=$((found + 1))
            if [ "$ACTION" = "--kill" ]; then
                kill -KILL "$pid" 2>/dev/null && printf "  → killed\n"
            fi
        fi
    done

# `found` got set in a subshell so we can't read it here directly.
# Re-run the check, count via wc -l.
zombie_count=$(
    ps -ax -o pid,etime,time,command \
        | grep -E "zig build test|test --cache-dir|\.zig-cache.*test " \
        | grep -v grep \
        | grep -v "$0" \
        | while read -r pid etime cputime cmd; do
            cpu_s=$(printf "%s" "$cputime" | awk -F: 'NF==2 { print int($1*60 + $2) } NF==3 { print int($1*3600 + $2*60 + $3) }')
            elapsed_s=$(to_secs "$etime")
            if [ "$elapsed_s" -gt "$ELAPSED_THRESHOLD" ] && [ "${cpu_s:-0}" -lt 10 ]; then
                echo z
            fi
        done | wc -l | tr -d ' '
)

printf "\n"
if [ "$zombie_count" -eq 0 ]; then
    printf "No zombie zig test processes detected.\n"
else
    printf "%s zombie zig test process(es) detected.\n" "$zombie_count"
    if [ "$ACTION" != "--kill" ]; then
        printf "Run with --kill to terminate them.\n"
    fi
fi

if [ "$ACTION" = "--strict" ] && [ "$zombie_count" -gt 0 ]; then
    exit 1
fi
exit 0
