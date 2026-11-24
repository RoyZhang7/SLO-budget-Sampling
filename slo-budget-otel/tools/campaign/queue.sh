#!/usr/bin/env bash
# Stage 3 queue runner (node-0, under nohup):  tools/campaign/queue.sh [queue_file]
#
# The queue file ($STAGE3/queue.txt by default) holds one job per line,
#   arm scenario volume seed [KEY=VALUE ...]
# ('#' comments; the KEY=VALUE tokens go to run_one.sh as environment, e.g. RUN_ID=... TEE=0).
# It is re-read before every job, so jobs can be appended or reordered while it runs.  A job
# whose run directory has `collected` is skipped; a failed job is retried once, then left
# `failed`.  Touch $STAGE3/STOP (or let a sanity check do it) to halt after the current run;
# the runner waits until STOP is removed; it writes STOP itself when four tries in a row failed.
# Progress: $STAGE3/queue.log.
set -u
cd "$(dirname "$0")/../.."
MOD=$(pwd)
: "${STAGE3:=/storage/royno7/stage3}"
Q=${1:-$STAGE3/queue.txt}
LOG=$STAGE3/queue.log
log() { echo "$(date '+%F %T') $*" >> "$LOG"; }
rundir() {   # rundir arm scenario volume seed [env tokens]
    local id=""
    for tok in "${@:5}"; do case "$tok" in RUN_ID=*) id=${tok#RUN_ID=} ;; esac; done
    [ -n "$id" ] || id="v$(python3 -c "print(round($3*100))")-$2-$1-s$4"
    echo "$STAGE3/runs/$id"
}
log "queue runner start (pid $$, queue $Q)"
row=0
while true; do
    if [ -f "$STAGE3/STOP" ]; then
        log "STOP present: $(tail -1 "$STAGE3/STOP")"
        while [ -f "$STAGE3/STOP" ]; do sleep 30; done
        log "STOP removed, continuing"
    fi
    next=""
    while read -r line; do
        line=${line%%#*}
        [ -z "${line// /}" ] && continue
        # shellcheck disable=SC2086
        rd=$(rundir $line)
        [ -f "$rd/collected" ] && continue
        tries=$(cat "$rd/tries" 2>/dev/null || echo 0)
        [ "$tries" -ge 2 ] && continue
        next=$line; break
    done < "$Q"
    if [ -z "$next" ]; then
        log "queue empty; waiting for jobs"
        sleep 60
        continue
    fi
    # shellcheck disable=SC2086
    set -- $next
    rd=$(rundir "$@")
    mkdir -p "$rd"
    echo $(( $(cat "$rd/tries" 2>/dev/null || echo 0) + 1 )) > "$rd/tries"
    log "start $next (try $(cat "$rd/tries"))"
    if env "${@:5}" bash "$MOD/tools/campaign/run_one.sh" "$1" "$2" "$3" "$4" > "$rd/run_one.out" 2>&1; then
        log "collected $next"
        row=0
    else
        log "FAILED $next: $(cat "$rd/failed" 2>/dev/null)"
        row=$((row + 1))
        if [ $row -ge 4 ]; then echo "$row tries in a row failed, the last one $next: $(cat "$rd/failed" 2>/dev/null)" >> "$STAGE3/STOP"; row=0; fi
        sleep "${PAUSE:-60}"
    fi
done
