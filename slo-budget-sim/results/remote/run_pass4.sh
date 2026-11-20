#!/usr/bin/env bash
# Pass 4 remote long run (node-0 of the CloudLab cluster), detached under tmux:
#   (a) 10 minutes of wrk2 ComposePost load against the bridged DSB-SN deployment,
#       then a chunked export of the traces from the Jaeger query API
#   (b) derive.py on that export: refit the generator's timing distributions
#   (c) cli all, 3 seeds, V* = 25 / 12 / 5 %, on the refitted workload (exp9 included)
# Nothing in the deployment is changed, restarted or configured.
# Logs: results/remote/run.log (phases), wrk.log, export.log, derive.log, cli.log.
set -u
cd "$(dirname "$0")/../.."
R=results/remote
mkdir -p "$R"
log() { echo "$(date '+%F %T') $*" | tee -a "$R/run.log"; }
# shellcheck disable=SC1090
source ~/bridges-env.sh 2>/dev/null || true
URL=${WRK2API_URL:-http://10.10.1.1:22903}
JQ=${JAEGER_QUERY:-http://192.168.134.21:16686}
LUA=${LUA:-$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/compose-post.lua}
SERVICE=unknown_service:wrk2api_service_pb_es_proc
DUR=${DUR:-600}
RPS=${RPS:-100}
JOBS=${JOBS:-20}

log "pass 4 remote run starting on $(hostname); wrk2api $URL; jaeger $JQ"
log "phase a: wrk -t2 -c10 -d${DUR}s -R$RPS ComposePost only (no mixed-workload script or rate is configured for the read routes)"
T0=$(date +%s)
wrk -t2 -c10 -d"${DUR}"s -L -s "$LUA" "$URL" -R"$RPS" > "$R/wrk.log" 2>&1
T1=$(date +%s)
log "wrk finished: $(grep -E 'requests in|Non-2xx|Requests/sec' "$R/wrk.log" | tr '\n' ' ')"
sleep 30                                   # let the collector and Jaeger drain
log "phase a: export $SERVICE from $((T0 - 5)) to $((T1 + 30)) in 60 s chunks"
python3 tools/jaeger_export.py --base "$JQ" --service "$SERVICE" --start $((T0 - 5)) --end $((T1 + 30)) \
    --chunk 60 --limit 50000 --out "$R/export-10min.json" > "$R/export.log" 2>&1
log "export: $(tail -1 "$R/export.log")"
if [ ! -s "$R/export-10min.json" ]; then
    log "export failed, see $R/export.log; stopping before derive"
    exit 1
fi

log "phase b: derive timing distributions from the export"
cp slo_budget_sim/workload_dsbsn.json "$R/workload_provisional.json"
.venv/bin/python -m slo_budget_sim.derive "$R/export-10min.json" -o slo_budget_sim/workload_dsbsn.json > "$R/derive.log" 2>&1 \
    || { log "derive failed, see $R/derive.log"; exit 1; }
cp slo_budget_sim/workload_dsbsn.json "$R/workload_dsbsn.json"
log "derive: $(head -1 "$R/derive.log")"
.venv/bin/python -m pytest -q 2>&1 | tail -1 | tee -a "$R/run.log"

log "phase c: cli all --jobs $JOBS --volumes 0.25 0.12 0.05 (3 seeds) -> $R/results"
.venv/bin/python -m slo_budget_sim.cli all --jobs "$JOBS" --volumes 0.25 0.12 0.05 --results "$R/results" > "$R/cli.log" 2>&1
log "cli all exit $?: $(grep -c ' runs in ' "$R/cli.log") experiments written"
log "done"
