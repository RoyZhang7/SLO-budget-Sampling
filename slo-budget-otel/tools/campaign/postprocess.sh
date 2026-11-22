#!/usr/bin/env bash
# Postprocess one collected Stage 3 run (node-0, background, one at a time):
#   Jaeger export -> gate.jsonl -> shadow compression -> evaluation (tools/eval) -> gaps per call
#   edge (faulted runs) -> export compression -> sanity checks (a failed check writes $STAGE3/STOP and the queue halts).
#   tools/campaign/postprocess.sh <run_dir>
set -u
RD=$1
cd "$(dirname "$0")/../.."
MOD=$(pwd)
: "${STAGE3:=/storage/royno7/stage3}"
: "${JQ:=http://192.168.134.21:16686}"
: "${PY:=$HOME/slo-budget-sim/.venv/bin/python3}"
exec 9> "$STAGE3/postprocess.lock"
flock 9
log() { echo "$(date '+%F %T') $*"; }
log "postprocess $RD"
SERVICES=$(for n in wrk2api composepost hometimeline usertimeline post_storage text urlshorten usermention socialgraph uniqueid media user; do printf 'unknown_service:%s_service_pb_es_proc,' $n; done)
T0=$(python3 -c "import json; print(int(json.load(open('$RD/meta.json'))['t0']))")
T1=$(python3 -c "import json; print(int(json.load(open('$RD/meta.json'))['t1']) + 1)")
if [ ! -s "$RD/export.json" ] && [ ! -s "$RD/export.json.zst" ]; then
    nice -n 5 python3 "$HOME/slo-budget-sim/tools/jaeger_export.py" --base "$JQ" --service "$SERVICES" --start $((T0 - 5)) --end $((T1 + 30)) \
        --chunk 60 --limit 50000 --out "$RD/export.json" > "$RD/export.log" 2>&1 || { log "export failed"; echo "export" > "$RD/postprocess-failed"; exit 1; }
    log "export: $(tail -1 "$RD/export.log")"
fi
grep -h 'capgate period' "$RD/central.log" | sed 's/^[^{]*//' > "$RD/gate.jsonl"
if [ -f "$RD/shadow.jsonl" ]; then
    nice -n 5 zstd -T8 -q -f "$RD/shadow.jsonl" -o "$RD/shadow.jsonl.zst" && rm -f "$RD/shadow.jsonl"
fi
[ -s "$RD/export.json.zst" ] && [ ! -s "$RD/export.json" ] && zstd -dq "$RD/export.json.zst" -o "$RD/export.json"
if [ -f "$MOD/tools/eval/eval_run.py" ]; then
    nice -n 5 "$PY" "$MOD/tools/eval/eval_run.py" "$RD" --sim "$HOME/slo-budget-sim" > "$RD/eval.log" 2>&1 \
        || { log "eval failed: $(tail -2 "$RD/eval.log" | tr '\n' ' ')"; echo "eval" > "$RD/postprocess-failed"; }
fi
if [ -f "$MOD/tools/eval/gaps_run.py" ] && [ ! -s "$RD/gaps.json" ]; then   # Stage 3: gap per call edge, before vs during a fault
    nice -n 5 "$PY" "$MOD/tools/eval/gaps_run.py" "$RD" > "$RD/gaps.log" 2>&1 || log "gaps failed: $(tail -1 "$RD/gaps.log")"
fi
[ -s "$RD/export.json" ] && nice -n 5 zstd -T8 -q -f "$RD/export.json" -o "$RD/export.json.zst" && rm -f "$RD/export.json"
python3 "$MOD/tools/campaign/sanity.py" "$RD" > "$RD/sanity.txt" 2>&1
if [ "${SANITY:-1}" = 1 ] && ! grep -q '^OK' "$RD/sanity.txt"; then
    log "sanity check failed: $(head -5 "$RD/sanity.txt" | tr '\n' ' ')"
    echo "$(basename "$RD"): $(head -3 "$RD/sanity.txt" | tr '\n' ' ')" >> "$STAGE3/STOP"
fi
touch "$RD/done"
log "postprocess done $RD"
