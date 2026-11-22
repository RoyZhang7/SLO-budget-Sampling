#!/usr/bin/env bash
# Fault calibration on node-0 with the deployment's stock collector (100 % of spans to
# Jaeger): for each phase start the fault, run wrk2 ComposePost for PHASE s at 100 req/s,
# stop it; then pull a sample of each phase's traces from Jaeger and report per phase the
# target services' server-span P50 / P95 and the share of requests whose root exceeds the
# SLO.  No agents or central are deployed.  A phase is `base` or `<mode> <service|all> <level>
# [extra]` as tools/hog/fault.sh takes it (burn: `burn <service> <threads> <quota_us>/<period_us>`);
# the first phase must be `base` for calib_own.py (own time against the own-time share b).
#   tools/hog/calibrate.sh            phases from PHASES below
set -u
cd "$(dirname "$0")/../.."
source ~/bridges-env.sh 2>/dev/null || true
: "${PHASE:=40}" ; : "${GAP:=15}" ; : "${RPS:=100}"
: "${URL:=${WRK2API_URL:-http://10.10.1.1:22903}}"
: "${LUA:=$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/compose-post.lua}"
R=results/calib-$(date +%Y%m%d-%H%M%S)
mkdir -p "$R"
: "${PHASES:=base;spin socialgraph 80;spin socialgraph 240;quota socialgraph 1500;quota socialgraph 500;spin post-storage 240;quota post-storage 1000;quota post-storage 400;spin all 80;quota all 3000;quota all 1500}"
kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}' | tee "$R/collector-image.txt"; echo
kubectl get pods -o wide --no-headers | awk '{print $1, $7}' > "$R/placement.txt"
if [ "${RESEED:-0}" = 1 ]; then   # as a run starts: fresh data, then a 30-s warm-up
    bash tools/reseed.sh > "$R/reseed.log" 2>&1 || { echo "reseed failed: $(tail -1 "$R/reseed.log")"; exit 1; }
    wrk -t2 -c10 -d30s -s "$LUA" "$URL" -R"$RPS" > "$R/wrk-warmup.log" 2>&1
fi
IFS=';' read -ra PH <<< "$PHASES"
echo "[" > "$R/phases.json"
first=1
for ph in "${PH[@]}"; do
    set -- $ph
    sleep "$GAP"
    t_start=$(date +%s.%N)
    if [ "$1" != base ]; then
        tools/hog/fault.sh "$1" "$2" "$3" $((PHASE + 5)) ${4:-} >> "$R/faults.log" 2>&1
        sleep 2
    fi
    t0=$(date +%s.%N)
    RANDOM_SEED=1 wrk -t2 -c10 -d"${PHASE}"s -L -s "$LUA" "$URL" -R"$RPS" > "$R/wrk-$(echo "$ph" | tr ' /' '_-').log" 2>&1
    t1=$(date +%s.%N)
    case "$1" in quota|burn) tools/hog/fault.sh clear "$2" >> "$R/faults.log" 2>&1 ;; esac
    [ $first = 1 ] || echo "," >> "$R/phases.json"
    first=0
    printf '{"phase": "%s", "t0": %s, "t1": %s}' "$ph" "$t0" "$t1" >> "$R/phases.json"
    echo "$(date '+%T') phase '$ph' done: $(grep -E 'Requests/sec|Latency ' "$R/wrk-$(echo "$ph" | tr ' /' '_-').log" | head -2 | tr -s ' ' | tr '\n' ' ')"
done
echo "]" >> "$R/phases.json"
sleep 20
python3 tools/hog/calib_analyze.py "$R" | tee "$R/summary.txt"
PY=$HOME/slo-budget-sim/.venv/bin/python3
[ -x "$PY" ] && nice -n 5 "$PY" tools/hog/calib_own.py "$R" | tee "$R/summary-own.txt"
echo "$R"
