#!/usr/bin/env bash
# The knee of the latency curve for the overhead experiment (node-0, after the campaign): config C
# (record everything into our agent at a collection volume target of 12 % of the spans at the
# step's rate), one reseed, then per rate: fresh agents and central with that target, a 30-s
# warm-up, 2 minutes of load.  The load is wrk2 with exponential gaps (-D exp; its fixed pace sends
# the connections' requests as one burst) on one connection per req/s: wrk2 does not wait for the
# response before the next request of a connection, and the server takes a connection's requests
# one at a time, so on 10 connections one request in seven would wait behind another.  The higher
# rate of the experiment is the highest rate whose p99 stays within KNEE_X (2) x the p99 at the
# first rate (100 req/s) and whose achieved rate is within 1 % of the target (and three standard
# deviations of the Poisson count); it is printed last ("knee rate: N").  No such line, and exit 1,
# when our agents or the central do not come up at a rate, or the first rate has no p99.
#   tools/overhead/knee.sh        (log and wrk logs in $STAGE3/overhead/knee/)
# A wider sweep stops at the first rate past the knee (UNTIL=1), in a directory of its own:
#   UNTIL=1 RATES="100 350 400 450 500 ..." KNEE_DIR=knee2 tools/overhead/knee.sh
set -u
cd "$(dirname "$0")/../.."
MOD=$(pwd)
# shellcheck disable=SC1090
source ~/bridges-env.sh 2>/dev/null || true
: "${STAGE3:=/storage/royno7/stage3}"; : "${RATES:=100 150 200 250 300 350}"; : "${STEP:=120}"; : "${KNEE_X:=2}"; : "${UNTIL:=0}"; : "${KNEE_DIR:=knee}"
: "${URL:=${WRK2API_URL:-http://10.10.1.1:22903}}"
: "${LUA:=$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/compose-post.lua}"
: "${IMAGE_TAG:=20261121-224737}"; : "${CALLGRAPH:=$MOD/deploy/callgraph.stages.json}"
IMG=10.10.1.1:30000/otelcol-slobudget:$IMAGE_TAG
# shellcheck disable=SC1091
source "$MOD/tools/campaign/faults.env"
RD=$STAGE3/overhead/$KNEE_DIR
mkdir -p "$RD"
log() { echo "$(date '+%F %T') $*" | tee -a "$RD/run.log"; }
restore() {
    kubectl replace -f "$STAGE3/otelcol-ds.stock.yaml" >> "$RD/run.log" 2>&1
    kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$RD/run.log" 2>&1
    kubectl delete deploy/slobudget-central svc/slobudget-central cm/slobudget-central-config cm/slobudget-callgraph \
        --ignore-not-found >> "$RD/run.log" 2>&1
}
fail() { log "FAILED: $*"; restore; exit 1; }
[ "$(kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')" = "$STOCK_COLLECTOR_IMAGE" ] \
    || { log "the stock collector is not in place"; exit 1; }
[ -z "$(kubectl get ds,pods -l app=slobudget-arrival --no-headers 2>/dev/null)" ] || { log "arrivald is still there (an E run)"; exit 1; }
WORKLOADS=$(kubectl get deploy,ds -o name | grep -- '-service-pb-es-ctr' | grep -v tracepressure)
[ -n "$WORKLOADS" ] || { log "no service workloads"; exit 1; }
for w in $WORKLOADS; do kubectl set env "$w" OTEL_SAMPLE_RATIO- SLO_BUDGET_PORT- BRIDGE_KIND=pb >> "$RD/run.log" 2>&1 || fail "set env $w"; done
for w in $WORKLOADS; do kubectl rollout status "$w" --timeout=300s >> "$RD/run.log" 2>&1 || fail "rollout $w"; done
bash "$MOD/tools/reseed.sh" > "$RD/reseed.log" 2>&1 || fail "reseed: $(tail -1 "$RD/reseed.log")"
rm -f "$RD"/wrk-*.log
# p99, whether it is past the knee: of rate $1 against the first rate
past() {
    python3 - "$RD" "$1" "${RATES%% *}" "$KNEE_X" "$STEP" <<'PY'
import math, re, sys
rd, r, r0, kx, step = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5])
def stat(r):
    t = open(f"{rd}/wrk-{r}.log", errors="replace").read()
    p = {k: float(v) * {"ms": 1, "s": 1000, "us": 0.001}[u] for k, v, u in re.findall(r"^\s*(50\.000|99\.000)%\s+([0-9.]+)(ms|s|us)", t, re.M)}
    m = re.search(r"Requests/sec:\s+([0-9.]+)", t)
    return p.get("99.000"), float(m.group(1)) if m else 0.0
(p99, got), (base, _) = stat(r), stat(r0)
sys.exit(0 if p99 is None or base is None or p99 > kx * base or got < 0.99 * r - 3 * math.sqrt(r / step) else 1)
PY
}
SWEPT=""
for r in $RATES; do
    # agents and central new at every rate: the target is 12 % of the spans at this rate (an agent
    # ignores a restarted central until its version passes the agent's, so both restart)
    python3 "$MOD/tools/campaign/render.py" --arm chosen --volume 0.12 --rate "$r" --seed 1 --image "$IMG" --no-tee --callgraph "$CALLGRAPH" > "$RD/manifests-$r.yaml" || fail "render at $r"
    kubectl apply -f "$RD/manifests-$r.yaml" >> "$RD/run.log" 2>&1 || fail "apply at $r"
    kubectl rollout restart deploy/slobudget-central ds/otelcol-pb-es-ctr >> "$RD/run.log" 2>&1 || fail "restart at $r"
    kubectl rollout status deploy/slobudget-central --timeout=180s >> "$RD/run.log" 2>&1 || fail "central did not come up at $r"
    kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$RD/run.log" 2>&1 || fail "agents did not roll at $r"
    sleep 15
    NREADY=$(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr --no-headers | grep -c Running)
    [ "$NREADY" = 3 ] || fail "collectors running at $r: $NREADY"
    wrk -t2 -c"$r" -D exp -d30s -s "$LUA" "$URL" -R"$r" > "$RD/wrk-warmup-$r.log" 2>&1
    RANDOM_SEED=1 wrk -t2 -c"$r" -D exp -d"${STEP}"s -L -s "$LUA" "$URL" -R"$r" > "$RD/wrk-$r.log" 2>&1
    log "rate $r: $(grep -E '^ *(50.000|99.000)%|Requests/sec' "$RD/wrk-$r.log" | tr -s ' ' | tr '\n' ' ')"
    sleep 15
    SWEPT="$SWEPT $r"
    if [ "$UNTIL" = 1 ] && past "$r"; then log "past the knee at $r"; break; fi
done
restore
python3 - "$RD" "$SWEPT" "$KNEE_X" "$STEP" <<'PY'
import math, re, sys
rd, rates, kx, step = sys.argv[1], [int(x) for x in sys.argv[2].split()], float(sys.argv[3]), float(sys.argv[4])
def stat(r):
    t = open(f"{rd}/wrk-{r}.log", errors="replace").read()
    p = {k: float(v) * {"ms": 1, "s": 1000, "us": 0.001}[u] for k, v, u in re.findall(r"^\s*(50\.000|99\.000)%\s+([0-9.]+)(ms|s|us)", t, re.M)}
    m = re.search(r"Requests/sec:\s+([0-9.]+)", t)
    return p.get("50.000"), p.get("99.000"), float(m.group(1)) if m else 0.0
base = stat(rates[0])[1]
if base is None:
    sys.exit(f"no p99 at {rates[0]} req/s: no knee")
knee = rates[0]
for r in rates:
    p50, p99, got = stat(r)
    ok = p99 is not None and p99 <= kx * base and got >= 0.99 * r - 3 * math.sqrt(r / step)
    print(f"rate {r}: p50 {p50} ms p99 {p99} ms achieved {got} req/s {'within' if ok else 'past the knee'}")
    if not ok:
        break
    knee = r
print(f"knee rate: {knee}")
PY
