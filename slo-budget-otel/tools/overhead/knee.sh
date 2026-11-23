#!/usr/bin/env bash
# The knee of the latency curve for the overhead experiment (node-0, after the campaign): config C
# (record everything into our agent at 12 %, the heaviest of the three), one reseed, a 30-s
# warm-up, then RATES back to back, 2 minutes each.  The higher rate of the experiment is the
# highest rate whose p99 stays within KNEE_X (2) x the p99 at the first rate (100 req/s) and
# whose achieved rate is within 1 % of the target; it is printed last ("knee rate: N").
#   tools/overhead/knee.sh        (log and wrk logs in $STAGE3/overhead/knee/)
set -u
cd "$(dirname "$0")/../.."
MOD=$(pwd)
# shellcheck disable=SC1090
source ~/bridges-env.sh 2>/dev/null || true
: "${STAGE3:=/storage/royno7/stage3}"; : "${RATES:=100 150 200 250 300 350}"; : "${STEP:=120}"; : "${KNEE_X:=2}"
: "${URL:=${WRK2API_URL:-http://10.10.1.1:22903}}"
: "${LUA:=$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/compose-post.lua}"
: "${IMAGE_TAG:=20261121-224737}"
IMG=10.10.1.1:30000/otelcol-slobudget:$IMAGE_TAG
# shellcheck disable=SC1091
source "$MOD/tools/campaign/faults.env"
RD=$STAGE3/overhead/knee
mkdir -p "$RD"
log() { echo "$(date '+%F %T') $*" | tee -a "$RD/run.log"; }
restore() {
    kubectl replace -f "$STAGE3/otelcol-ds.stock.yaml" >> "$RD/run.log" 2>&1
    kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$RD/run.log" 2>&1
    kubectl delete deploy/slobudget-central svc/slobudget-central cm/slobudget-central-config cm/slobudget-callgraph \
        --ignore-not-found >> "$RD/run.log" 2>&1
}
[ "$(kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')" = "$STOCK_COLLECTOR_IMAGE" ] \
    || { log "the stock collector is not in place"; exit 1; }
WORKLOADS=$(kubectl get deploy,ds -o name | grep -- '-service-pb-es-ctr' | grep -v tracepressure)
for w in $WORKLOADS; do kubectl set env "$w" OTEL_SAMPLE_RATIO- SLO_BUDGET_PORT- >> "$RD/run.log" 2>&1; done
for w in $WORKLOADS; do kubectl rollout status "$w" --timeout=300s >> "$RD/run.log" 2>&1; done
bash "$MOD/tools/reseed.sh" > "$RD/reseed.log" 2>&1 || { log "reseed failed"; exit 1; }
python3 "$MOD/tools/campaign/render.py" --arm chosen --volume 0.12 --seed 1 --image "$IMG" --no-tee > "$RD/manifests.yaml"
kubectl apply -f "$RD/manifests.yaml" >> "$RD/run.log" 2>&1
kubectl rollout status deploy/slobudget-central --timeout=180s >> "$RD/run.log" 2>&1
kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$RD/run.log" 2>&1
sleep 15
first=${RATES%% *}
wrk -t2 -c10 -d30s -s "$LUA" "$URL" -R"$first" > "$RD/wrk-warmup.log" 2>&1
for r in $RATES; do
    c=$(( r / 10 > 10 ? r / 10 : 10 ))
    RANDOM_SEED=1 wrk -t2 -c"$c" -d"${STEP}"s -L -s "$LUA" "$URL" -R"$r" > "$RD/wrk-$r.log" 2>&1
    log "rate $r: $(grep -E '^ *(50.000|99.000)%|Requests/sec' "$RD/wrk-$r.log" | tr -s ' ' | tr '\n' ' ')"
    sleep 15
done
restore
python3 - "$RD" "$RATES" "$KNEE_X" <<'PY'
import re, sys
rd, rates, kx = sys.argv[1], [int(x) for x in sys.argv[2].split()], float(sys.argv[3])
def stat(r):
    t = open(f"{rd}/wrk-{r}.log", errors="replace").read()
    p = {k: float(v) * {"ms": 1, "s": 1000, "us": 0.001}[u] for k, v, u in re.findall(r"^\s*(50\.000|99\.000)%\s+([0-9.]+)(ms|s|us)", t, re.M)}
    m = re.search(r"Requests/sec:\s+([0-9.]+)", t)
    return p.get("50.000"), p.get("99.000"), float(m.group(1)) if m else 0.0
base = stat(rates[0])[1]
knee = rates[0]
for r in rates:
    p50, p99, got = stat(r)
    ok = p99 is not None and p99 <= kx * base and got >= 0.99 * r
    print(f"rate {r}: p50 {p50} ms p99 {p99} ms achieved {got} req/s {'within' if ok else 'past the knee'}")
    if not ok:
        break
    knee = r
print(f"knee rate: {knee}")
PY
