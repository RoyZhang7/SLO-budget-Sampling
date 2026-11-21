#!/usr/bin/env bash
# Restore the DSB-SN social graph to the profile's seeded state (node-0).
#
# The variant's Mongo and Redis deployments keep their data in the container layer (no
# volumes), so restarting them empties them; the app services are restarted with them so
# that no connection or in-process cache outlives the stores; then the profile's seed is
# replayed (init_social_graph.py: 962 users registered and the socfb-Reed98 follow graph,
# no posts).  Every 10-minute ComposePost run at 100 req/s adds 60 000 posts, which slows
# usertimeline and drifts the healthy table, so reseed before a measurement series.
#
#   make reseed            (or: bash tools/reseed.sh)
set -u -o pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1090
source ~/bridges-env.sh 2>/dev/null || true
: "${SEED_DIR:=$HOME/DeathStarBench/socialNetwork}"
: "${SEED_PY:=$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/init_social_graph.py}"
: "${SUFFIX:=pb-es-ctr}"
# the seed needs aiohttp: the profile's Blueprint venv has it
PY=python3
[ -x "$HOME/blueprint-docc-mod/.venv/bin/python3" ] && PY="$HOME/blueprint-docc-mod/.venv/bin/python3"
"$PY" -c "import aiohttp" 2>/dev/null || { echo "no python with aiohttp (tried $PY); install it or point SEED_PY's interpreter at the Blueprint venv"; exit 1; }
NP=$(kubectl get svc "wrk2api-service-$SUFFIX" -o jsonpath='{.spec.ports[?(@.port==2000)].nodePort}')
[ -n "$NP" ] || { echo "no wrk2api NodePort"; exit 1; }
counts() {
    for db in user social post usertimeline urlshorten; do
        printf "  %s-db: " "$db"
        kubectl exec "deploy/$db-db-$SUFFIX" -- mongo --quiet --eval 'db.getMongo().getDBNames().forEach(function(n){ if (n=="admin"||n=="config"||n=="local") return; var d=db.getSiblingDB(n); d.getCollectionNames().forEach(function(c){ print(n+"."+c+"="+d[c].count()) }) })' 2>/dev/null | tr '\n' ' '
        echo
    done
}
echo "== before"; counts
STORES="user-db social-db post-db urlshorten-db usertimeline-db user-cache social-cache post-cache usertimeline-cache hometimeline-cache"
echo "== restart the stores (their data lives in the container layer)"
for d in $STORES; do kubectl rollout restart "deploy/$d-$SUFFIX" > /dev/null; done
for d in $STORES; do kubectl rollout status "deploy/$d-$SUFFIX" --timeout=180s | tail -1; done
echo "== restart the services"
SVCS=$(kubectl get deploy -o name | grep -- "-service-$SUFFIX")
for d in $SVCS; do kubectl rollout restart "$d" > /dev/null; done
kubectl rollout restart "ds/wrk2api-service-$SUFFIX" > /dev/null
for d in $SVCS; do kubectl rollout status "$d" --timeout=240s | tail -1; done
kubectl rollout status "ds/wrk2api-service-$SUFFIX" --timeout=240s | tail -1
sleep 10
echo "== seed (register + follow, as the profile does; $PY)"
if ! ( cd "$SEED_DIR" && "$PY" "$SEED_PY" --ip 10.10.1.1 --port "$NP" 2>&1 | tail -3 ); then
    echo "seed failed"; exit 1
fi
echo "== after"; counts
users=$(kubectl exec "deploy/user-db-$SUFFIX" -- mongo --quiet --eval 'db.getSiblingDB("user").user.count()' 2>/dev/null | tail -1)
[ "${users:-0}" -ge 900 ] || { echo "seed verification failed: $users users"; exit 1; }
echo "reseed done $(date '+%F %T'): $users users"
