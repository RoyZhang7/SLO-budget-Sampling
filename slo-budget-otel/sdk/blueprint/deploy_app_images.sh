#!/bin/bash
# Switch the live pb_es service Deployments / the wrk2api DaemonSet to the rebuilt images
# (SDK budget issuer); DBs, caches, Jaeger and ES stay.  Reversible: kubectl rollout undo.
set -uo pipefail
source ~/bridges-env.sh
TAG=$(cat ~/bp-patch/IMAGE_TAG)
echo "== rebuild sanity (from the build log)"
grep -E "trace.go with issuer|issuer files|requestBytes in wrk2api|FAIL" ~/bp-patch/rebuild.log
n=$(grep -oE "trace.go with issuer: [0-9]+" ~/bp-patch/rebuild.log | grep -oE "[0-9]+$")
rb=$(grep -oE "requestBytes in wrk2api handler: [0-9]+" ~/bp-patch/rebuild.log | grep -oE "[0-9]+$")
fails=$(grep -c "^FAIL" ~/bp-patch/rebuild.log)
if [ "${n:-0}" -lt 14 ] || [ "${rb:-0}" -lt 1 ] || [ "$fails" -gt 0 ]; then echo "sanity failed (issuer in $n workspaces, requestBytes $rb, fails $fails): not deploying"; exit 1; fi
echo "== images before"
kubectl get deploy,ds -o custom-columns=KIND:.kind,NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image --no-headers | grep -- "-service-pb-es-ctr" | tee ~/bp-patch/images-before.txt
echo "== set image -> $TAG"
for d in $(kubectl get deploy -o name | grep -- "-service-pb-es-ctr"); do
    name=${d#deployment.apps/}
    kubectl set image "$d" "$name=10.10.1.1:30000/$name:$TAG"
done
kubectl set image ds/wrk2api-service-pb-es-ctr "wrk2api-service-pb-es-ctr=10.10.1.1:30000/wrk2api-service-pb-es-ctr:$TAG"
echo "== rollouts"
for d in $(kubectl get deploy -o name | grep -- "-service-pb-es-ctr"); do
    kubectl rollout status "$d" --timeout=240s | tail -1
done
kubectl rollout status ds/wrk2api-service-pb-es-ctr --timeout=240s | tail -1
sleep 5
echo "== pods"
kubectl get pods -o wide --no-headers | grep -- "-service-pb-es-ctr" | awk '{print $1, $3, $7}'
echo "== issuer log lines (composepost, wrk2api)"
kubectl logs deploy/composepost-service-pb-es-ctr --tail=200 | grep -iE "slobudget|bridge_kind" | head -5
kubectl logs ds/wrk2api-service-pb-es-ctr --tail=200 2>/dev/null | grep -iE "slobudget|bridge_kind" | head -5
echo "== smoke: 10 s at 50 rps"
wrk -t2 -c10 -d10s -s "$BLUEPRINT_DIR/examples/dsb_sn/scripts/compose-post.lua" "$WRK2API_URL" -R50 2>&1 | grep -E "requests in|Non-2xx|Socket errors"
echo "== done"
