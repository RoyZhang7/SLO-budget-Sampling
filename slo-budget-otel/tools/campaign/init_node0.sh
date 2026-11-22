#!/usr/bin/env bash
# One-time Stage 3 setup on node-0 (idempotent): directories, the stock collector DaemonSet
# saved for restores (only while the stock image is in place), the Service port the SDK's
# budget issuer polls, and config validation of every collector config the arms use.
#   tools/campaign/init_node0.sh [image_tag]
set -u
cd "$(dirname "$0")/../.."
MOD=$(pwd)
: "${STAGE3:=/storage/royno7/stage3}"
TAG=${1:-$(cat build/IMAGE_TAG)}
BIN=$MOD/build/otelcol-slobudget/otelcol-slobudget
# shellcheck disable=SC1091
source "$MOD/tools/campaign/faults.env"
mkdir -p "$STAGE3/runs" "$STAGE3/tmp"
img=$(kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')
if [ ! -s "$STAGE3/otelcol-ds.stock.yaml" ]; then
    [ "$img" = "$STOCK_COLLECTOR_IMAGE" ] || { echo "the DaemonSet is not the stock one ($img): not saving"; exit 1; }
    kubectl get ds otelcol-pb-es-ctr -o json | python3 -c '
import json, sys
d = json.load(sys.stdin)
d.pop("status", None)
m = d["metadata"]
for k in ("uid", "resourceVersion", "creationTimestamp", "generation", "managedFields", "selfLink"):
    m.pop(k, None)
ann = m.get("annotations", {})
for k in ("kubectl.kubernetes.io/last-applied-configuration", "deprecated.daemonset.template.generation"):
    ann.pop(k, None)
json.dump(d, sys.stdout, indent=1)' > "$STAGE3/otelcol-ds.stock.yaml"
    echo "saved the stock DaemonSet ($img)"
fi
if ! kubectl get svc otelcol-pb-es-ctr -o jsonpath='{.spec.ports[*].port}' | grep -qw 8082; then
    kubectl patch svc otelcol-pb-es-ctr --type=json -p='[{"op":"add","path":"/spec/ports/-","value":{"name":"slobudget-sdk","port":8082,"protocol":"TCP","targetPort":8082}}]'
fi
echo "== validate collector configs (image tag $TAG)"
export NODE_NAME=node-1
ok=1
for arm in chosen threshold keep-parent head tail reservoir canopy; do
    for kind in agent central; do
        f=$STAGE3/tmp/$arm-$kind.yaml
        python3 "$MOD/tools/campaign/render.py" --arm "$arm" --volume 0.12 --image x --print "$kind" ${NOCAP:+--no-capgate} > "$f"
        sed -i -e "s#/etc/slobudget/callgraph.json#$MOD/deploy/callgraph.json#" -e "s#/etc/slobudget/ttable.json#$STAGE3/reservoir-ttable.json#" "$f"
        if "$BIN" validate --config "$f" > "$STAGE3/tmp/$arm-$kind.validate" 2>&1; then
            echo "  $arm $kind ok"
        else
            echo "  $arm $kind INVALID: $(tail -2 "$STAGE3/tmp/$arm-$kind.validate" | tr '\n' ' ' | cut -c1-300)"; ok=0
        fi
    done
done
python3 "$MOD/tools/campaign/render.py" --arm chosen --volume 0.12 --image x --print tee > "$STAGE3/tmp/tee.yaml"
"$BIN" validate --config "$STAGE3/tmp/tee.yaml" > /dev/null 2>&1 && echo "  tee ok" || { echo "  tee INVALID"; ok=0; }
sed "s#__PATH__#$STAGE3/tmp/shadow-validate.jsonl#" "$MOD/tools/campaign/shadow-config.yaml" > "$STAGE3/tmp/shadow.yaml"
"$BIN" validate --config "$STAGE3/tmp/shadow.yaml" > /dev/null 2>&1 && echo "  shadow ok" || { echo "  shadow INVALID"; ok=0; }
[ $ok = 1 ] && echo "all valid" || echo "some configs invalid"
