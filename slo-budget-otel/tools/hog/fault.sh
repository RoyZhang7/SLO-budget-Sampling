#!/usr/bin/env bash
# CPU fault on DSB-SN service containers (node-0; needs ssh to the nodes and docker there).
#
#   tools/hog/fault.sh spin  <service|all> <threads> <seconds> [duty]       spinning threads inside each container
#   tools/hog/fault.sh spinf <service|all> <threads> <seconds> <f>          as spin, plus one thread spinning a fraction f
#                                                                            of every 10 ms (no CPU limit on the container)
#   tools/hog/fault.sh quota <service|all> <quota_us> <seconds> [period_us]  CPU quota on each container (docker update)
#   tools/hog/fault.sh burn  <service|all> <threads> <seconds> <quota_us>/<period_us>
#                                                                            spinning threads inside each container under a
#                                                                            CPU limit on it (the fault stays in the container)
#   tools/hog/fault.sh clear <service|all>                                   remove quotas now
#
# <service> is the short name of a Deployment or DaemonSet `<service>-service-pb-es-ctr`
# (socialgraph, post-storage, wrk2api, ...); `all` = every *-service-pb-es-ctr pod except
# tracepressure.  Every pod of the service is targeted (wrk2api runs one per node), all in
# parallel.  The spinner (tools/hog/hog, static) is copied into the container with docker cp
# and runs in the container's cgroup until it exits by itself; a quota is lifted after
# <seconds> by a background `docker update --cpu-quota=-1`.  `burn` is the two together: the
# quota is set first, so the spinner and the service share at most quota/period of a CPU and
# the spinner cannot take CPU from the other containers on the node (Stage 3 confined fault;
# `spin` alone spills onto the node).  Container restarts (reseed) clear both.  Prints one
# line per container: <node> <pod> <mode> <level>.
set -u
SUFFIX=${SUFFIX:-pb-es-ctr}
HOG_BIN=${HOG_BIN:-$(cd "$(dirname "$0")" && pwd)/hog}
MODE=$1; TARGET=$2; LEVEL=${3:-}; SECS=${4:-0}; EXTRA=${5:-}
case "$MODE" in spin|spinf|quota|burn|clear) ;; *) echo "usage: fault.sh spin|spinf|quota|burn|clear <service|all> ..."; exit 2 ;; esac
if [ "$MODE" = burn ]; then
    case "$EXTRA" in */*) ;; *) echo "burn needs <quota_us>/<period_us>"; exit 2 ;; esac
fi
if [ "$TARGET" = all ]; then
    PODS=$(kubectl get pods --no-headers -o custom-columns=N:.metadata.name | grep -- "-service-$SUFFIX-" | grep -v tracepressure)
else
    PODS=$(kubectl get pods --no-headers -o custom-columns=N:.metadata.name -l "io.kompose.service=$TARGET-service-$SUFFIX")
fi
[ -n "$PODS" ] || { echo "no pods for $TARGET"; exit 1; }
# pod -> node and container id, once
declare -A NODE CID
for pod in $PODS; do
    NODE[$pod]=$(kubectl get pod "$pod" -o jsonpath='{.spec.nodeName}')
    CID[$pod]=$(kubectl get pod "$pod" -o jsonpath='{.status.containerStatuses[0].containerID}' | sed 's#^docker://##')
done
if [ "$MODE" = spin ] || [ "$MODE" = spinf ] || [ "$MODE" = burn ]; then   # the hog binary to each node once
    for node in $(printf '%s\n' "${NODE[@]}" | sort -u); do
        scp -q -o BatchMode=yes "$HOG_BIN" "$node:/tmp/slobudget-hog" || { echo "copy to $node failed"; exit 1; }
    done
fi
one() {
    local pod=$1 node=${NODE[$1]} cid=${CID[$1]}
    [ -n "$cid" ] || { echo "no container id for $pod"; return; }
    case "$MODE" in
    spin)
        local duty=${EXTRA:-1.0}
        ssh -o BatchMode=yes "$node" "docker cp /tmp/slobudget-hog $cid:/slobudget-hog && docker exec -d $cid /slobudget-hog -threads $LEVEL -duty $duty -dur ${SECS}s" \
            || { echo "spin failed on $pod"; return; }
        echo "$node $pod spin threads=$LEVEL duty=$duty dur=${SECS}s"
        ;;
    spinf)
        ssh -o BatchMode=yes "$node" "docker cp /tmp/slobudget-hog $cid:/slobudget-hog && docker exec -d $cid /slobudget-hog -threads $LEVEL -extra-duty $EXTRA -extra-period 10ms -dur ${SECS}s" \
            || { echo "spinf failed on $pod"; return; }
        echo "$node $pod spinf threads=$LEVEL extra=$EXTRA/10ms dur=${SECS}s"
        ;;
    burn)
        local quota=${EXTRA%/*} period=${EXTRA#*/}
        ssh -o BatchMode=yes "$node" "docker update --cpu-period=$period --cpu-quota=$quota $cid > /dev/null && docker cp /tmp/slobudget-hog $cid:/slobudget-hog && docker exec -d $cid /slobudget-hog -threads $LEVEL -dur ${SECS}s" \
            || { echo "burn failed on $pod"; return; }
        ( sleep "$SECS"; ssh -o BatchMode=yes "$node" "docker update --cpu-quota=-1 $cid > /dev/null" ) > /dev/null 2>&1 &
        echo "$node $pod burn threads=$LEVEL quota=${quota}us/${period}us dur=${SECS}s"
        ;;
    quota)
        local period=${EXTRA:-10000}
        ssh -o BatchMode=yes "$node" "docker update --cpu-period=$period --cpu-quota=$LEVEL $cid > /dev/null" \
            || { echo "quota failed on $pod"; return; }
        ( sleep "$SECS"; ssh -o BatchMode=yes "$node" "docker update --cpu-quota=-1 $cid > /dev/null" ) > /dev/null 2>&1 &
        echo "$node $pod quota=${LEVEL}us/${period}us dur=${SECS}s"
        ;;
    clear)
        ssh -o BatchMode=yes "$node" "docker update --cpu-quota=-1 $cid > /dev/null" && echo "$node $pod cleared"
        ;;
    esac
}
for pod in $PODS; do one "$pod" & done
wait
