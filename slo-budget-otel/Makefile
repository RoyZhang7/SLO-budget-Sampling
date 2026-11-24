# SLO-budget collector: developer and node-0 targets.  The cluster targets run on node-0
# (see README, "Runbook: the cluster").
.PHONY: test vet build image reseed reseed-check

test:            ## unit tests of the module, the W3C issuer and the arrival program
	go test ./... && (cd sdk && go test ./...) && cd arrivald && go test ./...

vet:
	go vet ./...

image:           ## node-0: tests, benchmark, ocb build, image push (build/IMAGE_TAG)
	tools/build-node0.sh

reseed:          ## node-0: restore the social graph to the profile's seeded state (empties the stores, replays the seed)
	bash tools/reseed.sh

reseed-check:    ## node-0: 5-min fault-free run at 12 % after a reseed; prints the healthy outlier share
	DUR=300 HOG_FOR=0 ARM=reseed-check tools/run_cluster.sh > results/run-reseed-check.log 2>&1
	R=$$(ls -td results/cluster-*-reseed-check | head -1) && \
	python3 tools/cluster_metrics.py $$R/export.json $$R/central-status.json $$R/times.json --out $$R/metrics.json > /dev/null && \
	python3 -c "import json,sys; m=json.load(open(sys.argv[1])); print('healthy outlier share', round(m['outlier_share_nonroot'],4), 'vol/V*', round(m['vol_ratio_mean'],3), 'B', round(m['B_mean'],2), 'roots', m['roots_kept'])" $$R/metrics.json
