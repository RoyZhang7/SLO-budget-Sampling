// Package slobudgetreconprocessor is the collector-side form of the trace reconstruction
// (slobudget/recon): it buffers a trace's kept spans for a hold and forwards them with the
// placeholder ancestors added.  Since Stage 2c reconstruction is offline (tools/recon) and
// this processor is excluded from the default build: its files carry the build tag
// slobudget_recon.  To build it in, uncomment its entry in builder/manifest.yaml and build the
// collector with `-tags slobudget_recon` (ocb: `go build -tags slobudget_recon` in the
// generated module, or set GOFLAGS=-tags=slobudget_recon for tools/build-node0.sh); its tests
// run with `go test -tags slobudget_recon ./processor/slobudgetreconprocessor/`.
package slobudgetreconprocessor
