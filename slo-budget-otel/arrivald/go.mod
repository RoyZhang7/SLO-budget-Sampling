module github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrivald

go 1.24.0

// The loader of the arrival program is a module of its own, so that the collector's module
// graph (and its ocb build) stays as it is.
require (
	github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel v0.0.0
	github.com/cilium/ebpf v0.19.0
	github.com/vishvananda/netlink v1.3.1
	github.com/vishvananda/netns v0.0.5
	golang.org/x/sys v0.36.0
)

replace github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel => ../
