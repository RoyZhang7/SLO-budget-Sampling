#!/usr/bin/env python3
"""Apply the SLO-budget issuer to the Blueprint fork checkout on node-0 (idempotent).

1. runtime/plugins/otelcol/slobudget_processor.go  (new file, copied beside this script)
2. runtime/plugins/otelcol/trace.go: register the issuer as a second span processor
3. plugins/http/httpcodegen/servergen.go: the HTTP handler puts the request size on the context
The bridge processors and their templates are not touched.
"""
import pathlib, shutil, sys

root = pathlib.Path.home() / "blueprint-docc-mod"
here = pathlib.Path(__file__).resolve().parent

dst = root / "runtime/plugins/otelcol/slobudget_processor.go"
src = (here / "slobudget_processor.go").read_text()
# drop the `//go:build ignore` header that keeps the file out of the sdk module's own build
if src.startswith("//go:build ignore"):
    src = src.split("

", 2)[2]
dst.write_text(src)
print("copied", dst)


def patch(rel, old, new):
    p = root / rel
    s = p.read_text()
    if new in s:
        print("already patched", rel)
        return
    assert s.count(old) == 1, (rel, s.count(old))
    p.write_text(s.replace(old, new))
    print("patched", rel)


patch("runtime/plugins/otelcol/trace.go",
      """	spanProcessor = wrapReverseCheckpointProcessor(spanProcessor)
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSampler(headSampler()),
		tracesdk.WithSpanProcessor(spanProcessor),
	)
""",
      """	spanProcessor = wrapReverseCheckpointProcessor(spanProcessor)
	tpOpts := []tracesdk.TracerProviderOption{tracesdk.WithSampler(headSampler())}
	// SLO-budget sampling: the budget issuer / baggage copier runs beside the bridge
	// processor (slobudget_processor.go); nil when SLO_BUDGET_PORT=off.
	if slo := NewSLOBudgetProcessor(addr); slo != nil {
		tpOpts = append(tpOpts, tracesdk.WithSpanProcessor(slo))
	}
	tpOpts = append(tpOpts, tracesdk.WithSpanProcessor(spanProcessor))
	tp := tracesdk.NewTracerProvider(tpOpts...)
""")

patch("plugins/http/httpcodegen/servergen.go",
      """	ctx := context.Background()
	{{RetVars $f "err"}} {{HasNewReturnVars $f}} handler.Service.{{$f.Name}}({{ArgVars $f "ctx"}})""",
      """	// SLO-budget sampling: the request's size on the wire (query + body bytes), read by the
	// SDK's budget issuer at the root span's start for the payload-size class.
	ctx := context.WithValue(context.Background(), "requestBytes", int64(len(r.URL.RawQuery))+max(r.ContentLength, 0))
	{{RetVars $f "err"}} {{HasNewReturnVars $f}} handler.Service.{{$f.Name}}({{ArgVars $f "ctx"}})""")
print("done")
