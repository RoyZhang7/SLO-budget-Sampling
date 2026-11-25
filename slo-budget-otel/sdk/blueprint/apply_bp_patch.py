#!/usr/bin/env python3
"""Apply the SLO-budget issuer and the in-service filter to the Blueprint fork checkout on node-0 (idempotent).

1. runtime/plugins/otelcol/slobudget_processor.go  (new file, copied beside this script)
2. runtime/plugins/otelcol/trace.go: register the issuer as a second span processor
3. plugins/http/httpcodegen/servergen.go: the HTTP handler puts the request size on the context
4. the in-service filter (off unless SLO_FILTER=on): runtime/plugins/otelcol/slofilter (../filter), .../sloarrival (../../arrival,
   the mapped table and the lookup), slo_filter_processor.go (the adapter), and one hook in the Path Bridge's OnEnd
   (pb_processor.go); the arrival package needs golang.org/x/sys in the runtime module (`go mod tidy` after the first apply)
The bridge processors and their templates are otherwise not touched.
"""
import pathlib, re, shutil, sys

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


def copy_package(src_dir, dst_rel, package):
    """every non-test .go file of src_dir into the fork as package `package`"""
    dst = root / dst_rel
    dst.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in sorted(pathlib.Path(src_dir).glob("*.go")):
        if f.name.endswith("_test.go"):
            continue
        s = f.read_text()
        s = re.sub(r"^package \w+", "package " + package, s, count=1, flags=re.M)
        (dst / f.name).write_text(s)
        n += 1
    print("copied", n, "files into", dst_rel)


copy_package(here / "../filter", "runtime/plugins/otelcol/slofilter", "slofilter")
copy_package(here / "../../arrival", "runtime/plugins/otelcol/sloarrival", "sloarrival")
dst = root / "runtime/plugins/otelcol/slo_filter_processor.go"
src = (here / "slo_filter_processor.go").read_text()
if src.startswith("//go:build ignore"):
    src = src.split("\n\n", 2)[2]
dst.write_text(src)
print("copied", dst)
patch("runtime/plugins/otelcol/pb_processor.go",
      """	attrs := s.Attributes()
	w := bridgeWires.take(s.SpanContext().SpanID())
	p.routeToPipeline(s, attrs, w, pathCheckpoint(s.SpanKind(), attrs, w))
""",
      """	attrs := s.Attributes()
	w := bridgeWires.take(s.SpanContext().SpanID())
	// SLO-budget sampling, the in-service filter (slo_filter_processor.go): a span it drops is never serialised
	if extra, export := sloFilterOnEnd(s); !export {
		return
	} else if len(extra) > 0 {
		attrs = append(append(make([]attribute.KeyValue, 0, len(attrs)+len(extra)), attrs...), extra...)
	}
	p.routeToPipeline(s, attrs, w, pathCheckpoint(s.SpanKind(), attrs, w))
""")

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
