package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget/recon"
)

// testdata/tiny.json: trace 1 = the shared package's one-tree case (usertimeline server at
// depth 4 with anchor 0x20, hometimeline client at depth 5); trace 2 = a kept root and a
// composepost server (depth 2, anchor = root) whose client parent 0x11 is missing.
func TestRunTiny(t *testing.T) {
	g, err := recon.LoadCallGraph("../../deploy/callgraph.json")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out.json")
	rep, err := run("testdata/tiny.json", out, g, options{defaultRoute: "compose_post", serviceFormat: "unknown_service:%s_service_pb_es_proc", synthesize: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Traces != 2 || rep.SpansReal != 4 || rep.Orphans != 3 {
		t.Fatalf("traces %d spans %d orphans %d", rep.Traces, rep.SpansReal, rep.Orphans)
	}
	// trace 1: 6 placeholders (two open chains by known ids); trace 2: one placeholder (0x11),
	// placed to the kept root with hole 1
	if rep.Placeholders != 7 || rep.PlacedToKept != 1 || rep.HoleOne != 1 || rep.OpenByKnownIDs != 2 {
		t.Fatalf("placeholders %d kept %d hole1 %d open %d hist %v", rep.Placeholders, rep.PlacedToKept, rep.HoleOne, rep.OpenByKnownIDs, rep.HoleHistKnownIDs)
	}
	if rep.OneTreeBefore != 0 || rep.OneTreeAfter != 2 || rep.OneTreeAfterKnownIDs != 1 {
		t.Fatalf("one tree before %d after %d known-ids %d", rep.OneTreeBefore, rep.OneTreeAfter, rep.OneTreeAfterKnownIDs)
	}
	if rep.Bridge.ByKind["server"]["br"] != 3 || rep.Bridge.ByKind["client"]["d"] != 1 || rep.Bridge.BRLengths["12"] != 3 || rep.Bridge.RootBRZero != 1 {
		t.Fatalf("bridge stats %+v", rep.Bridge)
	}
	if rep.Bridge.AnchorChecks["anchor == parent"] != 0 {
		t.Fatalf("anchor checks %v", rep.Bridge.AnchorChecks)
	}
	// the written export: same shape, placeholders appended with the synthetic tags and a
	// process for every placeholder service
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]any
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	if top["total"] == nil || top["limit"] == nil {
		t.Fatal("top-level keys lost")
	}
	traces := top["data"].([]any)
	tr := traces[0].(map[string]any)
	spans := tr["spans"].([]any)
	if len(spans) != 8 {
		t.Fatalf("trace 1 spans %d", len(spans))
	}
	synth := 0
	ids := map[string]bool{}
	for _, s := range spans {
		sm := s.(map[string]any)
		ids[sm["spanID"].(string)] = true
	}
	procs := tr["processes"].(map[string]any)
	for _, s := range spans {
		sm := s.(map[string]any)
		for _, tg := range sm["tags"].([]any) {
			tm := tg.(map[string]any)
			if tm["key"] == "synthetic" && tm["value"] == true {
				synth++
				if _, ok := procs[sm["processID"].(string)]; !ok {
					t.Fatalf("placeholder without process: %v", sm)
				}
				if op := sm["operationName"].(string); op[len(op)-len("[synthetic]"):] != "[synthetic]" {
					t.Fatalf("operation %q", op)
				}
			}
		}
		for _, ref := range sm["references"].([]any) {
			if pid := ref.(map[string]any)["spanID"].(string); !ids[pid] {
				t.Fatalf("parent %s missing after reconstruction", pid)
			}
		}
	}
	if synth != 6 {
		t.Fatalf("synthetic spans in trace 1: %d", synth)
	}
	// without synthesis the open ends are marked
	rep2, err := run("testdata/tiny.json", filepath.Join(t.TempDir(), "o2.json"), g, options{defaultRoute: "compose_post", serviceFormat: "%s", synthesize: false})
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Placeholders >= rep.Placeholders || rep2.SyntheticIDs != 0 {
		t.Fatalf("known-ids output: placeholders %d synthetic %d", rep2.Placeholders, rep2.SyntheticIDs)
	}
	_ = sortedKeys(rep.HoleHistFull)
}

// Trace 1: a kept root, a composepost server (depth 2, its client parent missing, anchor = root), a text server (depth 4, its
// client parent missing, anchor = the composepost server) and a client span under the composepost server: all reach the root.
// Trace 2 (no root): a text server whose anchor is missing (open), a client span of that text server (parent kept, still
// open above) and a depth-1 client span (placed at the root slot).
func TestCoverageByReason(t *testing.T) {
	enc := func(depth byte, anchor uint64) string {
		b := append([]byte{depth}, make([]byte, 11)...)
		binary.BigEndian.PutUint64(b[1:9], anchor)
		return base64.StdEncoding.EncodeToString(b)
	}
	span := func(tid string, id, parent uint64, kind, svc, br, d, reason string) map[string]any {
		refs := []any{}
		if parent != 0 {
			refs = append(refs, map[string]any{"refType": "CHILD_OF", "traceID": tid, "spanID": fmt.Sprintf("%016x", parent)})
		}
		tags := []any{tag("span.kind", "string", kind), tag("slobudget.reason", "string", reason)}
		if br != "" {
			tags = append(tags, tag("_br", "binary", br))
		}
		if d != "" {
			tags = append(tags, tag("_d", "binary", d))
		}
		return map[string]any{"traceID": tid, "spanID": fmt.Sprintf("%016x", id), "operationName": svc + "Server_X", "references": refs,
			"startTime": 1000, "duration": 100, "tags": tags, "processID": "p_" + svc}
	}
	d1 := func(n byte) string { return base64.StdEncoding.EncodeToString([]byte{n}) }
	procs := map[string]any{}
	for _, svc := range []string{"wrk2api", "composepost", "text"} {
		procs["p_"+svc] = map[string]any{"serviceName": "unknown_service:" + svc + "_service_pb_es_proc", "tags": []any{}}
	}
	t1, t2 := fmt.Sprintf("%032x", 1), fmt.Sprintf("%032x", 2)
	traces := []any{
		map[string]any{"traceID": t1, "processes": procs, "spans": []any{
			span(t1, 0x10, 0, "server", "wrk2api", enc(0, 0), "", "root"),
			span(t1, 0x12, 0x11, "server", "composepost", enc(2, 0x10), "", "outlier"),
			span(t1, 0x22, 0x21, "server", "text", enc(4, 0x12), "", "outlier"),
			span(t1, 0x31, 0x12, "client", "composepost", "", d1(3), "plan"),
		}},
		map[string]any{"traceID": t2, "processes": procs, "spans": []any{
			span(t2, 0x52, 0x51, "server", "text", enc(4, 0x99), "", "outlier"),
			span(t2, 0x61, 0x52, "client", "text", "", d1(5), "plan"),
			span(t2, 0x71, 0x70, "client", "wrk2api", "", d1(1), "plan"),
		}},
	}
	raw, err := json.Marshal(map[string]any{"data": traces})
	if err != nil {
		t.Fatal(err)
	}
	in := filepath.Join(t.TempDir(), "in.json")
	if err := os.WriteFile(in, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	g, err := recon.LoadCallGraph("../../deploy/callgraph.json")
	if err != nil {
		t.Fatal(err)
	}
	rep, err := run(in, "", g, options{defaultRoute: "compose_post", serviceFormat: "%s", synthesize: true})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]reasonCoverage{
		"root/server":    {Spans: 1, TraceRootKept: 1, ReachesRoot: 1},
		"outlier/server": {Spans: 3, TraceRootKept: 2, Orphans: 3, PlacedToKept: 2, Open: 1, ReachesRoot: 2},
		"plan/client":    {Spans: 3, TraceRootKept: 1, ParentKept: 2, Orphans: 1, PlacedToRoot: 1, ReachesRoot: 2},
	}
	if len(rep.ByReason) != len(want) {
		t.Fatalf("reasons %v", rep.ByReason)
	}
	for k, w := range want {
		if got := rep.ByReason[k]; got == nil || *got != w {
			t.Errorf("%s: %+v, want %+v", k, got, w)
		}
	}
}
