package main

import (
	"encoding/json"
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
