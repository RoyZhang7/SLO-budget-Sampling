// Command recon reconstructs the shed structure of the traces in a Jaeger JSON export
// (what tools/jaeger_export.py writes: {"data": [traces]}) and writes an export of the same
// shape with placeholder ancestors added, plus a coverage report.  See README.md.
//
//	go run ./tools/recon -in export.json -out export.recon.json -report recon-offline.json
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget/recon"
)

type options struct {
	callGraph     string
	defaultRoute  string
	serviceFormat string
	synthesize    bool
}

// report is the coverage report written with -report.
type report struct {
	Traces                  int            `json:"traces"`
	SpansReal               int            `json:"spans_real"`
	SpansSyntheticInput     int            `json:"spans_synthetic_in_input"`
	Orphans                 int            `json:"orphans"` // kept spans whose parent is missing
	OrphanFracOfKept        float64        `json:"orphans_frac_of_kept"`
	PlaceableByKnownIDs     int            `json:"placeable_by_known_ids"`
	PlaceableFracOfOrphans  float64        `json:"placeable_frac_of_orphans"`
	PlaceableFracOfKept     float64        `json:"placeable_frac_of_kept"`
	PlacedToKept            int            `json:"placed_to_kept_ancestor"`
	PlacedToRoot            int            `json:"placed_to_root_slot"`
	OpenByKnownIDs          int            `json:"open_by_known_ids"`
	HoleOne                 int            `json:"placed_with_hole_1"`
	HoleOneFracOfOrphans    float64        `json:"hole_1_frac_of_orphans"`
	HoleOneFracOfPlaceable  float64        `json:"hole_1_frac_of_placeable"`
	HoleHistKnownIDs        map[string]int `json:"hole_hist_known_ids"`        // class:hole -> count
	HoleHistFull            map[string]int `json:"hole_hist_full"`             // placeholders between the orphan and its nearest real ancestor / root
	OrphansByKind           map[string]int `json:"orphans_by_kind"`
	PlaceableByKind         map[string]int `json:"placeable_by_kind"`
	Placeholders            int            `json:"placeholders"`
	PlaceholdersPerRealSpan float64        `json:"placeholders_per_real_span"`
	PlaceholdersPerTrace    float64        `json:"placeholders_per_trace"`
	RootPlaceholders        int            `json:"root_placeholders"`
	SyntheticIDs            int            `json:"synthetic_ids"`
	SyntheticIDShare        float64        `json:"synthetic_id_share"`
	OneTreeBefore           int            `json:"traces_one_tree_before"`
	OneTreeAfter            int            `json:"traces_one_tree_after"`
	OneTreeAfterKnownIDs    int            `json:"traces_one_tree_after_known_ids_only"`
	OneTreeBeforeFrac       float64        `json:"traces_one_tree_before_frac"`
	OneTreeAfterFrac        float64        `json:"traces_one_tree_after_frac"`
	OneTreeKnownIDsFrac     float64        `json:"traces_one_tree_after_known_ids_only_frac"`
	TracesWithRealRoot      int            `json:"traces_with_real_root"`
	Bridge                  bridgeStats    `json:"bridge_fields"`
	SynthesizedIDsInOutput  bool           `json:"output_synthesizes_ids"`
}

// bridgeStats describes what `_br` / `_d` carry in the export.
type bridgeStats struct {
	ByKind                 map[string]map[string]int `json:"by_kind"` // kind -> {br, d, neither, both}
	BRLengths              map[string]int            `json:"br_byte_lengths"`
	DLengths               map[string]int            `json:"d_byte_lengths"`
	DepthByKind            map[string]map[string]int `json:"depth_by_kind"`
	BRDepthParity          map[string]int            `json:"br_depth_parity"` // even / odd depths among _br carriers
	AnchorChecks           map[string]int            `json:"anchor_checks"`   // grandparent kept: anchor == grandparent / mismatch; anchor == parent
	RootBRZero             int                       `json:"root_br_all_zero"`
}

type traceAcc struct {
	spans    []recon.Span
	route    string
	kept     map[recon.SpanID]recon.Span
	synthIn  int
}

func main() {
	in := flag.String("in", "", "Jaeger JSON export to read")
	out := flag.String("out", "", "reconstructed export to write (optional)")
	rep := flag.String("report", "", "coverage report JSON to write (optional; also printed)")
	cg := flag.String("callgraph", "deploy/callgraph.json", "call graph file (route -> {root, children})")
	route := flag.String("route", "compose_post", "route of traces whose spans carry no slo.root")
	svcFmt := flag.String("service-format", "unknown_service:%s_service_pb_es_proc", "resource service.name of a placeholder service not seen in the export (%s = short name)")
	noSynth := flag.Bool("no-synth", false, "do not synthesise ids: chains stop where nobody kept the id (open ends)")
	flag.Parse()
	if *in == "" {
		fmt.Fprintln(os.Stderr, "-in is required")
		os.Exit(2)
	}
	g, err := recon.LoadCallGraph(*cg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "callgraph:", err)
		os.Exit(1)
	}
	r, err := run(*in, *out, g, options{callGraph: *cg, defaultRoute: *route, serviceFormat: *svcFmt, synthesize: !*noSynth})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	text, _ := json.MarshalIndent(r, "", " ")
	fmt.Println(string(text))
	if *rep != "" {
		if err := os.WriteFile(*rep, text, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

// run reads the export, reconstructs every trace and writes the result and the report.
func run(inPath, outPath string, g *recon.CallGraph, opt options) (*report, error) {
	raw, err := os.ReadFile(inPath)
	if err != nil {
		return nil, err
	}
	raw = bytes.TrimPrefix(raw, []byte{0xEF, 0xBB, 0xBF}) // a BOM from a Windows writer
	var top map[string]json.RawMessage
	var traces []map[string]any
	if len(raw) > 0 && raw[0] == '[' {
		if err := json.Unmarshal(raw, &traces); err != nil {
			return nil, err
		}
	} else {
		if err := json.Unmarshal(raw, &top); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(top["data"], &traces); err != nil {
			return nil, fmt.Errorf("data: %w", err)
		}
	}
	rep := &report{HoleHistKnownIDs: map[string]int{}, HoleHistFull: map[string]int{}, OrphansByKind: map[string]int{}, PlaceableByKind: map[string]int{},
		Bridge: bridgeStats{ByKind: map[string]map[string]int{}, BRLengths: map[string]int{}, DLengths: map[string]int{}, DepthByKind: map[string]map[string]int{},
			BRDepthParity: map[string]int{}, AnchorChecks: map[string]int{}}, SynthesizedIDsInOutput: opt.synthesize}
	resNames := map[string]string{} // short -> resource service.name seen anywhere
	for _, tr := range traces {
		for _, p := range asMap(tr["processes"]) {
			if name, _ := asMap(p)["serviceName"].(string); name != "" {
				resNames[slobudget.NormalizeService(name)] = name
			}
		}
	}
	for _, tr := range traces {
		tid, err := parseTraceID(asString(tr["traceID"]))
		if err != nil {
			return nil, err
		}
		acc := parseTrace(tr, opt.defaultRoute, rep)
		if len(acc.spans) == 0 {
			continue
		}
		rep.Traces++
		if recon.OneTree(acc.spans, nil) {
			rep.OneTreeBefore++
		}
		for _, s := range acc.spans {
			if s.Parent.IsZero() {
				rep.TracesWithRealRoot++
				break
			}
		}
		full := recon.Reconstruct(tid, acc.route, acc.spans, g, recon.Options{SynthesizeIDs: true})
		known := recon.Reconstruct(tid, acc.route, acc.spans, g, recon.Options{SynthesizeIDs: false})
		if recon.OneTree(acc.spans, full.Placeholders) {
			rep.OneTreeAfter++
		}
		if recon.OneTree(acc.spans, known.Placeholders) {
			rep.OneTreeAfterKnownIDs++
		}
		for _, o := range full.Orphans {
			rep.Orphans++
			rep.OrphansByKind[o.Kind]++
			rep.HoleHistKnownIDs[fmt.Sprintf("%s:%d", o.Class, o.Hole)]++
			rep.HoleHistFull[fmt.Sprintf("%d", o.HoleFull)]++
			if o.Class == "kept" || o.Class == "root" {
				rep.PlaceableByKnownIDs++
				rep.PlaceableByKind[o.Kind]++
				if o.Hole == 1 {
					rep.HoleOne++
				}
			}
			switch o.Class {
			case "kept":
				rep.PlacedToKept++
			case "root":
				rep.PlacedToRoot++
			default:
				rep.OpenByKnownIDs++
			}
		}
		chosen := full
		if !opt.synthesize {
			chosen = known
		}
		rep.Placeholders += len(chosen.Placeholders)
		for _, p := range chosen.Placeholders {
			if p.Depth == 0 {
				rep.RootPlaceholders++
			}
			if !p.RealID {
				rep.SyntheticIDs++
			}
		}
		if outPath != "" {
			appendPlaceholders(tr, tid, chosen.Placeholders, resNames, opt.serviceFormat)
		}
	}
	if rep.SpansReal > 0 {
		rep.OrphanFracOfKept = ratio(rep.Orphans, rep.SpansReal)
		rep.PlaceableFracOfKept = ratio(rep.PlaceableByKnownIDs, rep.SpansReal)
		rep.PlaceholdersPerRealSpan = ratio(rep.Placeholders, rep.SpansReal)
	}
	if rep.Orphans > 0 {
		rep.PlaceableFracOfOrphans = ratio(rep.PlaceableByKnownIDs, rep.Orphans)
		rep.HoleOneFracOfOrphans = ratio(rep.HoleOne, rep.Orphans)
	}
	if rep.PlaceableByKnownIDs > 0 {
		rep.HoleOneFracOfPlaceable = ratio(rep.HoleOne, rep.PlaceableByKnownIDs)
	}
	if rep.Placeholders > 0 {
		rep.SyntheticIDShare = ratio(rep.SyntheticIDs, rep.Placeholders)
	}
	if rep.Traces > 0 {
		rep.PlaceholdersPerTrace = ratio(rep.Placeholders, rep.Traces)
		rep.OneTreeBeforeFrac = ratio(rep.OneTreeBefore, rep.Traces)
		rep.OneTreeAfterFrac = ratio(rep.OneTreeAfter, rep.Traces)
		rep.OneTreeKnownIDsFrac = ratio(rep.OneTreeAfterKnownIDs, rep.Traces)
	}
	if outPath != "" {
		var outRaw []byte
		if top != nil {
			data, err := json.Marshal(traces)
			if err != nil {
				return nil, err
			}
			top["data"] = data
			outRaw, err = json.Marshal(top)
			if err != nil {
				return nil, err
			}
		} else {
			outRaw, err = json.Marshal(traces)
			if err != nil {
				return nil, err
			}
		}
		if err := os.WriteFile(outPath, outRaw, 0o644); err != nil {
			return nil, err
		}
	}
	return rep, nil
}

func ratio(a, b int) float64 { return float64(int(10000*float64(a)/float64(b)+0.5)) / 10000 }

// parseTrace converts one Jaeger trace into the reconstruction's spans and feeds the bridge
// statistics.
func parseTrace(tr map[string]any, defaultRoute string, rep *report) traceAcc {
	acc := traceAcc{route: defaultRoute, kept: map[recon.SpanID]recon.Span{}}
	procs := asMap(tr["processes"])
	spans, _ := tr["spans"].([]any)
	type raw struct {
		span   recon.Span
		depth  int
		anchor recon.SpanID
		hasBR  bool
		hasD   bool
	}
	var raws []raw
	for _, x := range spans {
		sm := asMap(x)
		s := recon.Span{Kind: recon.KindServer}
		s.ID, _ = parseSpanID(asString(sm["spanID"]))
		for _, ref := range asList(sm["references"]) {
			rm := asMap(ref)
			if asString(rm["refType"]) == "CHILD_OF" {
				s.Parent, _ = parseSpanID(asString(rm["spanID"]))
			}
		}
		s.Operation = asString(sm["operationName"])
		start := asInt(sm["startTime"])
		s.Start = uint64(start) * 1000
		s.End = uint64(start+asInt(sm["duration"])) * 1000
		if p := asMap(procs[asString(sm["processID"])]); p != nil {
			s.Service = slobudget.NormalizeService(asString(p["serviceName"]))
		}
		r := raw{}
		for _, t := range asList(sm["tags"]) {
			tm := asMap(t)
			switch asString(tm["key"]) {
			case "span.kind":
				if asString(tm["value"]) == "client" {
					s.Kind = recon.KindClient
				}
			case "_br":
				s.BR = binaryTag(tm)
				r.hasBR = true
			case "_d":
				s.D = binaryTag(tm)
				r.hasD = true
			case "synthetic":
				if b, ok := tm["value"].(bool); ok && b {
					s.Synthetic = true
				}
			case "slo.root":
				if v := asString(tm["value"]); v != "" {
					acc.route = v
				}
			}
		}
		if s.Synthetic {
			acc.synthIn++
			rep.SpansSyntheticInput++
			continue
		}
		r.depth, r.anchor = recon.DecodeBridge(s.BR, s.D)
		r.span = s
		raws = append(raws, r)
		acc.spans = append(acc.spans, s)
		acc.kept[s.ID] = s
		rep.SpansReal++
	}
	// bridge field statistics
	for _, r := range raws {
		s := r.span
		bk := rep.Bridge.ByKind[s.Kind]
		if bk == nil {
			bk = map[string]int{}
			rep.Bridge.ByKind[s.Kind] = bk
		}
		switch {
		case r.hasBR && r.hasD:
			bk["both"]++
		case r.hasBR:
			bk["br"]++
		case r.hasD:
			bk["d"]++
		default:
			bk["neither"]++
		}
		if r.hasBR {
			rep.Bridge.BRLengths[fmt.Sprint(len(s.BR))]++
			if r.depth >= 0 {
				if r.depth%2 == 0 {
					rep.Bridge.BRDepthParity["even"]++
				} else {
					rep.Bridge.BRDepthParity["odd"]++
				}
			}
			if s.Parent.IsZero() && allZero(s.BR) {
				rep.Bridge.RootBRZero++
			}
		}
		if r.hasD {
			rep.Bridge.DLengths[fmt.Sprint(len(s.D))]++
		}
		dk := rep.Bridge.DepthByKind[s.Kind]
		if dk == nil {
			dk = map[string]int{}
			rep.Bridge.DepthByKind[s.Kind] = dk
		}
		dk[fmt.Sprint(r.depth)]++
		if r.hasBR && !r.anchor.IsZero() {
			if r.anchor == s.Parent {
				rep.Bridge.AnchorChecks["anchor == parent"]++
			}
			if parent, ok := acc.kept[s.Parent]; ok && !parent.Parent.IsZero() {
				if _, gok := acc.kept[parent.Parent]; gok {
					if r.anchor == parent.Parent {
						rep.Bridge.AnchorChecks["grandparent kept: anchor == grandparent"]++
					} else {
						rep.Bridge.AnchorChecks["grandparent kept: anchor != grandparent"]++
					}
				}
			}
		}
	}
	return acc
}

// appendPlaceholders adds the placeholders to the trace's span list in Jaeger's JSON shape.
func appendPlaceholders(tr map[string]any, tid recon.TraceID, ph []recon.Placeholder, resNames map[string]string, format string) {
	if len(ph) == 0 {
		return
	}
	procs := asMap(tr["processes"])
	if procs == nil {
		procs = map[string]any{}
		tr["processes"] = procs
	}
	pidOf := map[string]string{} // short service -> processID
	for pid, p := range procs {
		if name := asString(asMap(p)["serviceName"]); name != "" {
			pidOf[slobudget.NormalizeService(name)] = pid
		}
	}
	spans, _ := tr["spans"].([]any)
	tidHex := hex.EncodeToString(tid[:])
	for _, p := range ph {
		pid, ok := pidOf[p.Service]
		if !ok {
			name := resNames[p.Service]
			if name == "" {
				name = fmt.Sprintf(format, p.Service)
			}
			pid = fmt.Sprintf("p%d", len(procs)+1)
			for procs[pid] != nil {
				pid += "x"
			}
			procs[pid] = map[string]any{"serviceName": name, "tags": []any{}}
			pidOf[p.Service] = pid
		}
		refs := []any{}
		if !p.Parent.IsZero() {
			refs = append(refs, map[string]any{"refType": "CHILD_OF", "traceID": tidHex, "spanID": p.Parent.String()})
		}
		tags := []any{
			tag("synthetic", "bool", true),
			tag("slobudget.recon", "string", p.Reason),
			tag("slobudget.depth", "int64", p.Depth),
			tag("slobudget.recon.real_id", "bool", p.RealID),
			tag("span.kind", "string", p.Kind),
		}
		if p.Open {
			tags = append(tags, tag("slobudget.recon.open", "bool", true))
		}
		spans = append(spans, map[string]any{
			"traceID": tidHex, "spanID": p.ID.String(), "operationName": p.Operation, "references": refs,
			"startTime": p.Start / 1000, "duration": (p.End - p.Start) / 1000, "tags": tags, "logs": []any{},
			"processID": pid, "warnings": nil,
		})
	}
	tr["spans"] = spans
}

func tag(k, typ string, v any) map[string]any { return map[string]any{"key": k, "type": typ, "value": v} }

func binaryTag(tm map[string]any) []byte {
	v := asString(tm["value"])
	if v == "" {
		return nil
	}
	if asString(tm["type"]) == "binary" {
		for pad := 0; pad < 3; pad++ {
			if b, err := base64.StdEncoding.DecodeString(v + strings.Repeat("=", pad)); err == nil {
				return b
			}
		}
		return nil
	}
	return []byte(v)
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

func parseTraceID(s string) (recon.TraceID, error) {
	var t recon.TraceID
	b, err := hex.DecodeString(strings.Repeat("0", (32-len(s)%32)%32) + s)
	if err != nil || len(b) > 16 {
		return t, fmt.Errorf("trace id %q", s)
	}
	copy(t[16-len(b):], b)
	return t, nil
}

func parseSpanID(s string) (recon.SpanID, error) {
	var id recon.SpanID
	if s == "" {
		return id, nil
	}
	b, err := hex.DecodeString(strings.Repeat("0", (16-len(s)%16)%16) + s)
	if err != nil || len(b) > 8 {
		return id, fmt.Errorf("span id %q", s)
	}
	copy(id[8-len(b):], b)
	return id, nil
}

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }
func asList(v any) []any        { l, _ := v.([]any); return l }
func asString(v any) string      { s, _ := v.(string); return s }
func asInt(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return 0
}

// sortedKeys is used by tests to print maps deterministically.
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
