# tools/recon: offline trace reconstruction

Reads a Jaeger JSON export (the query API shape `tools/jaeger_export.py` writes,
`{"data": [traces], ...}` or a bare list) and writes an export of the same shape
with placeholder ancestors added for every kept span whose parent was dropped,
plus a coverage report.  The logic is `slobudget/recon` (pdata-free), shared
with the collector-side processor `processor/slobudgetreconprocessor`, which is
out of the default build since Stage 2c (build tag `slobudget_recon`).

```
go run ./tools/recon -in R/export.json -out R/export.recon.json -report R/recon-offline.json
   [-callgraph deploy/callgraph.json] [-route compose_post]
   [-service-format 'unknown_service:%s_service_pb_es_proc'] [-no-synth]
```

A 220 MB export (60 000 traces, 165 000 spans) takes about 25 s and 5 GB of
memory (generic JSON); the output is 1.6 x the input.

## Placeholder rules

For every kept span whose parent is not in the trace a chain of placeholders is
built upwards until it reaches a kept span or the root:

- The missing parent's id is the kept span's own parent id (exact).  The
  grandparent of a server span is the `_br` checkpoint anchor (exact).  Ids
  further up are reused when another kept span of the trace named the same slot
  (depth, service, kind, callee); otherwise they are synthesised as FNV-64 of
  the trace id and the slot, so every kept span whose chain passes through the
  slot computes the same id, and no two traces share one (`-no-synth` stops
  chains there instead and marks the placeholder `slobudget.recon.open`).
- Service: the parent of a server span is the caller's client span (caller from
  the call graph), the parent of a client span is the same service's server
  span; depth 0 is the route's root service (`compose_post root [synthetic]`).
- Operation: `<Svc>Client_<Method> [synthetic]` for the client span above a kept
  server span (derivable from the child's name); `<service> server [synthetic]`
  and `<caller> client to <callee> [synthetic]` otherwise.  Every placeholder
  name ends in `[synthetic]`, so per-operation figures of real operations are
  unchanged.
- Timing: start and end bound the kept descendants, so a placeholder adds no
  own time of its own beyond the gaps between non-overlapping children.
- Tags: `synthetic=true`, `slobudget.recon` (parent-missing | root-missing),
  `slobudget.depth`, `slobudget.recon.real_id`, `span.kind`.  A process entry
  is reused when the trace has one for the service, else added with the
  service name seen elsewhere in the export (or `-service-format`).

## What the Path Bridge fields carry (cpd 2, verified on the three Stage 2b exports)

- `_br` (tag type binary, base64 in JSON) = `uvarint(depth) || anchor[8] ||
  bloom[3]`, 12 bytes on every one of the 304 817 server spans, never on a
  client span.  `depth` is the absolute depth with the root at 0; every `_br`
  depth is even (0 / 2 / 4 / 6: wrk2api, composepost, the leaves and
  hometimeline, socialgraph), i.e. with cpd 2 every server span is a
  checkpoint.  `anchor` is the span id of the nearest checkpoint ancestor
  strictly above = the grandparent (the caller's server span): in all 3 171
  cases where parent and grandparent were both kept, anchor == grandparent, and
  it never equals the parent.  The root's `_br` is all zeros (depth 0, no
  anchor, empty bloom).  The bloom holds the ancestors strictly between the
  anchor and the span (one client span) and is not used here (see the comparison
  with the bridges repo below).
- `_d` (binary) = `uvarint(depth)`, 1 byte on every one of the 188 924 client
  spans (odd depths 1 / 3 / 5), never on a server span.
- Decoder: `recon.DecodeBridge(br, d)`; the Python equivalent is
  `depth_and_anchor` in `tools/jaeger_orphans.py` (it reads the depth as one
  byte, which holds while depth < 128).

## Coverage report

Per kept span whose parent is missing (an orphan), by known ids only: parent
id, then the anchor of a server span.

| class | meaning | hole |
|---|---|---|
| kept | a kept ancestor is reached (the grandparent, via the anchor) | 1 |
| root | the root slot is reached: depth-1 span, or a depth-2 server span whose anchor is the missing root | 1 or 2 |
| open | the next id up is nobody's: a client span's grandparent, or a server span's missing anchor at depth >= 4 | 1 or 2 known missing links |

`placeable_by_known_ids` = kept + root.  `hole_hist_full` counts the
placeholders between the orphan and its nearest real ancestor or the root in
the full reconstruction.  `traces_one_tree_after_known_ids_only` reconstructs
without synthesised ids.  `by_reason` has the same classes per keep reason and kind
(`outlier/server`, `plan/client`, ...; arms that stamp `slobudget.reason`): spans, the
root span of their trace kept, parent kept, orphans by class, and spans that reach
the root (or its slot) over kept parents and known ids.

### The three Stage 2b exports (10 min, 100 req/s, collection volume target 12 %)

| export | kept spans | orphans | placeable by known ids | hole = 1 | hole by known ids (kept:1 / root:1 / root:2 / open:1 / open:2) | full hole 1 / 2 / 3 / 4 / 5 / 6 | placeholders (per real span, synthetic-id share) | one tree before / known ids only / with synthesis |
|---|---|---|---|---|---|---|---|---|
| on-demand roots (`211026`) | 164 917 | 133 528 (81 %) | 20 369 = 15.3 % of orphans, 12.4 % of kept | 15 892 = 11.9 % of orphans, 78 % of placeable | 10 705 / 5 187 / 4 477 / 57 544 / 55 615 | 22 277 / 19 599 / 41 567 / 29 386 / 11 971 / 8 728 | 262 009 (1.59, 41 %) | 1.1 % / 14.9 % / 100 % |
| pinned roots (`215600`) | 167 114 | 94 394 (56 %) | 10 323 = 10.9 % / 6.2 % | 10 323 = 10.9 %, 100 % of placeable | 10 323 / 0 / 0 / 40 916 / 43 155 | 13 412 / 30 752 / 30 799 / 10 259 / 9 172 | 182 268 (1.09, 37 %) | 17.3 % / 27.0 % / 100 % |
| on-demand, burst (`222804`) | 161 710 | 128 106 (79 %) | 19 548 = 15.3 % / 12.1 % | 15 498 = 12.1 %, 79 % of placeable | 10 496 / 5 002 / 4 050 / 53 894 / 54 664 | 21 170 / 18 134 / 41 396 / 27 465 / 11 733 / 8 208 | 261 166 (1.61, 42 %) | 3.9 % / 17.7 % / 100 % |

Reading: by known ids alone only one orphan in seven is placeable (the server
spans whose caller's server span was kept, and the spans directly under the
root); client spans carry only their depth, so their chains need a synthesised
id after the first placeholder (the server span of their own service).  With
the synthesised slot ids every trace becomes one tree, at 1.1 - 1.6
placeholders per real span; with pinned roots the root placeholder disappears
and the ratio drops to 1.09.  The collector-side run of the same arm
(`cluster-20261120-224354-ondemand-recon2`, a different 10 minutes) wrote
255 698 placeholders for 164 627 real spans, 1.56 per real span, against the
offline 1.59 here.

## Compared with the bridges repo's reconstructor (checkpoint distance 2)

Read: docc-lab/bridges `e5bf8bd`, package `recon` (`ReconstructPB0`, the maximal-evidence Path
Bridge reconstructor, and `NewPCRBConfig(2, 8, 1e-4)` with the repo's defaults, `Prehashed`), and the
deployed SDK's encoder (`pb_processor.go`).  Both take the same wire: uvarint depth, 8-byte anchor,
3-byte bloom (20 bits, 14 probes, span ids unhashed).

| field | the bridges repo | `tools/recon` |
|---|---|---|
| anchor | names the checkpoint above; a fragment attaches to the surviving span with that id, at the window's top (here the grandparent), else to nothing | the same: a server span attaches to its anchor when it was kept (class `kept`, hole 1) |
| depth | window bounds, candidate levels, gap = depth difference - 1 | the gap, and the root class (depth 1 and 2) |
| bloom | candidate and chain test; a fragment without its own payload borrows evidence | not read |

The bloom cannot change a placement at distance 2: it holds the one ancestor between the anchor and
the span, which is the span's own parent, known exactly from its parent id, and no candidate level
lies between them.  Checked on 21 runs' exports with the runs' 100 % shadows as truth (1.17 million
server spans without their parent): the bloom contained the parent every time (0 misses), the anchor
was the true grandparent every time, and the bridges reconstructor attached exactly the spans that
`tools/recon` calls `kept`, each at its true nearest kept ancestor with the true gap (0 wrong, 0
different).  The filter's own false-positive rate is 5 to 9 % (designed 0.01 %); it does not matter at
distance 2 because nothing is left to test.

Coverage on those exports (12 % volume target, 60 000 traces each; "broken link" = a kept span whose
parent was not kept):

| arm and scenario | broken links (share of kept) | rejoined to a kept span by bridge data | joined at the root slot by its known id | rest |
|---|---|---|---|---|
| chosen, steady / saturated / other scenarios | 83 to 85 % | 5.5 to 6.0 % | 8.0 to 8.6 % | 85 to 86 % |
| canopy, steady / saturated | 90 % | 3.9 to 4.0 % | 5.0 to 5.1 % | 91 % |
| reservoir, steady / saturated | 13 / 19 % | 0.7 / 13.0 % | 2.0 / 3.2 % | 97 / 84 % |
| keep-parent, steady / saturated | 83 to 85 % | 5.9 to 6.0 % | 8.0 to 8.5 % | 86 % |
| head | complete traces (about 0 broken links) | | | |

The bridges repo's reconstruction uses the same evidence, so this is also the ceiling of the Path
Bridge at distance 2 on these kept sets.  What stops the rest on the chosen arm: for 67 to 73 % of
the broken links no kept span sits anywhere above (four of five traces lose their root); 12 to 14 %
are client spans, which carry only their depth (their parent server span's record is gone); 10 to
14 % are server spans whose anchor (the checkpoint two levels up) was itself dropped.  The anchor of
a server span is kept about 12 % of the time, the sampling rate, and nothing at distance 2 points
further up.

### Hole lengths, and what would place a hole of 3 (simulated, checkpoint distance as a parameter)

"Hole" = missing spans between a kept span and its nearest kept ancestor.  On the chosen arm (60 000
traces, 12 %) 68 % of the broken links have no kept ancestor at all (nothing to attach to); of the
others holes of 1 / 2 / 3 / 4 / 5 spans are 11.2 / 10.1 / 6.1 / 2.5 / 1.9 % of all broken links
(saturated seed 1).  The "holes of 3 are the most common" of the Stage 2c report counts the hole up to
the trace start when nothing above is kept (`hole_hist_full`); there most holes of 3 are depth-3
client spans with nothing kept above, which no bridge can attach to a kept span.

The simulation replays the bridges repo's own payload rules over the true spans of eight runs (the real
kept sets, the deployed payload and hash, `ReconstructPB0` as shipped) and checks every placement
against the shadow.  Shares of holes of 3 with a kept ancestor that are placed (saturated, chosen,
seed 1; runs vary by 2 to 6 points, 0.6 % on reservoir at distance 4):

| payload and reader | bytes per kept span | hole of 3 placed | attachable links placed | wrong |
|---|---|---|---|---|
| distance 2, as deployed | 6.9 | 0 % | 19 % | 0 |
| distance 3 | 10.1 | 0 % | 21 % | 4 of 9 175 |
| distance 4 | 8.8 | 58 % (depth-4 servers only) | 24 % | 0 of 10 203 |
| distance 6 | 9.1 | 51 % | 27 % | 63 of 11 799 |
| payload on every span, distance 4, a reader that needs no kept window root | 17.0 | 58 % | 64 % | 0 of 27 516 |
| the same, distance 6 | 21.0 | 99 % | 99 % | 689 of 43 261 |
| the same, distance 7 | 24.0 | 100 % | 100 % | 254 of 43 078 |

The last three rows are not an existing bridge: the SDK builds the payload for every span and strips it
at export (checkpoints and leaf servers keep it), and the shipped readers require the window's root
record.  The call-graph slot (depth, service, operation, kind) is unique in every trace of these runs
(0 of 60 000 traces with a repeated slot in seven runs, 0 of 72 000 in the bursty one), so the full
reconstruction (placeholder rules above) places all of these from the call graph alone; a bridge matters
for applications with repeated calls.
