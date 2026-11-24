// Command replay runs the arrival program's instructions (../prog on ../vm) over packet
// captures and checks them, packet by packet, against a ground truth made without the program:
// every counter's change, every id and its time in the table, the ids that are split between
// packets; and, with the spans and the times their exports reached the collector, the agent's
// lookups (../../arrival) at those times, each against the outcome the truth implies.
//
//	replay [-slots 131072] [-mono -1h] CAPDIR TRUTHDIR
//
// CAPDIR holds node-*.pcap (tcpdump -i any -y LINUX_SLL2) and node-*.net (ip -o link; ==; ip route);
// TRUTHDIR, written by a separate program: truth.csv (node, id, key_packet, key_last_packet,
// last_packet, ts_ns, key_offset, ...: one request each), repeats.csv (node, packet, ts_ns, id,
// key_offset, whole: what retransmitted packets hold) and spans.csv (node, export_ns, service, span,
// parent, kind, start_ns, end_ns; may be empty).
package main

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrival"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrivald/prog"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrivald/vm"
)

// What the program must do, written down here and not taken from it.
const (
	port    = 12345
	scanCap = 8192 // positions of a packet
	keyLen  = 10
	maxLog  = 20 // differences printed per node
)

// request is one request's id in the truth.
type request struct {
	id                uint64
	key, keyEnd, last int // packets of the key's first and last byte and of the id's last digit
	ts                int64
	offset            int // of the key in its packet's payload
}

// held is an id in a packet, or a key whose id the packet cuts.
type held struct {
	id     uint64
	offset int
	whole  bool
}

type span struct {
	export, start int64
	service       string
	parent        uint64
	kind          int
}

func rows(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r, err := csv.NewReader(f).ReadAll()
	if err != nil || len(r) == 0 {
		return nil, err
	}
	return r[1:], nil
}

func num(s string) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func hexID(s string) uint64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		panic(err)
	}
	return v
}

// packet is one that leaves the host's end of a pod's veth towards the pod.
type packet struct {
	n       int
	ts      int64
	frame   []byte
	service bool // TCP to the services' port
	payload int
}

var routeRE = regexp.MustCompile(`(?m)^(192\.168\.\d+\.\d+) dev (cali\w+)`)
var linkRE = regexp.MustCompile(`(?m)^(\d+): ([^:@]+)`)

// packets reads a node's capture, as Ethernet frames.
func packets(dir, node string) ([]packet, error) {
	net, err := os.ReadFile(filepath.Join(dir, node+".net"))
	if err != nil {
		return nil, err
	}
	links, routes, _ := strings.Cut(string(net), "==")
	index := map[string]uint32{}
	for _, m := range linkRE.FindAllStringSubmatch(links, -1) {
		index[m[2]] = uint32(num(m[1]))
	}
	pod := map[uint32]bool{}
	for _, m := range routeRE.FindAllStringSubmatch(routes, -1) {
		if i, ok := index[m[2]]; ok {
			pod[i] = true
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, node+".pcap"))
	if err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(raw) != 0xa1b2c3d4 || binary.LittleEndian.Uint32(raw[20:])&0xffff != 276 {
		return nil, fmt.Errorf("%s: not a LINUX_SLL2 capture in microseconds", node)
	}
	var out []packet
	n := 0
	for p := 24; p+16 <= len(raw); {
		sec, usec, cl := binary.LittleEndian.Uint32(raw[p:]), binary.LittleEndian.Uint32(raw[p+4:]), int(binary.LittleEndian.Uint32(raw[p+8:]))
		p += 16
		if p+cl > len(raw) {
			break
		}
		d := raw[p : p+cl]
		p += cl
		n++
		if len(d) < 20 || d[10] != 4 || !pod[binary.BigEndian.Uint32(d[4:])] { // outgoing, to a pod
			continue
		}
		x := packet{n: n, ts: int64(sec)*1e9 + int64(usec)*1e3, frame: make([]byte, 14+cl-20)}
		copy(x.frame[12:], d[:2]) // the EtherType
		copy(x.frame[14:], d[20:])
		if ip := d[20:]; binary.BigEndian.Uint16(d) == 0x0800 && len(ip) >= 20 && ip[9] == 6 {
			ihl := int(ip[0]&15) * 4
			if len(ip) >= ihl+20 && binary.BigEndian.Uint16(ip[ihl+2:]) == port {
				x.service = true
				x.payload = int(binary.BigEndian.Uint16(ip[2:])) - ihl - int(ip[ihl+12]>>4)*4
			}
		}
		out = append(out, x)
	}
	return out, nil
}

type bench struct {
	vm    *vm.VM
	table *arrival.Table
	ctr   *arrival.Counters
}

func newBench(slots uint32) (*bench, error) {
	maps := map[int]*vm.Map{1: vm.NewMap(arrival.SlotSize, int(slots)), 2: vm.NewMap(arrival.NumCounters*8, 1), 3: vm.NewMap(prog.ScratchSize, 1)}
	ins, err := prog.Build(prog.Config{Port: port, Slots: slots}, prog.Maps{Table: 1, Counters: 2, Scratch: 3})
	if err != nil {
		return nil, err
	}
	m, err := vm.New(ins, maps)
	if err != nil {
		return nil, err
	}
	t, err := arrival.NewTable(maps[1].Bytes(), nil)
	if err != nil {
		return nil, err
	}
	c, err := arrival.NewCounters(maps[2].Bytes(), nil)
	return &bench{m, t, c}, err
}

func quantiles(v []float64, qs ...float64) string {
	if len(v) == 0 {
		return "none"
	}
	sort.Float64s(v)
	var s []string
	for _, q := range qs {
		i := q * float64(len(v)-1)
		lo := int(i)
		x := v[lo]
		if lo+1 < len(v) {
			x += (i - float64(lo)) * (v[lo+1] - v[lo])
		}
		s = append(s, fmt.Sprintf("%.3f", x))
	}
	return strings.Join(s, " / ")
}

// model is the table as it must be, from the truth alone: slot -> the id in it and its (wall) time.
type model struct {
	slots map[uint64]slot
	mask  uint64
}

type slot struct {
	id uint64
	ts int64
}

// node holds what the truth says about a node.
type node struct {
	name  string
	reqs  []request
	in    map[int][]held // packet -> the ids it holds, in order
	byID  map[uint64]request
	pk    []packet
	spans []span
	first int64
}

func (nd *node) prepare(repeats map[int][]held) {
	nd.in, nd.byID = map[int][]held{}, map[uint64]request{}
	for _, r := range nd.reqs {
		nd.byID[r.id] = r
		switch {
		case r.key == r.last:
			nd.in[r.key] = append(nd.in[r.key], held{r.id, r.offset, true})
		case r.key == r.keyEnd: // the key is whole, its digits are not
			nd.in[r.key] = append(nd.in[r.key], held{r.id, r.offset, false})
		}
	}
	for n, h := range repeats {
		nd.in[n] = append(nd.in[n], h...)
	}
	for _, h := range nd.in {
		sort.Slice(h, func(i, j int) bool { return h[i].offset < h[j].offset })
	}
	sort.Slice(nd.spans, func(i, j int) bool { return nd.spans[i].export < nd.spans[j].export })
	if len(nd.pk) > 0 {
		nd.first = nd.pk[0].ts
	}
}

// replay runs a node's packets and lookups with a table of slots and returns the differences.
func (nd *node) replay(w io.Writer, slots uint32, mono time.Duration, detail bool) (bad int, err error) {
	b, err := newBench(slots)
	if err != nil {
		return 0, err
	}
	wrong := func(format string, args ...any) {
		if bad++; bad <= maxLog {
			fmt.Fprintf(w, nd.name+": "+format+"\n", args...)
		}
	}
	// the replay's monotonic clock: the capture's wall clock shifted
	toMono := func(wall int64) uint64 { return uint64(wall + int64(mono)) }
	now := nd.first
	res := &arrival.Resolver{Table: b.table, Clock: arrival.NewClock(func() (int64, int64) { return now, int64(toMono(now)) })}
	must := model{map[uint64]slot{}, uint64(slots - 1)}
	var expect, last [arrival.NumCounters]uint64

	var wait, delay []float64
	skipped, lookedUp, next := 0, 0, 0
	lookups := func(until int64) {
		for ; next < len(nd.spans) && nd.spans[next].export < until; next++ {
			s := nd.spans[next]
			if s.kind != 2 {
				continue
			}
			now = s.export
			res.Clock.Tick()
			r, known := nd.byID[s.parent]
			if s.parent != 0 && !known {
				if s.start < nd.first+int64(time.Second) { // its request came before the capture (or before the connection was in step)
					skipped++
				} else {
					wrong("the span %s at %d has a parent %016x that no packet carried", s.service, s.start, s.parent)
				}
				continue
			}
			if s.parent != 0 && r.ts >= s.export {
				wrong("the span %s reached the collector before its request came", s.service)
				continue
			}
			want, wantAt := arrival.Hit, s.start
			h := must.slots[s.parent&must.mask]
			switch {
			case s.parent == 0:
				want = arrival.MissRoot
			case h.id == 0:
				want = arrival.MissEmpty
			case h.id != s.parent && h.ts > s.start:
				want = arrival.MissOverwritten
			case h.id != s.parent:
				want = arrival.MissNotStored
			case h.ts > s.start:
				want = arrival.MissLate
			default:
				wantAt = h.ts
			}
			lookedUp++
			at, c := res.Arrival(s.parent, s.start)
			if c != want || at != wantAt {
				wrong("lookup of %016x: %s at %d, want %s at %d", s.parent, arrival.CauseNames[c], at, arrival.CauseNames[want], wantAt)
			}
			if s.parent != 0 {
				delay = append(delay, float64(s.export-r.ts)/1e6)
				wait = append(wait, float64(s.start-r.ts)/1e6) // of all, from the truth
			}
		}
	}

	steps, runs, service, stored := 0, 0, 0, 0
	for _, p := range nd.pk {
		lookups(p.ts)
		now = p.ts
		ret, st, e := b.vm.Run(p.frame, binary.BigEndian.Uint16(p.frame[12:]), toMono(p.ts))
		if e != nil || ret != -1 {
			return bad, errors.Join(e, fmt.Errorf("%s packet %d: return value %d", nd.name, p.n, ret))
		}
		// what it must have counted and stored
		if p.service && p.payload > 0 {
			service++
			steps += st
			runs++
			expect[arrival.CountPackets]++
			ids := 0
			for _, h := range nd.in[p.n] {
				switch {
				case h.offset >= scanCap: // none on these captures; not silently left out
					wrong("packet %d: an id behind the cap", p.n)
				case !h.whole:
					expect[arrival.CountCut]++
				default:
					if r := nd.byID[h.id]; r.key == p.n && r.ts != p.ts {
						wrong("packet %d at %d: the truth has its id at %d", p.n, p.ts, r.ts)
					}
					ids++
					k := h.id & must.mask
					old := must.slots[k]
					if old.id == h.id {
						expect[arrival.CountRepeats]++
						break
					}
					if old.id != 0 {
						expect[arrival.CountOverwritten]++
					}
					must.slots[k] = slot{h.id, p.ts}
					stored++
				}
			}
			expect[arrival.CountIDs] += uint64(ids)
			if ids == 0 {
				expect[arrival.CountNoID]++
			}
			if p.payload-scanCap >= keyLen {
				expect[arrival.CountCapped]++
			}
		} else if len(nd.in[p.n]) > 0 {
			wrong("packet %d: ids in the truth, but no payload to the services' port", p.n)
		}
		if c := b.ctr.Read(); c != expect {
			wrong("packet %d: counters %v (%v), want %v", p.n, diff(c, last), arrival.CounterNames, diff(expect, last))
			expect = c
		}
		last = b.ctr.Read()
		for _, h := range nd.in[p.n] {
			m := must.slots[h.id&must.mask]
			if id, ns, ok := b.table.Read(h.id); !ok || id != m.id || m.id != 0 && ns != toMono(m.ts) {
				wrong("packet %d: the slot of %016x holds %016x at %d, want %016x at %d", p.n, h.id, id, ns, m.id, toMono(m.ts))
			}
		}
	}
	lookups(1 << 62)
	// the whole table
	for s := uint64(0); s < uint64(slots); s++ {
		m := must.slots[s]
		if id, ns, ok := b.table.Read(s); !ok || id != m.id || m.id != 0 && ns != toMono(m.ts) {
			wrong("slot %d holds %016x at %d, want %016x at %d", s, id, ns, m.id, toMono(m.ts))
		}
	}

	if detail {
		whole, split := 0, 0
		for _, r := range nd.reqs {
			if r.key == r.last {
				whole++
			} else {
				split++
			}
		}
		fmt.Fprintf(w, "%s: %d packets to pods (%d to the services' port with payload), %d requests: %d whole in one packet, %d split\n", nd.name, len(nd.pk), service, len(nd.reqs), whole, split)
		fmt.Fprintf(w, "%s: counters", nd.name)
		for i, v := range last {
			fmt.Fprintf(w, " %s=%d", arrival.CounterNames[i], v)
		}
		fmt.Fprintf(w, "; instructions per packet %.0f (most %d)\n", float64(steps)/float64(max(1, runs)), b.vm.MaxSteps)
	}
	if len(nd.spans) > 0 {
		fmt.Fprintf(w, "%s, %d slots: %d lookups:", nd.name, slots, lookedUp)
		for i, v := range res.Counts {
			fmt.Fprintf(w, " %s=%d", arrival.CauseNames[i], v)
		}
		fmt.Fprintf(w, "; %d spans from before the capture left out\n", skipped)
		if detail {
			fmt.Fprintf(w, "%s: arrival to the span at the collector, ms p50 / p90 / p99 / p99.9 / max: %s (%d spans)\n", nd.name, quantiles(delay, 0.5, 0.9, 0.99, 0.999, 1), len(delay))
			fmt.Fprintf(w, "%s: arrival to the handler's start, ms min / p1 / p50 / p95 / p99 / max: %s\n", nd.name, quantiles(wait, 0, 0.01, 0.5, 0.95, 0.99, 1))
		}
	}
	return bad, nil
}

func diff(a, b [arrival.NumCounters]uint64) (d [arrival.NumCounters]int64) {
	for i := range a {
		d[i] = int64(a[i] - b[i])
	}
	return d
}

func run() error {
	slots := flag.Uint("slots", 1<<17, "slots of the table in the main check")
	mono := flag.Duration("mono", -time.Hour, "the replay's monotonic clock minus the capture's wall clock")
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		return errors.New("a capture and a truth directory")
	}
	dir, td := flag.Arg(0), flag.Arg(1)
	nodes := map[string]*node{}
	repeats := map[string]map[int][]held{}
	names := []string{"node-1", "node-2", "node-3"}
	for _, n := range names {
		nodes[n], repeats[n] = &node{name: n}, map[int][]held{}
	}
	r, err := rows(filepath.Join(td, "truth.csv"))
	if err != nil {
		return err
	}
	for _, x := range r {
		nodes[x[0]].reqs = append(nodes[x[0]].reqs, request{hexID(x[1]), int(num(x[2])), int(num(x[3])), int(num(x[4])), num(x[5]), int(num(x[6]))})
	}
	if r, err = rows(filepath.Join(td, "repeats.csv")); err != nil {
		return err
	}
	for _, x := range r {
		n := int(num(x[1]))
		repeats[x[0]][n] = append(repeats[x[0]][n], held{hexID(x[3]), int(num(x[4])), x[5] == "1"})
	}
	if r, err = rows(filepath.Join(td, "spans.csv")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, x := range r {
		nodes[x[0]].spans = append(nodes[x[0]].spans, span{num(x[1]), num(x[6]), x[2], hexID(x[4]), int(num(x[5]))})
	}
	bad, requests, spans := 0, 0, 0
	for _, name := range names {
		nd := nodes[name]
		if nd.pk, err = packets(dir, name); err != nil {
			return err
		}
		nd.prepare(repeats[name])
		requests += len(nd.reqs)
		spans += len(nd.spans)
		n, err := nd.replay(os.Stdout, uint32(*slots), *mono, true)
		if err != nil {
			return err
		}
		bad += n
		for _, s := range []uint32{1 << 8, 1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18} {
			n, err := nd.replay(os.Stdout, s, *mono, false)
			if err != nil {
				return err
			}
			bad += n
		}
	}
	if bad > 0 {
		return fmt.Errorf("%d differences", bad)
	}
	if requests == 0 {
		return errors.New("no requests in the truth")
	}
	fmt.Printf("%d requests: every counter, id and time as the truth says, at 7 table sizes\n", requests)
	if spans > 0 {
		fmt.Printf("%d spans: every lookup's outcome and time as the truth says\n", spans)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "replay:", err)
		os.Exit(1)
	}
}
