package prog_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrival"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrivald/prog"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrivald/vm"
)

const port = 12345

// bench is the program on the interpreter with the agent's reader on its table.
type bench struct {
	t     testing.TB
	vm    *vm.VM
	table *arrival.Table
	ctr   *arrival.Counters
	last  [arrival.NumCounters]uint64
}

func newBench(t testing.TB, slots uint32) *bench {
	t.Helper()
	maps := map[int]*vm.Map{1: vm.NewMap(arrival.SlotSize, int(slots)), 2: vm.NewMap(arrival.NumCounters*8, 1), 3: vm.NewMap(prog.ScratchSize, 1)}
	ins, err := prog.Build(prog.Config{Port: port, Slots: slots}, prog.Maps{Table: 1, Counters: 2, Scratch: 3})
	if err != nil {
		t.Fatal(err)
	}
	m, err := vm.New(ins, maps)
	if err != nil {
		t.Fatal(err)
	}
	tab, err := arrival.NewTable(maps[1].Bytes(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctr, err := arrival.NewCounters(maps[2].Bytes(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &bench{t: t, vm: m, table: tab, ctr: ctr}
}

// run sends a frame and returns what the counters gained.
func (b *bench) run(frame []byte, now uint64) (d [arrival.NumCounters]uint64) {
	b.t.Helper()
	ret, _, err := b.vm.Run(frame, 0x0800, now)
	if err != nil {
		b.t.Fatal(err)
	}
	if ret != -1 {
		b.t.Fatalf("return value %d: the packet must go on to the next filter", ret)
	}
	c := b.ctr.Read()
	for i := range c {
		d[i] = c[i] - b.last[i]
	}
	b.last = c
	return d
}

func (b *bench) want(d [arrival.NumCounters]uint64, pairs ...uint64) {
	b.t.Helper()
	var w [arrival.NumCounters]uint64
	for i := 0; i < len(pairs); i += 2 {
		w[pairs[i]] = pairs[i+1]
	}
	if d != w {
		b.t.Fatalf("counters %v (%v), want %v", d, arrival.CounterNames, w)
	}
}

func (b *bench) stored(id, ns uint64) {
	b.t.Helper()
	got, at, ok := b.table.Read(id)
	if !ok || got != id || at != ns {
		b.t.Fatalf("id %016x: slot holds %016x at %d (ok %v), want %d", id, got, at, ok, ns)
	}
}

type opts struct {
	dport, frag     uint16
	proto           byte
	ipOpts, tcpOpts int // bytes of options
	pad             int // bytes after the IP packet
	version         byte
}

// frame is an Ethernet frame with an IPv4 TCP packet to the service port.
func frame(payload []byte, o opts) []byte {
	if o.dport == 0 {
		o.dport = port
	}
	if o.proto == 0 {
		o.proto = 6
	}
	if o.version == 0 {
		o.version = 4
	}
	ihl, doff := 20+o.ipOpts, 20+o.tcpOpts
	f := make([]byte, 14+ihl+doff, 14+ihl+doff+len(payload)+o.pad)
	binary.BigEndian.PutUint16(f[12:], 0x0800)
	ip := f[14:]
	ip[0] = o.version<<4 | byte(ihl/4)
	binary.BigEndian.PutUint16(ip[2:], uint16(ihl+doff+len(payload)))
	binary.BigEndian.PutUint16(ip[6:], o.frag)
	ip[9] = o.proto
	tcp := ip[ihl:]
	binary.BigEndian.PutUint16(tcp[0:], 40000)
	binary.BigEndian.PutUint16(tcp[2:], o.dport)
	tcp[12] = byte(doff/4) << 4
	f = append(f, payload...)
	return append(f, make([]byte, o.pad)...)
}

const tail = `","TraceFlags":"01","TraceState":"","Remote":false}}`

// message is a request's bytes: some before the key, the key, the id, the 52 bytes after it.
func message(before int, id uint64) []byte {
	return append(append([]byte(strings.Repeat("x", before)), fmt.Sprintf(`%s%016x`, prog.Key, id)...), tail...)
}

const (
	pk = arrival.CountPackets
	id = arrival.CountIDs
	no = arrival.CountNoID
)

func TestOneID(t *testing.T) {
	b := newBench(t, 1024)
	b.want(b.run(frame(message(143, 0x9632f86518e15343), opts{tcpOpts: 12}), 77), pk, 1, id, 1)
	b.stored(0x9632f86518e15343, 77)
}

// Every position of the key in a packet of several chunks, the chunk borders among them.
func TestEveryPosition(t *testing.T) {
	b := newBench(t, 4096)
	rng := rand.New(rand.NewSource(1))
	for before := 0; before < 3*prog.Chunk+40; before++ {
		x := rng.Uint64()&^0xfff | uint64(before+1) // a slot of its own
		b.want(b.run(frame(message(before, x), opts{tcpOpts: 12}), uint64(before+1)), pk, 1, id, 1)
		b.stored(x, uint64(before+1))
	}
}

func TestEveryLength(t *testing.T) {
	// the packet ends 0 to 40 bytes after the id's first digit: cut short of 16 digits
	for end := 0; end < 41; end++ {
		b := newBench(t, 1024)
		m := message(300, 0x0123456789abcdef)[:300+prog.KeyLen+end]
		d := b.run(frame(m, opts{tcpOpts: 12}), 5)
		if end < prog.IDLen {
			b.want(d, pk, 1, no, 1, arrival.CountCut, 1)
		} else {
			b.want(d, pk, 1, id, 1)
			b.stored(0x0123456789abcdef, 5)
		}
	}
	// the packet ends inside the key: nothing to see
	for end := 0; end < prog.KeyLen; end++ {
		b := newBench(t, 1024)
		b.want(b.run(frame(message(300, 1)[:300+end], opts{tcpOpts: 12}), 5), pk, 1, no, 1)
	}
}

func TestSeveralIDs(t *testing.T) {
	b := newBench(t, 1<<16)
	for _, n := range []int{2, 3, 7, 12} {
		rng := rand.New(rand.NewSource(int64(n)))
		var m []byte
		var ids []uint64
		for i := 0; i < n; i++ {
			x := rng.Uint64() | 1
			ids = append(ids, x)
			m = append(m, message(90+rng.Intn(400), x)...)
			m = append(m, "\x00\x00\x08\x06\x00\x00\x00\x00\x00"...) // another frame's header
		}
		b.want(b.run(frame(m, opts{tcpOpts: 12}), uint64(n)), pk, 1, id, uint64(n))
		for _, x := range ids {
			b.stored(x, uint64(n)) // one time for the whole packet
		}
	}
}

func TestFirstTimeStaysAndOverwrite(t *testing.T) {
	b := newBench(t, 16)
	a, c := uint64(0x1111111111111115), uint64(0x2222222222222225) // the same slot
	b.want(b.run(frame(message(20, a), opts{}), 100), pk, 1, id, 1)
	b.want(b.run(frame(message(20, a), opts{}), 200), pk, 1, id, 1, arrival.CountRepeats, 1)
	b.stored(a, 100)
	b.want(b.run(frame(message(20, c), opts{}), 300), pk, 1, id, 1, arrival.CountOverwritten, 1)
	b.stored(c, 300)
	if got, _, _ := b.table.Read(a); got != c {
		t.Fatal("the slot still holds the first id")
	}
}

func TestNotOurs(t *testing.T) {
	m := message(20, 7)
	for name, f := range map[string][]byte{
		"another port": frame(m, opts{dport: 2000}), "udp": frame(m, opts{proto: 17}), "a later fragment": frame(m, opts{frag: 0x00b9}),
		"a first fragment": frame(m, opts{frag: 0x2000}), "no payload": frame(nil, opts{tcpOpts: 12}), "no payload, padded": frame(nil, opts{pad: 6}),
		"ip version 6": frame(m, opts{version: 6}), "short frame": frame(m, opts{})[:30], "only ethernet": make([]byte, 14),
	} {
		b := newBench(t, 64)
		d := b.run(f, 1)
		if d != [arrival.NumCounters]uint64{} {
			t.Errorf("%s: counters %v", name, d)
		}
	}
	b := newBench(t, 64) // not IPv4
	if _, _, err := b.vm.Run(frame(m, opts{}), 0x86dd, 1); err != nil || b.ctr.Read() != [arrival.NumCounters]uint64{} {
		t.Fatalf("IPv6: %v %v", err, b.ctr.Read())
	}
}

func TestHeadersWithOptions(t *testing.T) {
	for ipo := 0; ipo <= 40; ipo += 4 {
		for tcpo := 0; tcpo <= 40; tcpo += 4 {
			b := newBench(t, 64)
			b.want(b.run(frame(message(33, 0xabc), opts{ipOpts: ipo, tcpOpts: tcpo}), 9), pk, 1, id, 1)
			b.stored(0xabc, 9)
		}
	}
}

func TestPaddingIsNotPayload(t *testing.T) {
	// the bytes after the IP packet are padding, also when they look like a key
	b := newBench(t, 64)
	f := append(frame([]byte("short"), opts{}), message(3, 0xbad)...)
	b.want(b.run(f, 1), pk, 1, no, 1)
	// a frame shorter than the IP header says: up to the frame's end
	f = frame(message(10, 0xfeed), opts{})
	b.want(b.run(f[:len(f)-40], 1), pk, 1, id, 1)
}

func TestBadDigits(t *testing.T) {
	b := newBench(t, 64)
	ok := "0123456789abcdef"
	for i := 0; i < 16; i++ {
		for c := 0; c < 256; c++ {
			good := strings.IndexByte(ok, byte(c)) >= 0
			text := []byte("1" + strings.Repeat("0", 14) + "1")
			text[i] = byte(c)
			m := append([]byte("xx"+prog.Key), append(text, tail...)...)
			d := b.run(frame(m, opts{}), 3)
			if good && d[id] != 1 || !good && d[arrival.CountBadID] != 1 || !good && d[no] != 1 || good == (d[arrival.CountBadID] == 1) {
				t.Fatalf("digit %d = %#x: %v", i, c, d)
			}
		}
	}
	// the invalid id 0
	b.want(b.run(frame(message(2, 0), opts{}), 3), pk, 1, no, 1, arrival.CountBadID, 1)
}

func TestIDValues(t *testing.T) {
	b := newBench(t, 1<<16)
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 3000; i++ {
		x := rng.Uint64()
		if i < 64 {
			x = 1 << i
		} else if i < 128 {
			x = ^uint64(0) >> (i - 64)
		}
		b.run(frame(message(i%300, x), opts{tcpOpts: 12}), uint64(i+1))
		got, ns, ok := b.table.Read(x)
		if !ok || got != x || ns == 0 {
			t.Fatalf("%016x: got %016x", x, got)
		}
	}
}

func TestScanCap(t *testing.T) {
	b := newBench(t, 1024)
	// the key at the last position that is scanned, and at the first that is not
	b.want(b.run(frame(message(prog.ScanCap-1, 0x77), opts{}), 4), pk, 1, id, 1, arrival.CountCapped, 1)
	b.stored(0x77, 4)
	b.want(b.run(frame(message(prog.ScanCap, 0x78), opts{}), 4), pk, 1, no, 1, arrival.CountCapped, 1)
	// nothing left for a key behind the cap
	b.want(b.run(frame(message(prog.ScanCap, 1)[:prog.ScanCap+prog.KeyLen-1], opts{}), 4), pk, 1, no, 1)
}

func TestAlmostKeys(t *testing.T) {
	b := newBench(t, 64)
	for _, s := range []string{`"SpanID": "0123456789abcdef"`, `"SpanId":"0123456789abcdef"`, `\"SpanID\":\"0123456789abcdef`, `SpanID":"0123456789abcdef`, `"SpanID"`, `"SpanID""SpanID"`, strings.Repeat(`"`, 700)} {
		b.want(b.run(frame([]byte("xx"+s), opts{}), 1), pk, 1, no, 1)
	}
	// the key's first eight bytes over and over: more candidates than a chunk's list holds
	d := b.run(frame([]byte("xx"+strings.Repeat(`"SpanID"`, 40)), opts{}), 1)
	b.want(d, pk, 1, no, 1, arrival.CountCrowded, 2)
	// what that packet left in the buffer does not push this packet's key out of the list
	if d := b.run(frame(message(50, 0x77), opts{}), 2); d[id] != 1 || d[arrival.CountErrors] != 0 {
		t.Fatalf("after a crowded packet: %v", d)
	}
	b.stored(0x77, 2)
	b = newBench(t, 64)
	// a key in user text, then the real one
	m := append([]byte(`text "SpanID":"not an id"`), message(50, 0x55aa)...)
	b.want(b.run(frame(m, opts{}), 8), pk, 1, id, 1, arrival.CountBadID, 1)
	b.stored(0x55aa, 8)
}

// Bytes an earlier packet left in the buffer are not this packet's.
func TestStaleBuffer(t *testing.T) {
	b := newBench(t, 64)
	b.run(frame(message(100, 0x1234), opts{}), 1)
	for n := 1; n < 200; n++ {
		d := b.run(frame([]byte(strings.Repeat("y", n)), opts{}), 2)
		b.want(d, pk, 1, no, 1)
	}
	// a key cut by the packet's end, the rest of it and its digits left over from the last packet
	for end := 1; end < 110; end++ {
		b := newBench(t, 64)
		b.run(frame(message(100, 0x1234), opts{}), 1)
		b.last = b.ctr.Read()
		d := b.run(frame(message(100, 0x9999)[:100+min(end, 25)], opts{}), 2)
		if d[id] != 0 {
			t.Fatalf("end %d: an id from stale bytes %v", end, d)
		}
	}
}

// Random bytes over the key's alphabet against a plain search.
func TestRandomAgainstSearch(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	alphabet := []byte(`"SpanID:"0f`)
	for i := 0; i < 1500; i++ {
		b := newBench(t, 1<<12)
		n := 1 + rng.Intn(1200)
		m := make([]byte, n)
		for j := range m {
			m[j] = alphabet[rng.Intn(len(alphabet))]
		}
		for k := rng.Intn(5); k > 0; k-- { // some real ones
			x := fmt.Sprintf("%s%016x", prog.Key, rng.Uint64()|1)
			if n > len(x) {
				copy(m[rng.Intn(n-len(x)):], x)
			}
		}
		want := map[uint64]bool{}
		cut, bad := 0, 0
		for p := 0; p+prog.KeyLen <= n; p++ {
			if string(m[p:p+prog.KeyLen]) != prog.Key {
				continue
			}
			if p+prog.KeyLen+prog.IDLen > n {
				cut++
				continue
			}
			var x uint64
			good := true
			for _, c := range m[p+prog.KeyLen : p+prog.KeyLen+prog.IDLen] {
				v := strings.IndexByte("0123456789abcdef", c)
				good = good && v >= 0
				x = x<<4 | uint64(max(v, 0))
			}
			if good && x != 0 {
				want[x] = true
			} else {
				bad++
			}
		}
		d := b.run(frame(m, opts{tcpOpts: 4 * rng.Intn(11)}), 6)
		none := uint64(0)
		if len(want) == 0 {
			none = 1
		}
		if d[id]-d[arrival.CountRepeats] != uint64(len(want)) || d[arrival.CountCut] != uint64(cut) || d[arrival.CountBadID] != uint64(bad) || d[no] != none || d[arrival.CountErrors] != 0 {
			t.Fatalf("packet %d: %v, want %d ids %d cut %d bad\n%q", i, d, len(want), cut, bad, m)
		}
		for x := range want {
			got, _, _ := b.table.Read(x)
			if got != x && d[arrival.CountOverwritten] == 0 {
				t.Fatalf("packet %d: %016x not stored", i, x)
			}
		}
	}
}

func TestTooManyCandidates(t *testing.T) {
	// more keys in one chunk than the candidate list holds: counted, the first ones kept
	b := newBench(t, 1<<12)
	var m []byte
	for i := 0; i < 8; i++ {
		m = append(m, fmt.Sprintf("%s%016x", prog.Key, i+1)...)
	}
	d := b.run(frame(m, opts{}), 2)
	if d[id] != 4 || d[arrival.CountCrowded] != 1 || d[arrival.CountErrors] != 0 {
		t.Fatalf("%v", d)
	}
}

// Payload in page fragments (outside the skb's linear part) is read like any other and counted.
func TestPaged(t *testing.T) {
	const hdr = 14 + 20 + 32 // of a frame with 12 bytes of TCP options
	two := append(message(100, 0xa1), message(300, 0xb2)...)
	first := 100 + prog.KeyLen + prog.IDLen            // the first id's end in the payload
	second := len(message(100, 1)) + 300 + first - 100 // the second's
	for _, x := range []struct{ linear, paged, ids uint64 }{
		{0, 0, 0}, {hdr, 1, 2}, {hdr + 1, 1, 2}, {hdr + uint64(first) - 1, 1, 2}, {hdr + uint64(first), 1, 1},
		{hdr + uint64(second) - 1, 1, 1}, {hdr + uint64(second), 1, 0}, {hdr + uint64(len(two)) - 1, 1, 0}, {hdr + uint64(len(two)), 0, 0}, {1 << 16, 0, 0},
	} {
		b := newBench(t, 1024)
		b.vm.Linear = int(x.linear)
		b.want(b.run(frame(two, opts{tcpOpts: 12}), 9), pk, 1, id, 2, arrival.CountPaged, x.paged, arrival.CountPagedIDs, x.ids)
		b.stored(0xa1, 9)
		b.stored(0xb2, 9)
		b.want(b.run(frame(two, opts{tcpOpts: 12}), 10), pk, 1, id, 2, arrival.CountRepeats, 2, arrival.CountPaged, x.paged, arrival.CountPagedIDs, x.ids)
	}
	// an id in a later chunk, the linear part ending before, inside and after it
	b := newBench(t, 1024)
	m := message(5*prog.Chunk+17, 0xc3)
	end := hdr + 5*prog.Chunk + 17 + prog.KeyLen + prog.IDLen
	for _, x := range []struct{ linear, ids int }{{hdr + 200, 1}, {end - 1, 1}, {end, 0}} {
		b.vm.Linear = x.linear
		d := b.run(frame(m, opts{tcpOpts: 12}), 9)
		if d[arrival.CountPaged] != 1 || d[arrival.CountPagedIDs] != uint64(x.ids) || d[id] != 1 {
			t.Fatalf("linear %d: %v", x.linear, d)
		}
	}
	b.vm.Linear = 0
}

// The limits of the kernel's verifier the program is built around, and what a packet costs.
func TestSize(t *testing.T) {
	b := newBench(t, 1<<17)
	b.run(frame(message(143, 5), opts{tcpOpts: 12}), 1)
	small := b.vm.MaxSteps
	b.run(frame(message(1100, 6), opts{tcpOpts: 12}), 1)
	mid := b.vm.MaxSteps
	b.run(frame(message(20000, 7), opts{}), 1)
	t.Logf("%d instructions; steps: %d for a 221-byte payload, %d for 1178, %d at the cap", b.vm.Len(), small, mid, b.vm.MaxSteps)
	if b.vm.MaxSteps > 100000 {
		t.Fatalf("%d steps at the cap", b.vm.MaxSteps)
	}
}

func TestBuildRejects(t *testing.T) {
	for _, c := range []prog.Config{{Port: port, Slots: 0}, {Port: port, Slots: 1000}, {Port: 0, Slots: 1024}} {
		if _, err := prog.Build(c, prog.Maps{}); err == nil {
			t.Errorf("%+v accepted", c)
		}
	}
}

// Every instruction is executed by these packets (a wrong jump shows up as dead code), but the
// three that count a chunk the kernel cannot copy: its length was checked before.
func TestCoverage(t *testing.T) {
	b := newBench(t, 16)
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 400; i++ {
		var m []byte
		for k := rng.Intn(14); k >= 0; k-- {
			switch rng.Intn(7) {
			case 0:
				m = append(m, prog.Key+"nothexdigits1234"...)
			case 1:
				m = append(m, fmt.Sprintf("%s%016x", prog.Key, rng.Intn(64))...)
			default:
				m = append(m, message(rng.Intn(60), uint64(rng.Intn(64)))...)
			}
		}
		b.run(frame(m[:max(0, len(m)-rng.Intn(80))], opts{tcpOpts: 12}), uint64(i+1))
	}
	for at := 0; at < prog.Chunk; at++ { // a candidate in every block
		b.run(frame(message(at, 3), opts{}), 1)
		b.run(frame(message(at, 3)[:at+prog.KeyLen+3], opts{}), 1)
	}
	for n := 0; n < 8; n++ { // keys one after the other, each slot of the candidate list
		var m []byte
		for i := 0; i < n; i++ {
			m = append(m, fmt.Sprintf("%s%016x", prog.Key, 0x100+i%3)...)
		}
		b.run(frame(append(m, prog.Key+"0"...), opts{}), 1)
		b.run(frame(append(m, prog.Key+"zzzzzzzzzzzzzzzzzz"...), opts{}), 1)
		b.run(frame(append(m, fmt.Sprintf("%s%016x", prog.Key, 0x1ff0+n)...), opts{}), 1)
	}
	b.run(frame(message(20000, 1), opts{}), 1)
	b.run(frame(message(prog.ScanCap-1, 1), opts{}), 1)
	for _, o := range []opts{{dport: 1}, {proto: 17}, {frag: 1}, {version: 6}} {
		b.run(frame(message(1, 1), o), 1)
	}
	f := frame(message(10, 2), opts{})
	b.run(f[:len(f)-40], 1)                                 // shorter than its IP header says
	b.run(frame([]byte(`x"SpanID" : "SpanID"`), opts{}), 1) // candidates without a key
	ins, err := prog.Build(prog.Config{Port: port, Slots: 16}, prog.Maps{Table: 1, Counters: 2, Scratch: 3})
	if err != nil {
		t.Fatal(err)
	}
	first, last := -1, -1 // the instructions of the block
	for it := ins.Iterate(); it.Next(); {
		switch it.Ins.Symbol() {
		case "unreadable":
			first = int(it.Offset)
		case "done":
			last = int(it.Offset)
		}
	}
	if first < 0 || last < first {
		t.Fatalf("block %d to %d", first, last)
	}
	for _, u := range b.vm.Unreached() {
		if u < first || u >= last {
			t.Errorf("instruction %d of %d not executed", u, b.vm.Len())
		}
	}
}
