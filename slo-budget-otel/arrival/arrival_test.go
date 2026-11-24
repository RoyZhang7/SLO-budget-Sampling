package arrival

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// slots is a table's memory, written as the program writes it.
type slots struct {
	words []uint64
	mem   []byte
}

func newSlots(n int) *slots {
	w := make([]uint64, 2*n)
	return &slots{w, unsafe.Slice((*byte)(unsafe.Pointer(&w[0])), 16*n)}
}

func (s *slots) put(id, ns uint64) {
	i := int(Index(id, uint32(len(s.words)/2))) * 2
	atomic.StoreUint64(&s.words[i+1], 0)
	atomic.StoreUint64(&s.words[i], id)
	atomic.StoreUint64(&s.words[i+1], ns)
}

func (s *slots) table(t *testing.T) *Table {
	t.Helper()
	tab, err := NewTable(s.mem, nil)
	if err != nil {
		t.Fatal(err)
	}
	return tab
}

func TestLayout(t *testing.T) {
	s := newSlots(8)
	s.put(0x1122334455667705, 42) // low bits 101: slot 5
	if got := binary.LittleEndian.Uint64(s.mem[5*SlotSize+OffID:]); got != 0x1122334455667705 {
		t.Fatalf("id %x", got)
	}
	if got := binary.LittleEndian.Uint64(s.mem[5*SlotSize+OffTime:]); got != 42 {
		t.Fatalf("time %d", got)
	}
	id, ns, ok := s.table(t).Read(0xffffffffffffff05)
	if !ok || id != 0x1122334455667705 || ns != 42 {
		t.Fatalf("%x %d %v", id, ns, ok)
	}
}

func TestNewTableRejects(t *testing.T) {
	buf := newSlots(16).mem
	for _, m := range [][]byte{nil, buf[:8], buf[:48], buf[:20], buf[1:17]} {
		if _, err := NewTable(m, nil); err == nil {
			t.Errorf("%d bytes accepted", len(m))
		}
	}
}

// A slot whose time is 0 is being written: not read as anything.
func TestReadDuringAWrite(t *testing.T) {
	s := newSlots(4)
	s.words[2*1] = 5 // the id is in, the time not yet
	if _, _, ok := s.table(t).Read(5); ok {
		t.Fatal("read a slot without its time")
	}
	s.words[2*1], s.words[2*1+1] = 0, 9 // a time without an id does not occur either
	if _, _, ok := s.table(t).Read(5); ok {
		t.Fatal("read a time without an id")
	}
}

// A reader against a writer that keeps swapping two ids: never one's id with the other's time.
func TestReadIsConsistent(t *testing.T) {
	s := newSlots(1)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := uint64(1); ; i++ {
			select {
			case <-stop:
				return
			default:
				s.put(1+i%2, (1+i%2)<<32|i&0xffffffff)
			}
		}
	}()
	tab := s.table(t)
	read := 0
	for end := time.Now().Add(200 * time.Millisecond); time.Now().Before(end); {
		id, ns, ok := tab.Read(1)
		if ok && id != 0 {
			read++
			if ns>>32 != id {
				t.Fatalf("id %d with the time of %d", id, ns>>32)
			}
		}
	}
	close(stop)
	<-done
	if read == 0 {
		t.Fatal("nothing read")
	}
}

func TestClock(t *testing.T) {
	wall, mono := int64(1_000_000_000_000), int64(5_000)
	c := NewClock(func() (int64, int64) { return wall, mono })
	if c.Wall(6_000) != wall+1_000 || c.Mono(wall+1_000) != 6_000 {
		t.Fatal("conversion")
	}
	wall, mono = wall+int64(time.Second), mono+int64(time.Second)
	wall -= 7_000_000 // a step back
	c.Tick()
	if c.Wall(6_000) != 1_000_000_000_000+1_000-7_000_000 {
		t.Fatal("the step is not followed")
	}
	wall, mono = wall+int64(time.Second)+20, mono+int64(time.Second) // 20 ns of noise after it
	c.Tick()
	if d := c.TakeStep(); d != -7*time.Millisecond {
		t.Fatalf("the period's largest step: %v", d)
	}
	if d := c.TakeStep(); d != 0 {
		t.Fatalf("step %v after it was taken", d)
	}
}

func TestSampleClocks(t *testing.T) {
	u1, m1 := SampleClocks()
	time.Sleep(20 * time.Millisecond)
	u2, m2 := SampleClocks()
	if d := (u2 - u1) - (m2 - m1); d > int64(2*time.Millisecond) || d < -int64(2*time.Millisecond) {
		t.Fatalf("the clocks moved %v apart in 20 ms", time.Duration(d))
	}
	if d := time.Now().UnixNano() - u2; d < -int64(time.Second) || d > int64(time.Second) {
		t.Fatalf("the wall clock is %v off", time.Duration(d))
	}
}

func TestResolve(t *testing.T) {
	const off = int64(1_700_000_000_000_000_000) // wall - monotonic
	s := newSlots(256)
	r := &Resolver{Table: s.table(t), Clock: NewClock(func() (int64, int64) { return off, 0 })}
	s.put(0xa1, 1_000_000) // arrived at 1 ms
	s.put(0x0200, 3_000_000)
	s.words[2*0x33] = 0x33 // id without its time: being written
	for _, c := range []struct {
		name   string
		parent uint64
		start  int64 // monotonic
		want   Cause
		at     int64
	}{
		{"hit", 0xa1, 1_400_000, Hit, 1_000_000},
		{"a root has no parent", 0, 1_400_000, MissRoot, 1_400_000},
		{"a slot never used", 0xb2, 1_400_000, MissEmpty, 1_400_000},
		{"taken by a later arrival", 0xff00, 2_900_000, MissOverwritten, 2_900_000},
		{"an older id in the slot: never stored", 0xff00, 9_000_000, MissNotStored, 9_000_000},
		{"after the span's start", 0xa1, 900_000, MissLate, 900_000},
		{"being written", 0x33, 1_400_000, MissTorn, 1_400_000},
	} {
		r.Counts = [NumCauses]int{}
		at, cause := r.Arrival(c.parent, off+c.start)
		if cause != c.want || at != off+c.at {
			t.Errorf("%s: %s at %d, want %s at %d", c.name, CauseNames[cause], at-off, CauseNames[c.want], c.at)
		}
		if r.Counts[c.want] != 1 {
			t.Errorf("%s: counts %v", c.name, r.Counts)
		}
	}
	r.Take()
	if r.Table = nil; func() bool { _, c := r.Arrival(0xa1, off); return c != MissNoTable }() {
		t.Error("no table")
	}
	if n, _ := r.Take(); n[MissNoTable] != 1 || r.Counts != [NumCauses]int{} {
		t.Errorf("Take: %v, %v", n, r.Counts)
	}
}

func TestWaitSum(t *testing.T) {
	s := newSlots(16)
	r := &Resolver{Table: s.table(t), Clock: NewClock(func() (int64, int64) { return 0, 0 })}
	s.put(1, 100)
	s.put(2, 200)
	r.Arrival(1, 400)
	r.Arrival(2, 250)
	r.Arrival(3, 999) // a miss adds nothing
	if n, wait := r.Take(); wait != 350 || n[Hit] != 2 || n[MissEmpty] != 1 {
		t.Fatalf("%v %d", n, wait)
	}
}

func TestCounters(t *testing.T) {
	w := make([]uint64, NumCounters)
	w[CountIDs], w[NumCounters-1] = 7, 9
	c, err := NewCounters(unsafe.Slice((*byte)(unsafe.Pointer(&w[0])), NumCounters*8), nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := c.Read(); v[CountIDs] != 7 || v[NumCounters-1] != 9 || v[0] != 0 {
		t.Fatalf("%v", v)
	}
	if v := (*Counters)(nil).Read(); v != [NumCounters]uint64{} {
		t.Fatal("nil counters")
	}
	if _, err := NewCounters(make([]byte, 8), nil); err == nil {
		t.Fatal("too short accepted")
	}
	for i, n := range CounterNames {
		if n == "" {
			t.Errorf("counter %d has no name", i)
		}
	}
	for i, n := range CauseNames {
		if n == "" {
			t.Errorf("cause %d has no name", i)
		}
	}
}

func TestCloseOnce(t *testing.T) {
	n := 0
	tab, err := NewTable(newSlots(4).mem, func() error { n++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	_ = tab.Close()
	_ = tab.Close()
	_ = (*Table)(nil).Close()
	if n != 1 {
		t.Fatalf("closed %d times", n)
	}
}
