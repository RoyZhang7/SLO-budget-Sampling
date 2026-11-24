// Package arrival is the collector side of the arrival stamp: the table the eBPF program of
// ../arrivald fills at the pods' interfaces, read through mmap, the conversion of its
// monotonic times to the spans' clock, and the lookup of a server span's arrival by its
// parent span id, with the misses counted by cause.
package arrival

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// The layout the program and the reader share.  The table is an array of SlotSize-byte slots,
// a power of two of them; an id's slot is its low bits (the id as a big-endian number).
// A slot is (id, time): the program stores time 0, then the id, then the time.
const (
	SlotSize = 16
	OffID    = 0
	OffTime  = 8
)

// Counters of the program, in the order of the counter array.
const (
	CountPackets     = iota // packets to the service port with TCP payload (scanned)
	CountIDs                // ids found
	CountNoID               // packets with payload but no id
	CountOverwritten        // slots taken from another id
	CountCapped             // packets longer than the scan cap
	CountCut                // keys whose id runs past the packet's end (split between packets)
	CountBadID              // keys not followed by 16 lower-case hexadecimal digits
	CountRepeats            // ids already in their slot (the first time is kept)
	CountCrowded            // chunks with more candidates (the key's first bytes) than the program keeps
	CountErrors             // helper failures (payload not readable)
	NumCounters
)

// CounterNames are the names of the counters in logs and reports.
var CounterNames = [NumCounters]string{"packets", "ids", "no_id", "overwritten", "capped", "cut", "bad_id", "repeats", "crowded", "errors"}

// Index is the slot of an id.
func Index(id uint64, slots uint32) uint32 { return uint32(id) & (slots - 1) }

// Table reads the slots (the mapped array, or the memory of a test).
type Table struct {
	mem   []byte
	slots uint32
	close func() error
}

// NewTable wraps the memory of an array of slots.
func NewTable(mem []byte, closer func() error) (*Table, error) {
	n := len(mem) / SlotSize
	if n == 0 || n&(n-1) != 0 || len(mem)%SlotSize != 0 {
		return nil, fmt.Errorf("arrival table: %d bytes are not a power of two of %d-byte slots", len(mem), SlotSize)
	}
	if uintptr(unsafe.Pointer(&mem[0]))%8 != 0 {
		return nil, errors.New("arrival table: memory not aligned to 8 bytes")
	}
	return &Table{mem: mem, slots: uint32(n), close: closer}, nil
}

// Slots is the number of slots.
func (t *Table) Slots() uint32 { return t.slots }

// Close unmaps the table.
func (t *Table) Close() error {
	if t == nil || t.close == nil {
		return nil
	}
	c := t.close
	t.close = nil
	return c()
}

func load(mem []byte, off uintptr) uint64 {
	return atomic.LoadUint64((*uint64)(unsafe.Pointer(&mem[off])))
}

// Read returns the id and the time in the slot of id.  ok is false when the program kept
// writing the slot (its time is 0 while it does, and the three reads must agree).
func (t *Table) Read(id uint64) (got, ns uint64, ok bool) {
	off := uintptr(Index(id, t.slots)) * SlotSize
	for try := 0; try < 4; try++ {
		n1 := load(t.mem, off+OffTime)
		got = load(t.mem, off+OffID)
		n2 := load(t.mem, off+OffTime)
		if n1 == n2 && (n1 != 0) == (got != 0) {
			return got, n1, true
		}
	}
	return 0, 0, false
}
