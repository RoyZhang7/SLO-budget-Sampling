// Package vm runs the arrival program's instructions without a kernel: the encoded program, as
// the loader would load it, one packet at a time.  It covers the instructions and helpers the
// program uses, and stops with an error at what the kernel's verifier exists for: a register
// or stack byte read before it was written, an access outside the stack, the context or a map
// value, a program that does not end.
package vm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"runtime"
	"unsafe"

	"github.com/cilium/ebpf/asm"
)

// Map is an array (one value per entry) or a per-CPU array of one CPU.
type Map struct {
	ValueSize, Entries int
	words              []uint64 // 8-byte aligned, as the kernel's
}

// NewMap allocates a zeroed map.
func NewMap(valueSize, entries int) *Map {
	return &Map{ValueSize: valueSize, Entries: entries, words: make([]uint64, (valueSize*entries+7)/8)}
}

// Bytes is the map's memory.
func (m *Map) Bytes() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&m.words[0])), len(m.words)*8)[:m.ValueSize*m.Entries]
}

// Addresses: a region number in the high half, the offset in the low half.
const (
	regionCtx   = 1
	regionStack = 2
	regionMap   = 16                 // + file descriptor: the values
	mapHandle   = uint64(0xbf) << 56 // | file descriptor: the map itself
	stackSize   = 512
	ctxSize     = 192
	maxSteps    = 1 << 20
)

type insn struct {
	op       byte
	dst, src uint8
	off      int16
	imm      int32
	imm64    uint64 // of a 16-byte load
	wide     bool   // the first half of it
}

// VM is a program with its maps.
type VM struct {
	ins  []insn
	maps map[int]*Map

	reg       [11]uint64
	regOK     [11]bool
	stack     [stackSize]byte
	stackOK   [stackSize]bool
	ctx       [ctxSize]byte
	skb       []byte
	now       uint64
	seen      []bool // instructions ever executed (coverage)
	MaxSteps  int    // most instructions a packet took
	TotalRuns int
}

// New encodes the program (jumps resolved) for these maps, keyed by file descriptor.
func New(ins asm.Instructions, maps map[int]*Map) (*VM, error) {
	raw, err := encode(ins)
	if err != nil {
		return nil, err
	}
	v := &VM{maps: maps}
	for i := 0; i < len(raw); i += 8 {
		x := insn{op: raw[i], dst: raw[i+1] & 15, src: raw[i+1] >> 4,
			off: int16(binary.LittleEndian.Uint16(raw[i+2:])), imm: int32(binary.LittleEndian.Uint32(raw[i+4:]))}
		if x.op == 0x18 { // 16-byte load: the second half follows
			x.wide = true
			hi := binary.LittleEndian.Uint32(raw[i+12:])
			x.imm64 = uint64(uint32(x.imm)) | uint64(hi)<<32
		}
		v.ins = append(v.ins, x)
	}
	v.seen = make([]bool, len(v.ins))
	return v, nil
}

// encode is ins.Marshal, also on systems where cilium/ebpf refuses Linux's helpers (the tests
// run on Windows): their call instructions are encoded here.
func encode(ins asm.Instructions) ([]byte, error) {
	if runtime.GOOS == "linux" {
		var buf bytes.Buffer
		err := ins.Marshal(&buf, binary.LittleEndian)
		return buf.Bytes(), err
	}
	ins = append(asm.Instructions{}, ins...)
	_ = ins.Marshal(io.Discard, binary.LittleEndian) // resolves the jumps before it stops at a call
	var buf bytes.Buffer
	for i, x := range ins {
		if x.OpCode.Class().IsJump() && x.Reference() != "" && x.Offset == -1 {
			return nil, fmt.Errorf("instruction %d: no label %q", i, x.Reference())
		}
		if x.IsBuiltinCall() {
			buf.Write(binary.LittleEndian.AppendUint32([]byte{0x85, 0, 0, 0}, uint32(x.Constant)))
		} else if _, err := x.Marshal(&buf, binary.LittleEndian); err != nil {
			return nil, fmt.Errorf("instruction %d: %w", i, err)
		}
	}
	return buf.Bytes(), nil
}

// Len is the number of 8-byte instructions; Unreached are those no packet executed.
func (v *VM) Len() int { return len(v.ins) }

// Unreached lists the instructions never executed (the second half of a wide load left out).
func (v *VM) Unreached() (out []int) {
	for i := 0; i < len(v.ins); i++ {
		if !v.seen[i] {
			out = append(out, i)
		}
		if v.ins[i].wide {
			i++
		}
	}
	return out
}

// mem resolves n bytes at an address; write says which check applies to the stack.
func (v *VM) mem(addr uint64, n int, write bool) ([]byte, error) {
	region, off := addr>>32, int64(uint32(addr))
	if int32(uint32(addr)) < 0 { // a negative offset shows up in the region below
		return nil, fmt.Errorf("address %#x out of range", addr)
	}
	switch {
	case region == regionStack:
		if off < 0 || off+int64(n) > stackSize {
			return nil, fmt.Errorf("stack offset %d, %d bytes", off-stackSize, n)
		}
		if !write {
			for i := off; i < off+int64(n); i++ {
				if !v.stackOK[i] {
					return nil, fmt.Errorf("stack byte %d read before it was written", i-stackSize)
				}
			}
		}
		return v.stack[off : off+int64(n)], nil
	case region == regionCtx:
		if write || off+int64(n) > ctxSize || off%int64(n) != 0 {
			return nil, fmt.Errorf("context offset %d, %d bytes", off, n)
		}
		return v.ctx[off : off+int64(n)], nil
	case region >= regionMap:
		m := v.maps[int(region-regionMap)]
		if m == nil {
			return nil, fmt.Errorf("address %#x: no such map", addr)
		}
		if off < 0 || off+int64(n) > int64(m.ValueSize*m.Entries) || off/int64(m.ValueSize) != (off+int64(n)-1)/int64(m.ValueSize) {
			return nil, fmt.Errorf("map value offset %d, %d bytes, values of %d", off, n, m.ValueSize)
		}
		return m.Bytes()[off : off+int64(n)], nil
	}
	return nil, fmt.Errorf("address %#x is not memory", addr)
}

func (v *VM) written(addr uint64, n int) {
	if addr>>32 == regionStack {
		for i := 0; i < n; i++ {
			v.stackOK[uint32(addr)+uint32(i)] = true
		}
	}
}

func size(op byte) int { return [4]int{4, 2, 1, 8}[(op>>3)&3] }

func get(b []byte) uint64 {
	switch len(b) {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	}
	return binary.LittleEndian.Uint64(b)
}

func put(b []byte, x uint64) {
	switch len(b) {
	case 1:
		b[0] = byte(x)
	case 2:
		binary.LittleEndian.PutUint16(b, uint16(x))
	case 4:
		binary.LittleEndian.PutUint32(b, uint32(x))
	default:
		binary.LittleEndian.PutUint64(b, x)
	}
}

// Run executes the program on a packet (an Ethernet frame with this EtherType) at a monotonic time.
func (v *VM) Run(frame []byte, etherType uint16, now uint64) (ret int32, steps int, err error) {
	v.reg, v.regOK, v.stackOK, v.ctx = [11]uint64{}, [11]bool{}, [stackSize]bool{}, [ctxSize]byte{}
	v.reg[1], v.regOK[1] = regionCtx<<32, true
	v.reg[10], v.regOK[10] = regionStack<<32|stackSize, true
	binary.LittleEndian.PutUint32(v.ctx[0:], uint32(len(frame)))
	binary.BigEndian.PutUint16(v.ctx[16:], etherType) // __be16 in the low half of the word
	v.skb, v.now = frame, now
	v.TotalRuns++
	pc := 0
	for ; ; steps++ {
		if pc < 0 || pc >= len(v.ins) || steps > maxSteps {
			return 0, steps, fmt.Errorf("instruction %d after %d steps", pc, steps)
		}
		v.seen[pc] = true
		next, done, e := v.step(pc)
		if e != nil {
			return 0, steps, fmt.Errorf("instruction %d (%+v): %w", pc, v.ins[pc], e)
		}
		if done {
			break
		}
		pc = next
	}
	if !v.regOK[0] {
		return 0, steps, fmt.Errorf("no return value")
	}
	if steps > v.MaxSteps {
		v.MaxSteps = steps
	}
	return int32(uint32(v.reg[0])), steps, nil
}

func (v *VM) val(r uint8) (uint64, error) {
	if r > 10 || !v.regOK[r] {
		return 0, fmt.Errorf("r%d read before it was written", r)
	}
	return v.reg[r], nil
}

func (v *VM) set(r uint8, x uint64) error {
	if r > 9 {
		return fmt.Errorf("write to r%d", r)
	}
	v.reg[r], v.regOK[r] = x, true
	return nil
}

func (v *VM) step(pc int) (next int, done bool, err error) {
	in := &v.ins[pc]
	next = pc + 1
	switch class := in.op & 7; class {
	case 0: // LD: 16-byte immediate, a map or a map value
		if !in.wide {
			return 0, false, fmt.Errorf("unknown load %#x", in.op)
		}
		x := in.imm64
		switch in.src {
		case 1:
			x = mapHandle | uint64(uint32(in.imm))
		case 2:
			x = uint64(regionMap+uint32(in.imm))<<32 | in.imm64>>32
		}
		return pc + 2, false, v.set(in.dst, x)
	case 1: // LDX
		a, e := v.val(in.src)
		if e != nil {
			return 0, false, e
		}
		if in.op>>5 != 3 {
			return 0, false, fmt.Errorf("unknown load %#x", in.op)
		}
		b, e := v.mem(a+uint64(int64(in.off)), size(in.op), false)
		if e != nil {
			return 0, false, e
		}
		return next, false, v.set(in.dst, get(b))
	case 2, 3: // ST, STX
		a, e := v.val(in.dst)
		if e != nil {
			return 0, false, e
		}
		x := uint64(int64(in.imm))
		if class == 3 {
			if x, e = v.val(in.src); e != nil {
				return 0, false, e
			}
		}
		a += uint64(int64(in.off))
		n := size(in.op)
		switch in.op >> 5 {
		case 3:
			b, e := v.mem(a, n, true)
			if e != nil {
				return 0, false, e
			}
			put(b, x)
			v.written(a, n)
		case 6: // atomic
			if class != 3 || in.imm != 0 || n < 4 {
				return 0, false, fmt.Errorf("unknown atomic operation %#x", in.imm)
			}
			b, e := v.mem(a, n, false)
			if e != nil {
				return 0, false, e
			}
			put(b, get(b)+x)
		default:
			return 0, false, fmt.Errorf("unknown store %#x", in.op)
		}
		return next, false, nil
	case 4, 7:
		return next, false, v.alu(in, class == 7)
	}
	return v.jump(in, pc)
}

func (v *VM) alu(in *insn, wide bool) error {
	op := in.op >> 4
	if op == 0xd { // byte order: to big endian swaps, to little endian truncates
		x, e := v.val(in.dst)
		if e != nil {
			return e
		}
		be := in.op&8 != 0
		switch in.imm {
		case 16:
			x &= 0xffff
			if be {
				x = uint64(bits.ReverseBytes16(uint16(x)))
			}
		case 32:
			x &= 0xffffffff
			if be {
				x = uint64(bits.ReverseBytes32(uint32(x)))
			}
		case 64:
			if be {
				x = bits.ReverseBytes64(x)
			}
		default:
			return fmt.Errorf("byte order of %d bits", in.imm)
		}
		return v.set(in.dst, x)
	}
	b := uint64(int64(in.imm))
	if in.op&8 != 0 {
		var e error
		if b, e = v.val(in.src); e != nil {
			return e
		}
	}
	var a uint64
	if op != 0xb {
		var e error
		if a, e = v.val(in.dst); e != nil {
			return e
		}
	}
	mask := uint64(63)
	if !wide {
		a, b, mask = uint64(uint32(a)), uint64(uint32(b)), 31
	}
	switch op {
	case 0x0:
		a += b
	case 0x1:
		a -= b
	case 0x2:
		a *= b
	case 0x4:
		a |= b
	case 0x5:
		a &= b
	case 0x6:
		a <<= b & mask
	case 0x7:
		a >>= b & mask
	case 0x8:
		a = -a
	case 0xa:
		a ^= b
	case 0xb:
		a = b
	default:
		return fmt.Errorf("unknown arithmetic %#x", in.op)
	}
	if !wide {
		a = uint64(uint32(a))
	}
	return v.set(in.dst, a)
}

func (v *VM) jump(in *insn, pc int) (int, bool, error) {
	op, wide := in.op>>4, in.op&7 == 5
	switch {
	case in.op == 0x05:
		return pc + 1 + int(in.off), false, nil
	case in.op == 0x85:
		return pc + 1, false, v.call(in.imm)
	case in.op == 0x95:
		return 0, true, nil
	}
	a, e := v.val(in.dst)
	if e != nil {
		return 0, false, e
	}
	b := uint64(int64(in.imm))
	if in.op&8 != 0 {
		if b, e = v.val(in.src); e != nil {
			return 0, false, e
		}
	}
	sa, sb := int64(a), int64(b)
	if !wide {
		a, b = uint64(uint32(a)), uint64(uint32(b))
		sa, sb = int64(int32(a)), int64(int32(b))
	}
	var take bool
	switch op {
	case 0x1:
		take = a == b
	case 0x2:
		take = a > b
	case 0x3:
		take = a >= b
	case 0x4:
		take = a&b != 0
	case 0x5:
		take = a != b
	case 0x6:
		take = sa > sb
	case 0x7:
		take = sa >= sb
	case 0xa:
		take = a < b
	case 0xb:
		take = a <= b
	case 0xc:
		take = sa < sb
	case 0xd:
		take = sa <= sb
	default:
		return 0, false, fmt.Errorf("unknown jump %#x", in.op)
	}
	if take {
		return pc + 1 + int(in.off), false, nil
	}
	return pc + 1, false, nil
}

// The helpers (linux/bpf.h numbers).
const (
	fnMapLookup    = 1
	fnKtimeGetNs   = 5
	fnSkbLoadBytes = 26
)

func (v *VM) call(fn int32) error {
	args := map[int32]uint8{fnMapLookup: 2, fnKtimeGetNs: 0, fnSkbLoadBytes: 4}
	n, ok := args[fn]
	if !ok {
		return fmt.Errorf("unknown helper %d", fn)
	}
	var a [6]uint64
	for r := uint8(1); r <= n; r++ {
		x, e := v.val(r)
		if e != nil {
			return e
		}
		a[r] = x
	}
	for r := 1; r <= 5; r++ {
		v.regOK[r] = false
	}
	switch fn {
	case fnKtimeGetNs:
		return v.set(0, v.now)
	case fnMapLookup:
		fd := int(uint32(a[1]))
		m := v.maps[fd]
		if a[1]&mapHandle != mapHandle || m == nil {
			return fmt.Errorf("lookup in %#x", a[1])
		}
		k, e := v.mem(a[2], 4, false)
		if e != nil {
			return e
		}
		if i := get(k); i < uint64(m.Entries) {
			return v.set(0, uint64(regionMap+fd)<<32|i*uint64(m.ValueSize))
		}
		return v.set(0, 0)
	}
	if a[1] != regionCtx<<32 {
		return fmt.Errorf("skb_load_bytes of %#x", a[1])
	}
	if a[4] == 0 || a[4] > 1<<20 {
		return fmt.Errorf("skb_load_bytes of %d bytes", int64(a[4]))
	}
	to, e := v.mem(a[3], int(a[4]), true)
	if e != nil {
		return e
	}
	v.written(a[3], int(a[4]))
	if a[2] > 0xffff || a[2]+a[4] > uint64(len(v.skb)) { // -EFAULT, the buffer zeroed
		clear(to)
		return v.set(0, ^uint64(13))
	}
	copy(to, v.skb[a[2]:])
	return v.set(0, 0)
}
