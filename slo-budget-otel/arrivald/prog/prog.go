// Package prog builds the arrival program (tc, at the pods' interfaces): for TCP payload to
// the services' port it scans for the key `"SpanID":"`, parses the 16 hexadecimal digits after
// it and stores (id, monotonic time) in the table the agent maps (../../arrival).  It is
// written with cilium/ebpf's assembler, so that no C toolchain is needed and the tests run the
// very instructions the loader loads (see vm.go and the replay of the captures).
//
// How it scans: a packet's payload is copied in chunks of Chunk positions (plus the bytes a key
// that starts at the chunk's last position needs) into a per-CPU buffer, because it is not
// linear in general.  A chunk is scanned four positions at a time without a branch per
// position: the eight bytes at each position, xor the key's first eight, are multiplied, and a
// product of 0 makes the four positions a candidate.  That keeps the verifier's walk inside its
// limits: 5.15.0-203 walks 812 036 of one million instructions (arrivald -check), about 5 300 of
// 8 192 pending branches; the walk grows with the number of chunks.  The candidates of a chunk
// are then checked against the whole key and parsed.
package prog

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/asm"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrival"
)

// Key is what the scan looks for; the IDLen digits of the id follow it.
const (
	Key    = `"SpanID":"`
	KeyLen = 10
	IDLen  = 16
)

var _ = [1]struct{}{}[len(Key)-KeyLen] // KeyLen is Key's

// The scan: Chunk positions per copy, at most MaxChunks of them per packet.
const (
	Chunk     = 256
	MaxChunks = 32
	ScanCap   = Chunk * MaxChunks // positions scanned in a packet at most
	need      = KeyLen + IDLen    // bytes of a key and its id
	loadLen   = Chunk + need - 1
	maxCand   = 4 // candidates (blocks of four positions) of one chunk
)

// The per-CPU buffer: the chunk, then the packet's time, whether it had an id, the chunk's
// candidates.
const (
	bufLen       = (loadLen + 7) / 8 * 8
	scratchNow   = bufLen
	scratchFound = scratchNow + 8
	scratchNCand = scratchFound + 4
	scratchCand  = scratchNCand + 4
	ScratchSize  = (scratchCand + 2*maxCand + 7) / 8 * 8
)

// __sk_buff (linux/bpf.h), the headers, and the return value that leaves the packet to the
// filters after this one.
const (
	skbLen      = 0
	skbProtocol = 16
	skbData     = 76
	skbDataEnd  = 80
	ethLen      = 14
	ethIPv4     = 0x0800
	ipTCP       = 6
	actUnspec   = -1
)

// Stack slots (from the frame pointer).
const (
	stKey = -4  // map key
	stIP  = -24 // the IPv4 header's first 20 bytes
	stTCP = -48 // the TCP header's
	stEnd = -56 // end of the payload in the packet
	stN   = -64 // bytes of the chunk that are valid
	stID  = -72 // the id being stored
	stLin = -80 // bytes of the packet in the skb's linear part
	stPos = -84 // the key's position in the chunk
)

// Config of a program.
type Config struct {
	Port  uint16 // the services' gRPC port
	Slots uint32 // slots of the table (a power of two)
}

// Maps are the file descriptors of the maps the program uses.
type Maps struct{ Table, Counters, Scratch int }

// Specs are the maps: the table and the counters (arrays the agent maps) and the buffer.
func Specs(c Config) (table, counters, scratch *ebpf.MapSpec) {
	const mmapable = 1 << 10
	return &ebpf.MapSpec{Name: arrival.PinTable, Type: ebpf.Array, KeySize: 4, ValueSize: arrival.SlotSize, MaxEntries: c.Slots, Flags: mmapable},
		&ebpf.MapSpec{Name: arrival.PinCounters, Type: ebpf.Array, KeySize: 4, ValueSize: arrival.NumCounters * 8, MaxEntries: 1, Flags: mmapable},
		&ebpf.MapSpec{Name: "scratch", Type: ebpf.PerCPUArray, KeySize: 4, ValueSize: ScratchSize, MaxEntries: 1}
}

// builder appends instructions; a label goes to the next one.
type builder struct {
	ins   asm.Instructions
	label string
	maps  Maps
}

func (b *builder) emit(ins ...asm.Instruction) {
	for _, i := range ins {
		if b.label != "" {
			i = i.WithSymbol(b.label)
			b.label = ""
		}
		b.ins = append(b.ins, i)
	}
}

func (b *builder) at(label string) {
	if b.label != "" { // two labels on one instruction: a jump between them
		b.emit(asm.Ja.Label(label))
	}
	b.label = label
}

// count adds one to a counter (r1, r2).
func (b *builder) count(i int) {
	b.emit(asm.LoadMapValue(asm.R1, b.maps.Counters, uint32(8*i)),
		asm.Mov.Imm(asm.R2, 1),
		asm.AddAtomic.Mem(asm.R1, asm.R2, asm.DWord, 0))
}

// countNegative adds one to a counter when r2 is negative (r1, r2); no branch, so that the
// verifier's walk does not grow.
func (b *builder) countNegative(i int) {
	b.emit(asm.LoadMapValue(asm.R1, b.maps.Counters, uint32(8*i)),
		asm.RSh.Imm(asm.R2, 63),
		asm.AddAtomic.Mem(asm.R1, asm.R2, asm.DWord, 0))
}

// loadBytes copies packet bytes (the offset in off) to a stack slot; failures go to fail.
func (b *builder) loadBytes(off asm.Instruction, slot int32, n int32, fail string) {
	b.emit(asm.Mov.Reg(asm.R1, asm.R6), off,
		asm.Mov.Reg(asm.R3, asm.R10), asm.Add.Imm(asm.R3, slot),
		asm.Mov.Imm(asm.R4, n), asm.FnSkbLoadBytes.Call(),
		asm.JNE.Imm(asm.R0, 0, fail))
}

// lookup finds the value of the key on the stack (stKey); r0 is the pointer or 0.
func (b *builder) lookup(fd int) {
	b.emit(asm.Mov.Reg(asm.R2, asm.R10), asm.Add.Imm(asm.R2, stKey),
		asm.LoadMapPtr(asm.R1, fd), asm.FnMapLookupElem.Call())
}

// hex converts the eight ASCII digits in x to their 32-bit number and ors what is wrong with
// them into bad (0: eight lower-case hexadecimal digits).  Clobbers r0, r1, r5.
func (b *builder) hex(x, bad asm.Register) {
	each := func(v byte) asm.Instruction { // a byte repeated, in r5
		return asm.LoadImm(asm.R5, int64(uint64(v)*0x0101010101010101), asm.DWord)
	}
	b.emit(
		// n: the low nibble, plus 9 for a letter (bit 6 set)
		each(0x01), asm.Mov.Reg(asm.R0, x), asm.RSh.Imm(asm.R0, 6), asm.And.Reg(asm.R0, asm.R5),
		asm.Mov.Reg(asm.R1, asm.R0), asm.LSh.Imm(asm.R1, 3), asm.Add.Reg(asm.R1, asm.R0),
		each(0x0f), asm.Mov.Reg(asm.R0, x), asm.And.Reg(asm.R0, asm.R5), asm.Add.Reg(asm.R0, asm.R1),
		// the digits of n (n + '0', plus 'a' - '0' - 10 from 10 on) must be x, and n below 16
		each(0x06), asm.Mov.Reg(asm.R1, asm.R0), asm.Add.Reg(asm.R1, asm.R5), asm.RSh.Imm(asm.R1, 4),
		each(0x01), asm.And.Reg(asm.R1, asm.R5), asm.Mul.Imm(asm.R1, 'a'-'0'-10), asm.Add.Reg(asm.R1, asm.R0),
		each('0'), asm.Add.Reg(asm.R1, asm.R5), asm.Xor.Reg(asm.R1, x), asm.Or.Reg(bad, asm.R1),
		each(0xf0), asm.Mov.Reg(asm.R1, asm.R0), asm.And.Reg(asm.R1, asm.R5), asm.Or.Reg(bad, asm.R1),
		// pack the eight nibbles: pairs to bytes, bytes to halves, halves to a word
		asm.Mov.Reg(x, asm.R0), asm.LSh.Imm(x, 4), asm.RSh.Imm(asm.R0, 8), asm.Or.Reg(x, asm.R0),
		asm.LoadImm(asm.R5, 0x00ff00ff00ff00ff, asm.DWord), asm.And.Reg(x, asm.R5),
		asm.Mov.Reg(asm.R0, x), asm.RSh.Imm(asm.R0, 8), asm.Or.Reg(x, asm.R0),
		asm.LoadImm(asm.R5, 0x0000ffff0000ffff, asm.DWord), asm.And.Reg(x, asm.R5),
		asm.Mov.Reg(asm.R0, x), asm.RSh.Imm(asm.R0, 16), asm.Or.Reg(x, asm.R0),
		asm.HostTo(asm.BE, x, asm.Word))
}

func be16(v uint16) int32 { return int32(v>>8 | v<<8) }

// Build is the program for these maps.
func Build(c Config, m Maps) (asm.Instructions, error) {
	if c.Slots == 0 || c.Slots&(c.Slots-1) != 0 || c.Slots > 1<<30 {
		return nil, fmt.Errorf("%d slots: a power of two up to 2^30", c.Slots)
	}
	if c.Port == 0 {
		return nil, errors.New("no service port")
	}
	if binary.NativeEndian.Uint16([]byte{1, 0}) != 1 {
		return nil, errors.New("the program is written for little-endian hosts")
	}
	b := &builder{maps: m}
	key8 := int64(binary.LittleEndian.Uint64([]byte(Key)))
	key2 := int32(binary.LittleEndian.Uint16([]byte(Key[8:])))

	// the headers: IPv4 without options or with, not a fragment, TCP to the service port, payload
	b.emit(asm.Mov.Reg(asm.R6, asm.R1),
		asm.LoadMem(asm.R2, asm.R6, skbProtocol, asm.Word), asm.JNE.Imm(asm.R2, be16(ethIPv4), "out"))
	b.loadBytes(asm.Mov.Imm(asm.R2, ethLen), stIP, 20, "out")
	b.emit(asm.LoadMem(asm.R2, asm.R10, stIP, asm.Byte),
		asm.Mov.Reg(asm.R3, asm.R2), asm.RSh.Imm(asm.R3, 4), asm.JNE.Imm(asm.R3, 4, "out"),
		asm.And.Imm(asm.R2, 15), asm.LSh.Imm(asm.R2, 2), asm.JLT.Imm(asm.R2, 20, "out"),
		asm.LoadMem(asm.R3, asm.R10, stIP+9, asm.Byte), asm.JNE.Imm(asm.R3, ipTCP, "out"),
		asm.LoadMem(asm.R3, asm.R10, stIP+6, asm.Half), asm.And.Imm(asm.R3, be16(0x3fff)), asm.JNE.Imm(asm.R3, 0, "out"),
		asm.LoadMem(asm.R3, asm.R10, stIP+2, asm.Half), asm.HostTo(asm.BE, asm.R3, asm.Half),
		asm.And.Imm(asm.R3, 0xffff), asm.Add.Imm(asm.R3, ethLen), // the swap leaves the verifier without bounds
		asm.StoreMem(asm.R10, stEnd, asm.R3, asm.DWord),
		asm.Mov.Reg(asm.R8, asm.R2), asm.Add.Imm(asm.R8, ethLen))
	b.loadBytes(asm.Mov.Reg(asm.R2, asm.R8), stTCP, 20, "out")
	b.emit(asm.LoadMem(asm.R3, asm.R10, stTCP+2, asm.Half), asm.JNE.Imm(asm.R3, be16(c.Port), "out"),
		asm.LoadMem(asm.R3, asm.R10, stTCP+12, asm.Byte), asm.RSh.Imm(asm.R3, 4), asm.LSh.Imm(asm.R3, 2),
		asm.JLT.Imm(asm.R3, 20, "out"), asm.Add.Reg(asm.R8, asm.R3),
		// the payload ends with the IP packet (a short frame is padded)
		asm.LoadMem(asm.R2, asm.R10, stEnd, asm.DWord), asm.LoadMem(asm.R3, asm.R6, skbLen, asm.Word),
		asm.JLE.Reg(asm.R2, asm.R3, "end"), asm.Mov.Reg(asm.R2, asm.R3))
	b.at("end") // masked: the two ways here then look alike, and what follows is verified once
	b.emit(asm.And.Imm(asm.R2, 0x1ffff),
		asm.StoreMem(asm.R10, stEnd, asm.R2, asm.DWord), asm.JGE.Reg(asm.R8, asm.R2, "out"),
		asm.StoreImm(asm.R10, stKey, 0, asm.Word))
	b.lookup(m.Scratch)
	b.emit(asm.JEq.Imm(asm.R0, 0, "out"), asm.Mov.Reg(asm.R7, asm.R0),
		asm.FnKtimeGetNs.Call(), asm.StoreMem(asm.R7, scratchNow, asm.R0, asm.DWord), // one time for the packet
		asm.StoreImm(asm.R7, scratchFound, 0, asm.Word))
	b.count(arrival.CountPackets)
	// the linear part: data_end - data (the rest of the packet is in page fragments, which
	// skb_load_bytes copies from all the same)
	b.emit(asm.LoadMem(asm.R2, asm.R6, skbDataEnd, asm.Word), asm.LoadMem(asm.R1, asm.R6, skbData, asm.Word),
		asm.Sub.Reg(asm.R2, asm.R1), asm.StoreMem(asm.R10, stLin, asm.R2, asm.DWord),
		asm.LoadMem(asm.R1, asm.R10, stEnd, asm.DWord), asm.Sub.Reg(asm.R2, asm.R1))
	b.countNegative(arrival.CountPaged)
	b.emit(asm.Mov.Imm(asm.R9, 0))

	// a chunk: r8 its offset in the packet, r9 its number
	b.at("chunk")
	b.emit(asm.LoadMem(asm.R4, asm.R10, stEnd, asm.DWord), asm.Sub.Reg(asm.R4, asm.R8),
		asm.JSLT.Imm(asm.R4, KeyLen, "done"),
		asm.JLE.Imm(asm.R4, loadLen, "copy"), asm.Mov.Imm(asm.R4, loadLen))
	b.at("copy")
	b.emit(asm.StoreMem(asm.R10, stN, asm.R4, asm.DWord),
		asm.Mov.Reg(asm.R1, asm.R6), asm.Mov.Reg(asm.R2, asm.R8), asm.Mov.Reg(asm.R3, asm.R7),
		asm.FnSkbLoadBytes.Call(), asm.JNE.Imm(asm.R0, 0, "unreadable"),
		asm.StoreImm(asm.R7, scratchNCand, 0, asm.Word),
		asm.LoadImm(asm.R5, key8, asm.DWord))
	for at := int16(0); at < Chunk; at += 4 {
		next := fmt.Sprintf("scan%d", at+4)
		if at%64 == 0 && at > 0 { // past the copy's end: what follows is an earlier chunk's
			b.emit(asm.LoadMem(asm.R1, asm.R10, stN, asm.DWord), asm.JLE.Imm(asm.R1, int32(at), "cand0"))
		}
		b.emit(asm.LoadMem(asm.R1, asm.R7, at, asm.DWord), asm.Xor.Reg(asm.R1, asm.R5),
			asm.LoadMem(asm.R2, asm.R7, at+1, asm.DWord), asm.Xor.Reg(asm.R2, asm.R5), asm.Mul.Reg(asm.R1, asm.R2),
			asm.LoadMem(asm.R2, asm.R7, at+2, asm.DWord), asm.Xor.Reg(asm.R2, asm.R5),
			asm.LoadMem(asm.R3, asm.R7, at+3, asm.DWord), asm.Xor.Reg(asm.R3, asm.R5), asm.Mul.Reg(asm.R2, asm.R3),
			asm.Mul.Reg(asm.R1, asm.R2), asm.JNE.Imm(asm.R1, 0, next),
			// a candidate: the first maxCand positions are listed
			asm.LoadMem(asm.R2, asm.R7, scratchNCand, asm.Word),
			asm.Mov.Reg(asm.R3, asm.R2), asm.Add.Imm(asm.R3, 1), asm.StoreMem(asm.R7, scratchNCand, asm.R3, asm.Word),
			asm.JGE.Imm(asm.R2, maxCand, next), asm.LSh.Imm(asm.R2, 1),
			asm.Mov.Reg(asm.R4, asm.R7), asm.Add.Reg(asm.R4, asm.R2),
			asm.StoreImm(asm.R4, scratchCand, int64(at), asm.Half))
		b.at(next)
	}

	// the chunk's candidates
	for i := 0; i < maxCand; i++ {
		l := func(s string) string { return fmt.Sprintf("%s%d", s, i) }
		next := fmt.Sprintf("cand%d", i+1)
		b.at(fmt.Sprintf("cand%d", i))
		b.emit(asm.LoadMem(asm.R2, asm.R7, scratchNCand, asm.Word), asm.JLE.Imm(asm.R2, int32(i), "next"),
			asm.LoadMem(asm.R1, asm.R7, int16(scratchCand+2*i), asm.Half), asm.And.Imm(asm.R1, Chunk-4),
			asm.Mov.Reg(asm.R2, asm.R7), asm.Add.Reg(asm.R2, asm.R1), asm.LoadImm(asm.R5, key8, asm.DWord))
		for j := 0; j < 4; j++ { // which of the four positions has the key
			no := fmt.Sprintf("no%d_%d", i, j)
			b.emit(asm.LoadMem(asm.R3, asm.R2, int16(j), asm.DWord), asm.JNE.Reg(asm.R3, asm.R5, no),
				asm.LoadMem(asm.R3, asm.R2, int16(j+8), asm.Half), asm.JNE.Imm(asm.R3, key2, no),
				asm.Add.Imm(asm.R1, int32(j)), asm.Ja.Label(l("key")))
			b.at(no)
		}
		b.emit(asm.Ja.Label(next))

		b.at(l("key")) // r1: the key's position in the chunk
		b.emit(asm.LoadMem(asm.R4, asm.R10, stN, asm.DWord),
			asm.Mov.Reg(asm.R3, asm.R1), asm.Add.Imm(asm.R3, need), asm.JGT.Reg(asm.R3, asm.R4, l("short")),
			asm.StoreMem(asm.R10, stPos, asm.R1, asm.Word),
			asm.Mov.Reg(asm.R2, asm.R7), asm.Add.Reg(asm.R2, asm.R1),
			asm.LoadMem(asm.R3, asm.R2, KeyLen, asm.DWord), asm.LoadMem(asm.R4, asm.R2, KeyLen+8, asm.DWord),
			asm.Mov.Imm(asm.R2, 0))
		b.hex(asm.R3, asm.R2)
		b.hex(asm.R4, asm.R2)
		b.emit(asm.JNE.Imm(asm.R2, 0, l("bad")),
			asm.LSh.Imm(asm.R3, 32), asm.Or.Reg(asm.R3, asm.R4), asm.JEq.Imm(asm.R3, 0, l("bad")),
			asm.StoreMem(asm.R10, stID, asm.R3, asm.DWord),
			asm.And.Imm(asm.R3, int32(c.Slots-1)), asm.StoreMem(asm.R10, stKey, asm.R3, asm.Word))
		b.lookup(m.Table)
		b.emit(asm.JEq.Imm(asm.R0, 0, l("bad")),
			asm.LoadMem(asm.R3, asm.R0, arrival.OffID, asm.DWord), asm.LoadMem(asm.R4, asm.R10, stID, asm.DWord),
			asm.JEq.Reg(asm.R3, asm.R4, l("same")), asm.JEq.Imm(asm.R3, 0, l("free")))
		b.count(arrival.CountOverwritten)
		b.at(l("free")) // time 0, the id, the time: a reader that sees the id sees its time or 0
		b.emit(asm.Mov.Imm(asm.R1, 0), asm.StoreMem(asm.R0, arrival.OffTime, asm.R1, asm.DWord),
			asm.StoreMem(asm.R0, arrival.OffID, asm.R4, asm.DWord),
			asm.LoadMem(asm.R1, asm.R7, scratchNow, asm.DWord),
			asm.StoreMem(asm.R0, arrival.OffTime, asm.R1, asm.DWord),
			asm.Ja.Label(l("stored")))
		b.at(l("same")) // a retransmission: the first time stays
		b.count(arrival.CountRepeats)
		b.at(l("stored"))
		b.emit(asm.LoadMem(asm.R2, asm.R10, stLin, asm.DWord), asm.LoadMem(asm.R1, asm.R10, stPos, asm.Word),
			asm.Sub.Reg(asm.R2, asm.R1), asm.Sub.Reg(asm.R2, asm.R8), asm.Sub.Imm(asm.R2, need))
		b.countNegative(arrival.CountPagedIDs)
		b.count(arrival.CountIDs)
		b.emit(asm.StoreImm(asm.R7, scratchFound, 1, asm.Word), asm.Ja.Label(next))
		b.at(l("bad"))
		b.count(arrival.CountBadID)
		b.emit(asm.Ja.Label(next))
		b.at(l("short")) // the packet ends inside the id (or, in stale bytes, inside the key)
		b.emit(asm.Mov.Reg(asm.R3, asm.R1), asm.Add.Imm(asm.R3, KeyLen), asm.JGT.Reg(asm.R3, asm.R4, next))
		b.count(arrival.CountCut)
	}
	b.at(fmt.Sprintf("cand%d", maxCand))
	b.emit(asm.LoadMem(asm.R2, asm.R7, scratchNCand, asm.Word), asm.JLE.Imm(asm.R2, maxCand, "next"))
	b.count(arrival.CountCrowded) // (leftovers of an earlier packet behind the copy's end count too)

	b.at("next")
	b.emit(asm.Add.Imm(asm.R8, Chunk), asm.Add.Imm(asm.R9, 1), asm.JLT.Imm(asm.R9, MaxChunks, "chunk"),
		asm.LoadMem(asm.R4, asm.R10, stEnd, asm.DWord), asm.Sub.Reg(asm.R4, asm.R8),
		asm.JSLT.Imm(asm.R4, KeyLen, "done"))
	b.count(arrival.CountCapped)
	b.emit(asm.Ja.Label("done"))
	b.at("unreadable")
	b.count(arrival.CountErrors)
	b.at("done")
	b.emit(asm.LoadMem(asm.R1, asm.R7, scratchFound, asm.Word), asm.JNE.Imm(asm.R1, 0, "out"))
	b.count(arrival.CountNoID)
	b.at("out")
	b.emit(asm.Mov.Imm(asm.R0, actUnspec), asm.Return())
	return b.ins, nil
}

// Spec is the program as the loader loads it.
func Spec(c Config, m Maps) (*ebpf.ProgramSpec, error) {
	ins, err := Build(c, m)
	if err != nil {
		return nil, err
	}
	return &ebpf.ProgramSpec{Name: "slo_arrival", Type: ebpf.SchedCLS, License: "GPL", Instructions: ins}, nil
}
