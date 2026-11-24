package prog_test

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/cilium/ebpf"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrival"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrivald/prog"
)

// The program in the kernel (its verifier, then BPF_PROG_TEST_RUN: nothing is attached) against
// the interpreter, packet by packet.  Root only, and only when asked for: ARRIVAL_KERNEL=1.
func TestKernelAgreesWithInterpreter(t *testing.T) {
	if os.Getenv("ARRIVAL_KERNEL") == "" {
		t.Skip("ARRIVAL_KERNEL is not set")
	}
	const slots = 256
	cfg := prog.Config{Port: port, Slots: slots}
	ts, cs, ss := prog.Specs(cfg)
	var maps [3]*ebpf.Map
	// pinned as the loader pins them, and read through the agent's mapping
	dir, err := os.MkdirTemp("/sys/fs/bpf", "slobudget-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(dir)
	for i, s := range []*ebpf.MapSpec{ts, cs, ss} {
		m, err := ebpf.NewMap(s)
		if err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		maps[i] = m
		if i < 2 {
			p := filepath.Join(dir, s.Name)
			if err := m.Pin(p); err != nil {
				t.Fatal(err)
			}
			defer os.Remove(p)
		}
	}
	table, err := arrival.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	counters, err := arrival.OpenCounters(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer counters.Close()
	spec, err := prog.Spec(cfg, prog.Maps{Table: maps[0].FD(), Counters: maps[1].FD(), Scratch: maps[2].FD()})
	if err != nil {
		t.Fatal(err)
	}
	p, err := ebpf.NewProgramWithOptions(spec, ebpf.ProgramOptions{LogLevel: ebpf.LogLevelStats})
	if err != nil {
		var ve *ebpf.VerifierError
		if errors.As(err, &ve) {
			t.Fatalf("the verifier: %+v", ve)
		}
		t.Fatal(err)
	}
	defer p.Close()
	t.Logf("verifier: %s", p.VerifierLog)

	b := newBench(t, slots)
	rng := rand.New(rand.NewSource(9))
	for i := 0; i < 2000; i++ {
		var m []byte
		for k := rng.Intn(8); k >= 0 && len(m) < 2600; k-- {
			switch rng.Intn(8) {
			case 0:
				m = append(m, prog.Key+"nothexdigits1234"...)
			case 1:
				m = append(m, `"SpanID""SpanID`...)
			default:
				m = append(m, message(rng.Intn(400), uint64(1+rng.Intn(900)))...)
			}
		}
		m = m[:max(1, len(m)-rng.Intn(80))]
		f := frame(m, opts{tcpOpts: 4 * rng.Intn(11), ipOpts: 4 * rng.Intn(3), pad: rng.Intn(4)})
		if len(f) > 3200 { // BPF_PROG_TEST_RUN takes less than a page
			f = f[:3200]
		}
		if rng.Intn(12) == 0 {
			f = frame(m, opts{dport: 2000})
			f = f[:min(3200, len(f))]
		}
		b.run(f, uint64(i+1))
		ret, err := p.Run(&ebpf.RunOptions{Data: f})
		if err != nil {
			t.Fatal(err)
		}
		if int32(ret) != -1 {
			t.Fatalf("packet %d: return value %d", i, int32(ret))
		}
		var kc [arrival.NumCounters]uint64
		if err := maps[1].Lookup(uint32(0), &kc); err != nil {
			t.Fatal(err)
		}
		if kc != b.ctr.Read() || counters.Read() != kc {
			t.Fatalf("packet %d: the kernel counts %v (mapped %v), the interpreter %v", i, kc, counters.Read(), b.ctr.Read())
		}
	}
	for s := uint32(0); s < slots; s++ {
		var v [2]uint64
		if err := maps[0].Lookup(s, &v); err != nil {
			t.Fatal(err)
		}
		id, ns, _ := b.table.Read(uint64(s))
		if v[0] != id || (v[1] == 0) != (ns == 0) {
			t.Fatalf("slot %d: the kernel holds %016x, the interpreter %016x", s, v[0], id)
		}
		if got, at, ok := table.Read(uint64(s)); !ok || got != v[0] || at != v[1] {
			t.Fatalf("slot %d: mapped %016x at %d, the map holds %016x at %d", s, got, at, v[0], v[1])
		}
	}
}
