// Command arrivald loads the arrival program (prog) and keeps it at the ingress of every
// service pod's interface on its node (tc, in the pod's network namespace).  The table and the
// counters are pinned for the agent, which maps them (../arrival).  Service pods are the
// network namespaces with a listener on the services' port; new ones are found by polling
// /proc.  It needs the host's process ids, CAP_SYS_ADMIN, CAP_NET_ADMIN and CAP_BPF
// (render.py --arrival: privileged), and the bpf filesystem.  On SIGTERM it takes the program
// off every interface and removes the pins; arrivald -clean does that for one that could not,
// arrivald -dump writes what the table holds.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/rlimit"
	"golang.org/x/sys/unix"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrival"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrivald/prog"
)

const version = "arrivald 2"

type options struct {
	port, slots         uint
	pid                 int
	iface, pin          string
	poll, print, verify time.Duration
	stats               bool
}

// loaded is the program with its maps.
type loaded struct {
	prog                     *ebpf.Program
	id                       int
	table, counters, scratch *ebpf.Map
	pins                     map[string]*ebpf.Map // path -> what we pinned there
}

func (l *loaded) Close() {
	for _, c := range []interface{ Close() error }{l.prog, l.table, l.counters, l.scratch} {
		_ = c.Close()
	}
}

// unpin removes our pins (a loader that started after us has replaced them by its own).
func (l *loaded) unpin(dir string) {
	for path, m := range l.pins {
		p, err := ebpf.LoadPinnedMap(path, nil)
		if err != nil {
			continue
		}
		if sameMap(p, m) {
			_ = os.Remove(path)
		}
		p.Close()
	}
	_ = os.Remove(dir) // when it is empty
}

func sameMap(a, b *ebpf.Map) bool {
	ai, err1 := a.Info()
	bi, err2 := b.Info()
	if err1 != nil || err2 != nil {
		return false
	}
	x, ok1 := ai.ID()
	y, ok2 := bi.ID()
	return ok1 && ok2 && x == y
}

// pin creates a map and pins it, in place of an earlier one.
func (l *loaded) pin(spec *ebpf.MapSpec, dir string) (*ebpf.Map, error) {
	path := filepath.Join(dir, spec.Name)
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	m, err := ebpf.NewMap(spec)
	if err != nil {
		return nil, fmt.Errorf("map %s: %w", spec.Name, err)
	}
	if err := m.Pin(path); err != nil {
		m.Close()
		return nil, fmt.Errorf("pin %s: %w", path, err)
	}
	l.pins[path] = m
	return m, nil
}

func verifier(err error) error {
	var ve *ebpf.VerifierError
	if errors.As(err, &ve) {
		return fmt.Errorf("the verifier: %+v", ve)
	}
	return fmt.Errorf("program: %w", err)
}

// load fills l; what it created stays in l when it fails.
func (l *loaded) load(o options) error {
	if err := rlimit.RemoveMemlock(); err != nil {
		return err
	}
	if err := os.MkdirAll(o.pin, 0o700); err != nil {
		return err
	}
	c := prog.Config{Port: uint16(o.port), Slots: uint32(o.slots)}
	ts, cs, ss := prog.Specs(c)
	var err error
	if l.scratch, err = ebpf.NewMap(ss); err != nil {
		return fmt.Errorf("map scratch: %w", err)
	}
	// the counters first: the agent opens the table, and the counters must be there by then
	if l.counters, err = l.pin(cs, o.pin); err != nil {
		return err
	}
	if l.table, err = l.pin(ts, o.pin); err != nil {
		return err
	}
	spec, err := prog.Spec(c, prog.Maps{Table: l.table.FD(), Counters: l.counters.FD(), Scratch: l.scratch.FD()})
	if err != nil {
		return err
	}
	if l.prog, err = ebpf.NewProgram(spec); err != nil {
		return verifier(err)
	}
	info, err := l.prog.Info()
	if err != nil {
		return err
	}
	id, _ := info.ID()
	l.id = int(id)
	return nil
}

// follow attaches the program to service pods as they come until stop is signalled.
func follow(o options, l *loaded, stop <-chan os.Signal) error {
	p := proc{"/proc", os.Readlink}
	on := map[string]*attachment{}
	seen := looks{}
	defer func() {
		for _, a := range on {
			if err := a.detach(l.id); err != nil {
				slog.Info("detach", "error", err)
			}
		}
		slog.Info("detached", "interfaces", len(on))
	}()
	counters, err := arrival.OpenCounters(o.pin)
	if err != nil {
		return err
	}
	defer counters.Close()
	if o.stats { // the kernel's own count of the program's runs and time
		c, err := ebpf.EnableStats(unix.BPF_STATS_RUN_TIME)
		if err != nil {
			return fmt.Errorf("run statistics: %w", err)
		}
		defer c.Close()
	}
	poll, print, verify := time.NewTicker(o.poll), time.NewTicker(o.print), time.NewTicker(o.verify)
	defer poll.Stop()
	defer print.Stop()
	defer verify.Stop()
	scan := func() {
		spaces, err := p.namespaces()
		if err != nil {
			slog.Warn("namespaces", "error", err)
			return
		}
		if o.pid > 0 {
			spaces = p.only(spaces, o.pid)
		}
		changed := false
		for ns, a := range on {
			if _, alive := spaces[ns]; !alive { // the pod is gone, its interface and filter with it
				if o.pid > 0 { // or only the process: a container that restarts keeps the pod's namespace
					if err := a.detach(l.id); err != nil {
						slog.Info("detach", "error", err)
					}
				} else {
					a.handle.Close()
				}
				delete(on, ns)
				changed = true
			}
		}
		seen.forget(spaces)
		now := time.Now()
		for ns, pid := range spaces {
			if on[ns] != nil || !seen.due(ns, now) || !p.listens(pid, uint16(o.port)) {
				continue
			}
			a, err := attach(pid, o.iface, l.prog.FD())
			if err != nil {
				slog.Warn("attach", "pid", pid, "error", err)
				continue
			}
			on[ns] = a
			changed = true
		}
		if changed {
			var names []string
			for ns := range on {
				names = append(names, ns)
			}
			sort.Strings(names)
			slog.Info("attached", "interfaces", len(on), "namespaces", names)
		}
	}
	// is it still there?  (another loader on its way out may have taken it)
	again := func() {
		for ns, a := range on {
			if ok, err := a.present(l.id); err != nil {
				slog.Warn("filters", "namespace", ns, "error", err)
			} else if !ok {
				err := a.add(l.prog.FD())
				slog.Warn("attached again", "namespace", ns, "error", err)
			}
		}
	}
	var last [arrival.NumCounters]uint64
	var lastRun ebpf.ProgramStats
	report := func() {
		v := counters.Read()
		args := []any{"interfaces", len(on)}
		for i, x := range v {
			args = append(args, arrival.CounterNames[i], x-last[i])
		}
		last = v
		if o.stats {
			if st, err := l.prog.Stats(); err == nil {
				args = append(args, "runs", st.RunCount-lastRun.RunCount, "run_ns", (st.Runtime - lastRun.Runtime).Nanoseconds())
				lastRun = *st
			}
		}
		slog.Info("counters", args...)
	}
	scan()
	for {
		select {
		case s := <-stop:
			report()
			slog.Info("stopping", "signal", s.String())
			return nil
		case <-poll.C:
			scan()
		case <-verify.C:
			again()
		case <-print.C:
			report()
		}
	}
}

// clean takes every arrival program off the pods of the node and removes the pins.
func clean(o options) error {
	p := proc{"/proc", os.Readlink}
	spaces, err := p.namespaces()
	if err != nil {
		return err
	}
	n := 0
	var bad []error // of one pod: the others are cleaned all the same
	for _, pid := range spaces {
		a, err := open(pid, o.iface)
		if err != nil {
			continue // no such interface there
		}
		ours, others, err := a.filters()
		if err != nil || len(ours) == 0 {
			a.handle.Close()
			continue
		}
		a.qdisc = others == 0
		if err := a.detach(0); err != nil && !errors.Is(err, unix.ENODEV) { // (no device: the pod is going away)
			bad = append(bad, err)
			continue
		}
		n++
	}
	for _, f := range []string{arrival.PinTable, arrival.PinCounters} {
		if err := os.Remove(filepath.Join(o.pin, f)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			bad = append(bad, err)
		}
	}
	_ = os.Remove(o.pin)
	fmt.Println("taken off", n, "interfaces")
	return errors.Join(bad...)
}

// dump writes what the pinned table and counters hold, through the agent's mapping, and the two
// clocks (the live check compares it with a capture).
func dump(o options) error {
	t, err := arrival.Open(o.pin)
	if err != nil {
		return err
	}
	defer t.Close()
	c, err := arrival.OpenCounters(o.pin)
	if err != nil {
		return err
	}
	defer c.Close()
	unixNs, monoNs := arrival.SampleClocks()
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	fmt.Fprintf(w, "clocks,%d,%d\nslots,%d\n", unixNs, monoNs, t.Slots())
	for i, v := range c.Read() {
		fmt.Fprintf(w, "counter,%s,%d\n", arrival.CounterNames[i], v)
	}
	for i := uint32(0); i < t.Slots(); i++ {
		if id, ns, ok := t.Read(uint64(i)); !ok {
			fmt.Fprintf(w, "torn,%d\n", i)
		} else if id != 0 {
			fmt.Fprintf(w, "id,%016x,%d\n", id, ns)
		}
	}
	return nil
}

// check runs the kernel's verifier over the program; nothing is pinned or attached.
func check(o options) error {
	if err := rlimit.RemoveMemlock(); err != nil {
		return err
	}
	c := prog.Config{Port: uint16(o.port), Slots: uint32(o.slots)}
	var fds [3]int
	ts, cs, ss := prog.Specs(c)
	for i, s := range []*ebpf.MapSpec{ts, cs, ss} {
		m, err := ebpf.NewMap(s)
		if err != nil {
			return fmt.Errorf("map %s: %w", s.Name, err)
		}
		defer m.Close()
		fds[i] = m.FD()
	}
	spec, err := prog.Spec(c, prog.Maps{Table: fds[0], Counters: fds[1], Scratch: fds[2]})
	if err != nil {
		return err
	}
	p, err := ebpf.NewProgramWithOptions(spec, ebpf.ProgramOptions{LogLevel: ebpf.LogLevelStats})
	if err != nil {
		return verifier(err)
	}
	defer p.Close()
	fmt.Println(p.VerifierLog)
	return nil
}

func run() error {
	var o options
	flag.UintVar(&o.port, "port", 12345, "the services' gRPC port")
	flag.UintVar(&o.slots, "slots", 1<<20, "slots of the table (a power of two, 16 bytes each)")
	flag.StringVar(&o.iface, "iface", "eth0", "the pods' interface")
	flag.StringVar(&o.pin, "pin", "/sys/fs/bpf/slobudget", "directory of the pinned maps (bpf filesystem)")
	flag.DurationVar(&o.poll, "poll", 2*time.Second, "how often new pods are looked for")
	flag.DurationVar(&o.verify, "verify", 10*time.Second, "how often the program is checked to be where it was put")
	flag.DurationVar(&o.print, "print", 5*time.Second, "how often the counters are logged")
	flag.BoolVar(&o.stats, "stats", false, "the kernel's run statistics of the program (time and runs, in the counters' line)")
	flag.IntVar(&o.pid, "pid", 0, "only the pod this process is in (0: every service pod; the live check's one pod)")
	show := flag.Bool("version", false, "print the version and exit")
	doCheck := flag.Bool("check", false, "load the program (the kernel's verifier), attach nothing, exit")
	doClean := flag.Bool("clean", false, "take the program off every pod, remove the pins, exit")
	doDump := flag.Bool("dump", false, "write what the pinned table and counters hold, exit")
	flag.Parse()
	if *show {
		fmt.Println(version)
		return nil
	}
	if o.port == 0 || o.port > 65535 || o.slots == 0 || o.slots&(o.slots-1) != 0 || o.slots > 1<<30 {
		return errors.New("-port 1-65535, -slots a power of two")
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	switch {
	case *doCheck:
		return check(o)
	case *doClean:
		return clean(o)
	case *doDump:
		return dump(o)
	}
	stop := make(chan os.Signal, 1) // before the load: a signal during it must not skip the clean-up
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	signal.Ignore(syscall.SIGPIPE) // nor a lost stdout
	l := &loaded{pins: map[string]*ebpf.Map{}}
	defer l.Close()
	defer l.unpin(o.pin)
	if err := l.load(o); err != nil {
		return err
	}
	slog.Info("loaded", "version", version, "port", o.port, "slots", o.slots, "pin", o.pin, "program", l.id)
	return follow(o, l, stop)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "arrivald:", err)
		os.Exit(1)
	}
}
