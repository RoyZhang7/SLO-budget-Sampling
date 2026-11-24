package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// proc is the part of /proc the discovery reads.
type proc struct {
	root     string
	readlink func(string) (string, error)
}

// namespaces maps every network namespace but the host's (process 1's) to one of its processes.
func (p proc) namespaces() (map[string]int, error) {
	host, err := p.readlink(filepath.Join(p.root, "1", "ns", "net"))
	if err != nil {
		return nil, err
	}
	dirs, err := os.ReadDir(p.root)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	for _, d := range dirs {
		pid, err := strconv.Atoi(d.Name())
		if err != nil {
			continue
		}
		ns, err := p.readlink(filepath.Join(p.root, d.Name(), "ns", "net"))
		if err != nil || ns == "" || ns == host { // gone, or a kernel thread
			continue
		}
		if _, ok := out[ns]; !ok {
			out[ns] = pid
		}
	}
	return out, nil
}

// only keeps the namespace a process is in.
func (p proc) only(spaces map[string]int, pid int) map[string]int {
	ns, err := p.readlink(filepath.Join(p.root, strconv.Itoa(pid), "ns", "net"))
	if _, ok := spaces[ns]; err != nil || !ok {
		return nil
	}
	return map[string]int{ns: pid}
}

// looks spaces the checks of a namespace without the listener: at every poll in its first
// minute (a pod's server comes up seconds after its namespace), then every half minute.  Each
// check walks the host's whole table of connections.
type looks map[string]*look

type look struct{ first, next time.Time }

func (l looks) due(ns string, now time.Time) bool {
	x := l[ns]
	if x == nil {
		x = &look{first: now}
		l[ns] = x
	}
	if now.Sub(x.first) < time.Minute {
		return true
	}
	if now.Before(x.next) {
		return false
	}
	x.next = now.Add(30 * time.Second)
	return true
}

// forget drops the namespaces that are gone.
func (l looks) forget(alive map[string]int) {
	for ns := range l {
		if _, ok := alive[ns]; !ok {
			delete(l, ns)
		}
	}
}

// listens says whether the network namespace of a process has a TCP listener on the port.
func (p proc) listens(pid int, port uint16) bool {
	for _, f := range []string{"tcp", "tcp6"} {
		if listensIn(filepath.Join(p.root, strconv.Itoa(pid), "net", f), port) {
			return true
		}
	}
	return false
}

// listensIn reads a /proc/net/tcp table: local address (hexadecimal address:port) and state (0A: LISTEN).
func listensIn(path string, port uint16) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		w := strings.Fields(s.Text())
		if len(w) < 4 || w[3] != "0A" {
			continue
		}
		_, hex, ok := strings.Cut(w[1], ":")
		if v, err := strconv.ParseUint(hex, 16, 16); ok && err == nil && uint16(v) == port {
			return true
		}
	}
	return false
}
