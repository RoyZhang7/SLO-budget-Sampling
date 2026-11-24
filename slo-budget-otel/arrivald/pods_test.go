package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeProc is a /proc with plain files: ns/net holds what readlink would return.
func fakeProc(t *testing.T, procs map[string]map[string]string) proc {
	t.Helper()
	root := t.TempDir()
	for pid, files := range procs {
		for name, text := range files {
			p := filepath.Join(root, pid, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	return proc{root, func(p string) (string, error) {
		b, err := os.ReadFile(p)
		return string(b), err
	}}
}

const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"

const (
	listen12345  = header + "   0: 00000000:3039 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 1 1\n"
	listen12345v = header + "   0: 00000000000000000000000000000000:3039 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 1 1\n"
	client12345  = header + "   0: BC77A8C0:A620 AA54A8C0:3039 01 00000000:00000000 00:00000000 00000000     0        0 1 1\n" +
		"   1: BC77A8C0:3039 AA54A8C0:A620 01 00000000:00000000 00:00000000 00000000     0        0 1 1\n"
)

func TestNamespaces(t *testing.T) {
	p := fakeProc(t, map[string]map[string]string{
		"1":    {"ns/net": "net:[1]"},                         // the host
		"50":   {"ns/net": "net:[1]", "net/tcp": listen12345}, // a host process that listens: not a pod
		"100":  {"ns/net": "net:[7]"},
		"101":  {"ns/net": "net:[7]"},
		"200":  {"ns/net": "net:[8]"},
		"300":  {"comm": "gone"}, // no namespace to read
		"self": {"ns/net": "net:[9]"},
	})
	got, err := p.namespaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["net:[7]"] != 100 && got["net:[7]"] != 101 || got["net:[8]"] != 200 {
		t.Fatalf("%v", got)
	}
	if _, err := (proc{t.TempDir(), p.readlink}).namespaces(); err == nil {
		t.Fatal("no host namespace accepted")
	}
}

func TestLooks(t *testing.T) {
	l := looks{}
	now := time.Unix(1000, 0)
	due := 0
	for i := 0; i < 180; i++ { // three minutes, a poll a second
		if l.due("net:[7]", now.Add(time.Duration(i)*time.Second)) {
			due++
		}
	}
	if due != 60+4 { // the first minute, then at 60, 90, 120 and 150 s
		t.Fatalf("%d checks", due)
	}
	l.due("net:[8]", now)
	l.forget(map[string]int{"net:[8]": 5})
	if len(l) != 1 || l["net:[8]"] == nil {
		t.Fatalf("%v", l)
	}
}

func TestListens(t *testing.T) {
	p := fakeProc(t, map[string]map[string]string{
		"1": {"net/tcp": listen12345}, "2": {"net/tcp": header, "net/tcp6": listen12345v},
		"3": {"net/tcp": client12345, "net/tcp6": header}, // connections on the port, no listener
		"4": {}, "5": {"net/tcp": "garbage\n0: 00000000 x 0A\n:3039 1 2 0A"},
	})
	for pid, want := range map[int]bool{1: true, 2: true, 3: false, 4: false, 5: false, 6: false} {
		if got := p.listens(pid, 12345); got != want {
			t.Errorf("process %d: %v", pid, got)
		}
	}
	if p.listens(1, 2000) {
		t.Error("another port")
	}
}
