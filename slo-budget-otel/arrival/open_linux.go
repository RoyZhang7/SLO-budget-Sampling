//go:build linux

package arrival

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/unix"
)

// The bpf(2) commands and constants used here (linux/bpf.h).
const (
	bpfObjGet       = 7
	bpfObjGetInfo   = 15
	mapTypeArray    = 2
	mapFlagMmapable = 1 << 10
)

// Open maps the pinned table of a pin directory read-only.  It needs CAP_BPF (bpf(2) is
// closed to unprivileged processes on the nodes); the mapping itself needs nothing.
func Open(dir string) (*Table, error) {
	mem, closer, err := mapPinned(filepath.Join(dir, PinTable), SlotSize)
	if err != nil {
		return nil, err
	}
	t, err := NewTable(mem, closer)
	if err != nil {
		_ = closer()
		return nil, err
	}
	return t, nil
}

// OpenCounters maps the program's counters.
func OpenCounters(dir string) (*Counters, error) {
	mem, closer, err := mapPinned(filepath.Join(dir, PinCounters), NumCounters*8)
	if err != nil {
		return nil, err
	}
	return NewCounters(mem, closer)
}

// mapPinned opens a pinned mmapable array of valueSize-byte values and maps it read-only.  The
// map is opened read-write (a read-only map cannot be mapped shared: do_mmap drops VM_SHARED
// for a file without write access and bpf_map_mmap refuses the rest); the mapping is PROT_READ
// and the kernel clears its VM_MAYWRITE.
func mapPinned(path string, valueSize int) ([]byte, func() error, error) {
	p, err := unix.BytePtrFromString(path)
	if err != nil {
		return nil, nil, err
	}
	get := struct {
		pathname  unsafe.Pointer // not an integer: the runtime must see the pointers in the attributes
		fd, flags uint32
	}{pathname: unsafe.Pointer(p)}
	fd, err := bpf(bpfObjGet, unsafe.Pointer(&get), unsafe.Sizeof(get))
	if err != nil {
		return nil, nil, fmt.Errorf("open pinned %s: %w", path, err)
	}
	defer unix.Close(fd) // the mapping keeps the map
	var raw [80]byte     // struct bpf_map_info: type, id, key_size, value_size, max_entries, map_flags, ...
	info := struct {
		fd, size uint32
		info     unsafe.Pointer
	}{uint32(fd), uint32(len(raw)), unsafe.Pointer(&raw[0])}
	_, err = bpf(bpfObjGetInfo, unsafe.Pointer(&info), unsafe.Sizeof(info))
	if err != nil {
		return nil, nil, fmt.Errorf("info of %s: %w", path, err)
	}
	u32 := func(i int) uint32 { return binary.NativeEndian.Uint32(raw[4*i:]) }
	typ, vs, entries, flags := u32(0), int(u32(3)), int(u32(4)), u32(5)
	if typ != mapTypeArray || flags&mapFlagMmapable == 0 || vs != valueSize {
		return nil, nil, fmt.Errorf("%s: not an mmapable array of %d-byte values (type %d, %d bytes, flags %#x)", path, valueSize, typ, vs, flags)
	}
	n := vs * entries
	page := unix.Getpagesize()
	mem, err := unix.Mmap(fd, 0, (n+page-1)/page*page, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	return mem[:n:n], func() error { return unix.Munmap(mem) }, nil
}

func bpf(cmd uintptr, attr unsafe.Pointer, size uintptr) (int, error) {
	r, _, e := unix.Syscall(unix.SYS_BPF, cmd, uintptr(attr), size)
	if e != 0 {
		return 0, e
	}
	return int(r), nil
}

// Identity names the pinned table (device and inode), so that a table pinned anew is noticed.
func Identity(dir string) (string, error) {
	var st unix.Stat_t
	if err := unix.Stat(filepath.Join(dir, PinTable), &st); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d:%d", st.Dev, st.Ino), nil
}

// SampleClocks reads the wall and the monotonic clock together: the tightest of five pairs.
func SampleClocks() (unixNs, monoNs int64) {
	best := int64(1 << 62)
	for i := 0; i < 5; i++ {
		var a, m, b unix.Timespec
		_ = unix.ClockGettime(unix.CLOCK_REALTIME, &a)
		_ = unix.ClockGettime(unix.CLOCK_MONOTONIC, &m)
		_ = unix.ClockGettime(unix.CLOCK_REALTIME, &b)
		if d := b.Nano() - a.Nano(); d >= 0 && d < best {
			best, unixNs, monoNs = d, a.Nano()+d/2, m.Nano()
		}
	}
	return unixNs, monoNs
}
