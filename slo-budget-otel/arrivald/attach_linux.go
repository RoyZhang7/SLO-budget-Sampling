package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const filterName = "slo_arrival"

// attachment is the program on one pod's interface.  The open handle keeps the namespace, so
// its number cannot be another pod's before the loader has noticed that the pod is gone.
type attachment struct {
	handle *netlink.Handle // in the pod's network namespace
	index  int
	qdisc  bool // the clsact qdisc is ours: added, or left by a loader that did not stop cleanly
}

// open is the interface in the network namespace of a process.
func open(pid int, iface string) (*attachment, error) {
	ns, err := netns.GetFromPid(pid)
	if err != nil {
		return nil, fmt.Errorf("namespace of %d: %w", pid, err)
	}
	defer ns.Close()
	h, err := netlink.NewHandleAt(ns, unix.NETLINK_ROUTE)
	if err != nil {
		return nil, fmt.Errorf("netlink in the namespace of %d: %w", pid, err)
	}
	_ = h.SetSocketTimeout(5 * time.Second)
	l, err := h.LinkByName(iface)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("%s in the namespace of %d: %w", iface, pid, err)
	}
	return &attachment{handle: h, index: l.Attrs().Index}, nil
}

// attach puts the program at the ingress of the interface in the network namespace of a process.
// The clsact qdisc's coming and going takes the interface's transmit queue down for a moment
// (5.15: qdisc_graft deactivates the device), so attach and detach stay outside measured load.
func attach(pid int, iface string, fd int) (*attachment, error) {
	a, err := open(pid, iface)
	if err != nil {
		return nil, err
	}
	if err := a.add(fd); err != nil {
		a.handle.Close()
		return nil, err
	}
	return a, nil
}

func (a *attachment) clsact() netlink.Qdisc {
	return &netlink.GenericQdisc{QdiscType: "clsact", QdiscAttrs: netlink.QdiscAttrs{
		LinkIndex: a.index, Handle: netlink.MakeHandle(0xffff, 0), Parent: netlink.HANDLE_CLSACT}}
}

func (a *attachment) filter(fd int) netlink.Filter {
	return &netlink.BpfFilter{Fd: fd, Name: filterName, DirectAction: true, FilterAttrs: netlink.FilterAttrs{
		LinkIndex: a.index, Parent: netlink.HANDLE_MIN_INGRESS, Handle: 1, Priority: 1, Protocol: unix.ETH_P_IP}}
}

func (a *attachment) add(fd int) error {
	switch err := a.handle.QdiscAdd(a.clsact()); {
	case err == nil:
		a.qdisc = true
	case !errors.Is(err, unix.EEXIST):
		return fmt.Errorf("clsact on %d: %w", a.index, err)
	default:
		ours, others, err := a.filters()
		a.qdisc = a.qdisc || err == nil && len(ours) > 0 && others == 0
	}
	if err := a.handle.FilterReplace(a.filter(fd)); err != nil {
		if ours, others, e := a.filters(); a.qdisc && e == nil && len(ours)+others == 0 {
			_ = a.handle.QdiscDel(a.clsact())
		}
		return fmt.Errorf("filter on %d: %w", a.index, err)
	}
	return nil
}

// filters lists the arrival program's filters on the interface and counts the others'.
func (a *attachment) filters() (ours []*netlink.BpfFilter, others int, err error) {
	l := &netlink.GenericLink{LinkAttrs: netlink.LinkAttrs{Index: a.index}}
	for _, parent := range []uint32{netlink.HANDLE_MIN_INGRESS, netlink.HANDLE_MIN_EGRESS} {
		fs, err := a.handle.FilterList(l, parent)
		if err != nil {
			return nil, 0, err
		}
		for _, f := range fs {
			if b, ok := f.(*netlink.BpfFilter); ok && b.Name == filterName && parent == netlink.HANDLE_MIN_INGRESS {
				ours = append(ours, b)
			} else {
				others++
			}
		}
	}
	return ours, others, nil
}

// present says whether the program with this id is still on the interface.
func (a *attachment) present(id int) (bool, error) {
	ours, _, err := a.filters()
	for _, f := range ours {
		if f.Id == id {
			return true, nil
		}
	}
	return false, err
}

// detach takes the program with this id off again (0: whichever), and the qdisc when it was ours
// and nothing is left on it.  A pod that is gone took both with it.
func (a *attachment) detach(id int) error {
	defer a.handle.Close()
	ours, others, err := a.filters()
	if err != nil {
		return err
	}
	left := others
	for _, f := range ours {
		if id != 0 && f.Id != id { // another loader's
			left++
			continue
		}
		if err := a.handle.FilterDel(f); err != nil {
			return err
		}
	}
	if a.qdisc && left == 0 {
		return a.handle.QdiscDel(a.clsact())
	}
	return nil
}
