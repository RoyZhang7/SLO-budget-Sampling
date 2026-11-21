package slobudgetprocessor

import (
	"context"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Kept-notice: a 17-byte UDP datagram (16-byte trace id + 1 flag byte) from
// any agent that keeps a span to the agent that holds the trace's root.  With
// slo.h (root host) in baggage the notice is unicast; without it (the DSB-SN
// deployment) it is sent to every peer.

const noticeLen = 17

type noticer struct {
	log     *zap.Logger
	conn    *net.UDPConn
	peers   []string
	port    int
	mu      sync.Mutex
	flagged map[[16]byte]time.Time // trace ids a notice arrived for (or kept locally)
	onFlag  func(tid [16]byte)
	sent    int
	recv    int
	resolve map[string]time.Time
	addrs   []*net.UDPAddr
}

func newNoticer(log *zap.Logger, listen string, peers []string, port int, onFlag func([16]byte)) (*noticer, error) {
	n := &noticer{log: log, peers: peers, port: port, flagged: map[[16]byte]time.Time{}, onFlag: onFlag}
	if listen != "" {
		addr, err := net.ResolveUDPAddr("udp", listen)
		if err != nil {
			return nil, err
		}
		n.conn, err = net.ListenUDP("udp", addr)
		if err != nil {
			return nil, err
		}
	}
	return n, nil
}

func (n *noticer) start(ctx context.Context) {
	if n.conn == nil {
		return
	}
	go func() {
		buf := make([]byte, 64)
		for {
			k, _, err := n.conn.ReadFromUDP(buf)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			if k < noticeLen {
				continue
			}
			var tid [16]byte
			copy(tid[:], buf[:16])
			n.mu.Lock()
			n.recv++
			n.flagged[tid] = time.Now()
			n.mu.Unlock()
			if n.onFlag != nil {
				n.onFlag(tid)
			}
		}
	}()
}

func (n *noticer) stop() {
	if n.conn != nil {
		_ = n.conn.Close()
	}
}

// flag marks a trace as having a kept span (a local keep or a received notice).
func (n *noticer) flag(tid [16]byte) {
	n.mu.Lock()
	n.flagged[tid] = time.Now()
	n.mu.Unlock()
}

func (n *noticer) isFlagged(tid [16]byte) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.flagged[tid]
	return ok
}

// expire drops flags older than ttl.
func (n *noticer) expire(ttl time.Duration) {
	cut := time.Now().Add(-ttl)
	n.mu.Lock()
	for k, t := range n.flagged {
		if t.Before(cut) {
			delete(n.flagged, k)
		}
	}
	n.mu.Unlock()
}

// send a notice for tid to rootHost (unicast) or to every peer.
func (n *noticer) send(tid [16]byte, rootHost string) {
	if n.conn == nil {
		return
	}
	var msg [noticeLen]byte
	copy(msg[:16], tid[:])
	msg[16] = 1
	targets := n.targets(rootHost)
	for _, a := range targets {
		if _, err := n.conn.WriteToUDP(msg[:], a); err == nil {
			n.mu.Lock()
			n.sent++
			n.mu.Unlock()
		}
	}
}

// targets resolves the peer list (every 2 s) or the given root host.
func (n *noticer) targets(rootHost string) []*net.UDPAddr {
	if rootHost != "" {
		if a, err := net.ResolveUDPAddr("udp", net.JoinHostPort(rootHost, strconv.Itoa(n.port))); err == nil {
			return []*net.UDPAddr{a}
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if t, ok := n.resolve["peers"]; ok && time.Since(t) < 2*time.Second {
		return n.addrs
	}
	var addrs []*net.UDPAddr
	for _, p := range n.peers {
		host, port, err := net.SplitHostPort(p)
		if err != nil {
			host, port = p, strconv.Itoa(n.port)
		}
		ips, err := net.LookupIP(host)
		if err != nil {
			continue
		}
		for _, ip := range ips {
			pn, _ := strconv.Atoi(port)
			addrs = append(addrs, &net.UDPAddr{IP: ip, Port: pn})
		}
	}
	if n.resolve == nil {
		n.resolve = map[string]time.Time{}
	}
	n.resolve["peers"] = time.Now()
	n.addrs = addrs
	return addrs
}

func (n *noticer) counts() (sent, recv int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sent, n.recv
}
