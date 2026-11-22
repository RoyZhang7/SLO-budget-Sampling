package slobudgetprocessor

import (
	"context"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Kept-notice: a 17-byte UDP datagram (16-byte trace id + 1 flag byte) from
// any agent that keeps a span to the agent that holds the trace's root.  With
// slo.h (root host) in baggage the notice is unicast; without it (the DSB-SN
// deployment) it is sent to every peer.
//
// Force-notice (keep_parent ablation): a 25-byte datagram (16-byte trace id +
// 8-byte span id + type byte 2) broadcast to every peer when an agent keeps an
// outlier: whichever agent sees that span id (the outlier's parent, or its `_br`
// checkpoint anchor) keeps it at p = 1.

const (
	noticeLen      = 17
	forceNoticeLen = 25
	forceType      = 2
)

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

	self     string                  // this agent's host: a notice for a root held here is a local flag
	peerAddr map[string]*net.UDPAddr // host -> notice address, published by the central from the agents' stats
	unicast  int
	selfA    string

	// force-notices (keep_parent ablation)
	onForce              func(tid [16]byte, sid [8]byte)
	forceSent, forceRecv int
}

func newNoticer(log *zap.Logger, listen string, peers []string, port int, onFlag func([16]byte),
	onForce func([16]byte, [8]byte)) (*noticer, error) {
	n := &noticer{log: log, peers: peers, port: port, flagged: map[[16]byte]time.Time{}, onFlag: onFlag, onForce: onForce}
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
			if k == forceNoticeLen && buf[forceNoticeLen-1] == forceType {
				var tid [16]byte
				var sid [8]byte
				copy(tid[:], buf[:16])
				copy(sid[:], buf[16:24])
				n.mu.Lock()
				n.forceRecv++
				n.mu.Unlock()
				if n.onForce != nil {
					n.onForce(tid, sid)
				}
				continue
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
	if len(targets) == 0 {
		return
	}
	for _, a := range targets {
		if _, err := n.conn.WriteToUDP(msg[:], a); err == nil {
			n.mu.Lock()
			n.sent++
			n.mu.Unlock()
		}
	}
}

// sendForce broadcasts a force-notice for span sid of trace tid to every peer.
func (n *noticer) sendForce(tid [16]byte, sid [8]byte) {
	if n.conn == nil {
		return
	}
	var msg [forceNoticeLen]byte
	copy(msg[:16], tid[:])
	copy(msg[16:24], sid[:])
	msg[forceNoticeLen-1] = forceType
	for _, a := range n.targets("") {
		if _, err := n.conn.WriteToUDP(msg[:], a); err == nil {
			n.mu.Lock()
			n.forceSent++
			n.mu.Unlock()
		}
	}
}

// targets: the root host's agent when the root host is known (slo.root_host) and the
// central published its address; nothing when the root is held here; else every peer.
func (n *noticer) targets(rootHost string) []*net.UDPAddr {
	n.mu.Lock()
	defer n.mu.Unlock()
	if rootHost != "" {
		if rootHost == n.self {
			return nil
		}
		if a := n.peerAddr[rootHost]; a != nil {
			n.unicast++
			return []*net.UDPAddr{a}
		}
	}
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

func (n *noticer) counts() (sent, recv, unicast int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sent, n.recv, n.unicast
}

// setPeers installs the central's host -> "ip:port" map.
func (n *noticer) setPeers(peers map[string]string) {
	m := make(map[string]*net.UDPAddr, len(peers))
	for h, addr := range peers {
		if a, err := net.ResolveUDPAddr("udp", addr); err == nil {
			m[h] = a
		}
	}
	n.mu.Lock()
	n.peerAddr = m
	n.mu.Unlock()
}

// selfAddr is this agent's notice address (POD_IP or the first non-loopback IPv4, plus
// the notice port), reported to the central for the peers map.
func (n *noticer) selfAddr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.selfA != "" {
		return n.selfA
	}
	ip := os.Getenv("POD_IP")
	if ip == "" {
		if addrs, err := net.InterfaceAddrs(); err == nil {
			for _, a := range addrs {
				if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() && ipn.IP.To4() != nil {
					ip = ipn.IP.String()
					break
				}
			}
		}
	}
	if ip == "" || n.conn == nil {
		return ""
	}
	n.selfA = net.JoinHostPort(ip, strconv.Itoa(n.port))
	return n.selfA
}
