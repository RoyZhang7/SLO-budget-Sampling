#!/usr/bin/env python3
"""What the arrival program stored at one pod's interface against a capture taken at the same interface at the same time
(live_check.sh runs both).

    python3 tools/overhead/live_check.py RUN_DIR

RUN_DIR holds capture.pcap (Ethernet, the pod's eth0, ingress, TCP to the services' port, nanoseconds), dump.csv (arrivald -dump: the
table and the counters through the agent's mapping, and the two clocks), and from live_check.sh before.txt and after.txt (the
interface's qdiscs and filters), pins.txt and pod.txt.  Writes live-check.json and live-check.md there, prints the latter, exits 0
only when all holds.

Two readings of the capture, neither of which shares code with the program:
  by packet   every key followed by 16 lower-case hexadecimal digits inside one packet's payload (what the program can see)
  by stream   TCP reassembly, HTTP/2 frames and gRPC messages: every request's id, with the packets its key and its digits came in
              (what there was; an id split between two packets is in this one only)
The table the program should hold follows from the packets in their order (first time of an id stays, another id takes the slot).
"""
from __future__ import annotations

import bisect
import collections
import json
import os
import re
import struct
import sys

PORT = 12345
CAP = 8192                      # positions of a packet the program scans
KEY = b'"SpanID":"'
FOUND = re.compile(re.escape(KEY) + rb"([0-9a-f]{16})")
PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
MSS = 1398                      # TCP payload of a full segment here: MTU 1450 - 20 - 20 - 12 (timestamps)
EARLY_MS, LATE_MS, MAX_MS = -0.05, 0.5, 20.0    # the program runs right after the capture's tap: earliest, the 99th percentile's latest, the latest
STREAMED = 0.995                # of the whole ids, the reading by stream must have (the capture starts and ends inside messages)
OK_SHARE = 0.99                 # of the ids a packet held whole must be in the table with a right time (EARLY_MS .. LATE_MS)


def read_pcap(path: str):
    """(time ns, frame, cut short) of an Ethernet pcap, micro- or nanoseconds"""
    b = open(path, "rb").read()
    magic, link = struct.unpack("<I", b[:4])[0], struct.unpack("<I", b[20:24])[0] & 0xFFFF
    unit = {0xA1B2C3D4: 1000, 0xA1B23C4D: 1}.get(magic)
    if unit is None or link != 1:
        raise ValueError(f"{path}: not an Ethernet pcap (magic {magic:#x}, link type {link})")
    p = 24
    while p + 16 <= len(b):
        s, f, cl, ol = struct.unpack("<IIII", b[p:p + 16])
        p += 16
        if p + cl > len(b):
            return
        yield s * 10 ** 9 + f * unit, b[p:p + cl], ol > cl
        p += cl


def packets(path: str, stats: collections.Counter):
    """the TCP packets to the service port with payload: (number, time, flow, sequence number, SYN, payload, bytes in front of it)"""
    for n, (ts, d, cut) in enumerate(read_pcap(path), 1):
        stats["captured"] += 1
        stats["truncated"] += cut
        if len(d) < 54 or struct.unpack(">H", d[12:14])[0] != 0x0800:
            continue
        ip = d[14:]
        ihl, total, frag = (ip[0] & 15) * 4, struct.unpack(">H", ip[2:4])[0], struct.unpack(">H", ip[6:8])[0]
        if ip[0] >> 4 != 4 or ip[9] != 6 or frag & 0x3FFF:
            continue
        t = ip[ihl:]
        sp, dp, seq = struct.unpack(">HHI", t[:8])
        if dp != PORT:
            continue
        off = (t[12] >> 4) * 4
        yield n, ts, (ip[12:16], sp, ip[16:20]), seq, bool(t[13] & 2), bytes(t[off:(total - ihl) if total else len(t)]), 14 + ihl + off


class Flow:
    """one direction of a connection, reassembled; which packet every byte came in first"""

    def __init__(self):
        self.next = None
        self.buf = bytearray()
        self.off, self.pkt = [], []
        self.pend = {}

    def _put(self, data, pkt):
        self.off.append(len(self.buf))
        self.pkt.append(pkt)
        self.buf += data
        self.next = (self.next + len(data)) & 0xFFFFFFFF

    def add(self, seq, data, pkt):
        if not data:
            return
        if self.next is None:
            self.next = seq
        d = (seq - self.next) & 0xFFFFFFFF
        if d >= 0x80000000:                     # came before
            back = 0x100000000 - d
            if back >= len(data):
                return
            data = data[back:]
        elif d > 0:
            self.pend[seq] = (data, pkt)
            return
        self._put(data, pkt)
        while self.next in self.pend:
            self._put(*self.pend.pop(self.next))

    def at(self, off):
        return self.pkt[bisect.bisect_right(self.off, off) - 1]


def frames(b: bytes, i: int):
    while i + 9 <= len(b):
        ln, ty, fl, sid = (b[i] << 16) | (b[i + 1] << 8) | b[i + 2], b[i + 3], b[i + 4], struct.unpack(">I", b[i + 5:i + 9])[0]
        if ty > 9 or ln > 16384 or sid & 0x80000000 or i + 9 + ln > len(b):
            return
        yield i, ln, ty, fl, sid
        i += 9 + ln


def start_of(fl: Flow) -> int | None:
    """where the frames start: after the preface, or at a packet from which three frames follow (a connection from before the capture)"""
    b = bytes(fl.buf)
    if b.startswith(PREFACE):
        return 24
    for off in fl.off[:400]:
        if sum(1 for _, _ in zip(frames(b, off), range(3))) == 3:
            return off
    return None


def requests(fl: Flow, stats: collections.Counter):
    """(id, packet of the key's first byte, of its last byte, of the last digit) of every gRPC message with a key"""
    st = start_of(fl)
    if st is None:
        stats["bytes of streams without frames"] += len(fl.buf)
        return
    b = bytes(fl.buf)
    acc: dict[int, list[tuple[int, int]]] = {}
    for off, ln, ty, flg, sid in frames(b, st):
        if ty != 0:
            continue
        o, n = off + 9, ln
        if flg & 8:                             # padded
            o, n = o + 1, n - 1 - b[o]
        pieces = acc.setdefault(sid, [])
        if n > 0:
            pieces.append((o, n))
        raw = b"".join(b[x:x + y] for x, y in pieces)
        if len(raw) < 5 or len(raw) < 5 + struct.unpack(">I", raw[1:5])[0]:
            continue
        msg = raw[5:5 + struct.unpack(">I", raw[1:5])[0]]
        acc[sid] = []
        stats["messages"] += 1
        k = msg.rfind(KEY)
        if raw[0] or k < 0:
            stats["messages without an id"] += 1
            continue

        def where(x, pieces=pieces):
            x += 5
            for o2, n2 in pieces:
                if x < n2:
                    return fl.at(o2 + x)
                x -= n2
            raise IndexError
        yield msg[k + len(KEY):k + len(KEY) + 16].decode(), where(k), where(k + len(KEY) - 1), where(k + len(KEY) + 15)


def read_dump(path: str) -> dict:
    d: dict = {"ids": {}, "counters": {}, "torn": 0, "slots": 0}
    for ln in open(path, encoding="ascii"):
        w = ln.strip().split(",")
        if w[0] == "clocks":
            d["offset"] = int(w[1]) - int(w[2])
        elif w[0] == "slots":
            d["slots"] = int(w[1])
        elif w[0] == "counter":
            d["counters"][w[1]] = int(w[2])
        elif w[0] == "torn":
            d["torn"] += 1
        elif w[0] == "id":
            d["ids"][w[1]] = int(w[2])
    if not d["slots"] or "offset" not in d:
        raise ValueError(f"{path}: not a dump of arrivald")
    return d


def quantiles(v: list[float]) -> dict:
    v = sorted(v)
    pick = lambda q: v[min(len(v) - 1, int(q * len(v)))]
    return {"n": len(v), "min": v[0], "p50": pick(.5), "p99": pick(.99), "p99.9": pick(.999), "max": v[-1]} if v else {"n": 0}


def run_stats(path: str) -> dict:
    """the kernel's count of the program's runs and time, summed over the loader's counter lines"""
    runs = ns = 0
    if os.path.exists(path):
        for ln in open(path, encoding="utf-8", errors="replace"):
            try:
                d = json.loads(ln)
            except ValueError:
                continue
            if isinstance(d, dict) and d.get("msg") == "counters":
                runs, ns = runs + d.get("runs", 0), ns + d.get("run_ns", 0)
    return {"runs": runs, "ns_per_run": ns / runs if runs else None}


def compare(rd: str) -> dict:
    stats: collections.Counter = collections.Counter()
    dump = read_dump(os.path.join(rd, "dump.csv"))
    mask = dump["slots"] - 1
    flows: dict = {}
    seen: list[tuple[int, int, list[str], int]] = []                # packets with payload: number, time, ids whole in it, bytes
    first: dict[str, int] = {}                                      # id -> time of the first packet it was whole in
    table: dict[int, tuple[str, int]] = {}                          # what the program should hold
    want: collections.Counter = collections.Counter()
    where: dict[str, int] = {}                                      # id -> its key's offset in that packet's payload
    again: set[str] = set()                                         # ids in more than one packet: the first time must stay
    for n, ts, flow, seq, syn, data, head in packets(os.path.join(rd, "capture.pcap"), stats):
        fl = flows.setdefault(flow, Flow())
        if syn:
            fl.next = (seq + 1) & 0xFFFFFFFF
            continue
        fl.add(seq, data, n)
        if not data:
            continue
        ids = []
        for m in FOUND.finditer(data):
            if m.start() < CAP and int(m[1], 16):
                ids.append(m[1].decode())
                where.setdefault(ids[-1], m.start())
        seen.append((n, ts, ids, len(data)))
        want["packets"] += 1
        want["ids"] += len(ids)
        want["no_id"] += not ids
        for x in ids:
            first.setdefault(x, ts)
            slot = int(x, 16) & mask
            old = table.get(slot)
            if old and old[0] == x:
                want["repeats"] += 1
                again.add(x)
                continue
            want["overwritten"] += bool(old)
            table[slot] = (x, ts)
    by_stream = [r for fl in flows.values() for r in requests(fl, stats)]
    stats["segments out of order at the end"] += sum(len(fl.pend) for fl in flows.values())
    split = [r for r in by_stream if not r[1] == r[2] == r[3]]
    split_ids = {r[0] for r in split}
    held = {x: ts for x, ts in table.values()}
    got = dump["ids"]
    missing = sorted(x for x in held if x not in got)
    extra = sorted(x for x in got if x not in held)
    diff = {x: (got[x] + dump["offset"] - held[x]) / 1e6 for x in held if x in got}
    moved = sorted(x for x in again if diff.get(x, 0.0) > LATE_MS)
    right = sum(1 for d in diff.values() if EARLY_MS <= d <= LATE_MS)
    never_whole = split_ids - set(first)                            # the program cannot see them
    streamed = len(set(first) & {r[0] for r in by_stream})          # of the ids whole in a packet, those the stream reading has too
    sizes = [s for _, _, _, s in seen]
    per = collections.Counter(len(i) for _, _, i, _ in seen if i)
    big = {n: s for n, _, _, s in seen if s > MSS}
    ids_in_big = sum(len(i) for n, _, i, _ in seen if n in big)
    c = dump["counters"]
    res = {"slots": dump["slots"], "requests_by_stream": len(by_stream), "split_between_packets": len(split), "split_in_key": sum(1 for r in split if r[1] != r[2]),
           "ids_whole_in_a_packet": len(first), "expected_in_table": len(held), "in_table": len(got), "missing": missing[:20], "n_missing": len(missing), "extra": extra[:20],
           "n_extra": len(extra), "split_stored": sorted(split_ids & set(got) - set(first))[:20], "torn": dump["torn"], "program_minus_capture_ms": quantiles(list(diff.values())),
           "counters": c, "counters_expected": dict(want), "payload_bytes": quantiles([float(s) for s in sizes]), "ids_per_packet": dict(sorted(per.items())),
           "packets_over_one_segment": len(big), "ids_in_packets_over_one_segment": ids_in_big, "ids_behind_the_first_segment": sum(1 for x in where.values() if x >= MSS),
           "matched_with_a_right_time": right, "share_matched": right / len(held) if held else 0.0,
           "whole_ids_in_the_streams": streamed, "never_whole": len(never_whole), "share_of_capture": right / (len(held) + len(never_whole)) if held else 0.0,
           "capture": dict(stats)}

    def text(name):
        p = os.path.join(rd, name)
        return open(p, encoding="utf-8", errors="replace").read().strip() if os.path.exists(p) else None
    before, after, pins, pod = text("before.txt"), text("after.txt"), text("pins.txt"), text("pod.txt")
    cont = os.path.join(rd, "dump-container.csv")
    try:
        res["container_sees_the_table"] = read_dump(cont)["ids"] == got
    except (OSError, ValueError):
        res["container_sees_the_table"] = False
    res["run_stats"] = run_stats(os.path.join(rd, "arrivald.log"))
    bad = []
    if not held:
        bad.append("no id in the capture")
    if missing:
        bad.append(f"{len(missing)} ids of the capture are not in the table")
    if extra:
        bad.append(f"{len(extra)} ids in the table that the capture does not have")
    q = res["program_minus_capture_ms"]
    if q["n"] and (q["min"] < EARLY_MS or q["p99"] > LATE_MS or q["max"] > MAX_MS):
        bad.append(f"the program's time is {q['min']:.3f} to (p99) {q['p99']:.3f}, at most {q['max']:.3f} ms after the capture's")
    if moved:
        bad.append(f"{len(moved)} ids of several packets do not have their first packet's time: {moved[:5]}")
    if first and streamed < STREAMED * len(first):
        bad.append(f"the reading by stream has only {streamed} of the {len(first)} ids that were whole in a packet")
    if held and right < OK_SHARE * len(held):
        bad.append(f"{right} of {len(held)} ids matched with a right time, under {100 * OK_SHARE:g} %")
    for k in ("ids", "repeats", "overwritten"):
        if c.get(k) != want[k]:
            bad.append(f"counter {k}: {c.get(k)}, the capture says {want[k]}")
    if c.get("errors") or c.get("capped") or dump["torn"]:
        bad.append(f"errors {c.get('errors')}, capped {c.get('capped')}, torn slots {dump['torn']}")
    if stats["truncated"]:
        bad.append(f"{stats['truncated']} truncated packets in the capture")
    if before is None or after != before:
        bad.append("the interface is not as it was before")
    if pins != "":
        bad.append(f"pins left: {pins}")
    if not res["container_sees_the_table"]:
        bad.append("a container with the agent's rights does not read the table")
    if not res["run_stats"]["runs"]:
        bad.append("the kernel counted no runs of the program (-stats)")
    if text("left.txt"):
        bad.append(f"left on the node: {text('left.txt')}")
    if pod is not None and len(set(pod.splitlines())) != 1:
        bad.append(f"the pod changed: {pod.splitlines()}")
    res["problems"] = bad
    return res


def report(r: dict) -> str:
    q, c, w = r["program_minus_capture_ms"], r["counters"], r["counters_expected"]
    out = ["# Live check of the arrival program", "", "**" + ("All holds." if not r["problems"] else "FAILED: " + "; ".join(r["problems"])) + "**", "",
           "| | |", "|---|---|",
           f"| requests in the capture (by stream) | {r['requests_by_stream']:,} |",
           f"| ids whole in one packet | {r['ids_whole_in_a_packet']:,} |",
           f"| ids split between two packets (in the key / in the digits) | {r['split_between_packets']:,} ({r['split_in_key']} / {r['split_between_packets'] - r['split_in_key']}) |",
           f"| ids the table should hold / holds | {r['expected_in_table']:,} / {r['in_table']:,} of {r['slots']:,} slots |",
           f"| missing / extra / torn | {r['n_missing']} / {r['n_extra']} / {r['torn']} |",
           f"| **ids matched with a right time** ({EARLY_MS} to {LATE_MS} ms after the capture's) | **{r['matched_with_a_right_time']:,} of {r['expected_in_table']:,} ({100 * r['share_matched']:.2f} %)** |",
           f"| the same with the ids that never came whole in a packet counted as not found | **{r['matched_with_a_right_time']:,} of {r['expected_in_table'] + r['never_whole']:,} ({100 * r['share_of_capture']:.2f} %)** |"]
    if q["n"]:
        out.append(f"| program's time minus the capture's, ms (min, p50, p99, p99.9, max) | {q['min']:.4f}, {q['p50']:.4f}, {q['p99']:.4f}, {q['p99.9']:.4f}, {q['max']:.4f} |")
    out += [f"| counters, program (capture) | " + ", ".join(f"{k} {c.get(k)}" + (f" ({w[k]})" if k in w else "") for k in c) + " |",
            f"| ids per packet with ids | " + ", ".join(f"{k}: {v:,}" for k, v in r["ids_per_packet"].items()) + " |",
            f"| program's runs, ns per run (the kernel's statistics) | {r['run_stats']['runs']:,}, {r['run_stats']['ns_per_run'] or 0:.0f} |",
            f"| a container with the agent's rights reads the same table | {r['container_sees_the_table']} |", "",
            "Payload in several fragments:", "",
            f"- {c.get('paged', 0):,} of {c.get('packets', 0):,} packets had payload outside the skb's linear part (page fragments); {c.get('paged_ids', 0):,} of {c.get('ids', 0):,} ids were read from there"
            " (the program copies with bpf_skb_load_bytes, which follows the fragments).",
            f"- {r['packets_over_one_segment']:,} packets carried more than one TCP segment (over {MSS:,} bytes, merged before the pod); {r['ids_in_packets_over_one_segment']:,} ids came in them, "
            f"{r['ids_behind_the_first_segment']:,} of them behind the first {MSS:,} bytes.",
            f"- An id whose key or digits are split between two packets is not seen: {r['split_between_packets']:,} of {r['requests_by_stream']:,}"
            f" ({100 * r['split_between_packets'] / max(1, r['requests_by_stream']):.2f} %)."]
    return "\n".join(out) + "\n"


def main() -> int:
    rd = sys.argv[1]
    r = compare(rd)
    with open(os.path.join(rd, "live-check.json"), "w", encoding="utf-8") as f:
        json.dump(r, f, indent=1)
    t = report(r)
    with open(os.path.join(rd, "live-check.md"), "w", encoding="utf-8") as f:
        f.write(t)
    print(t)
    return 1 if r["problems"] else 0


if __name__ == "__main__":
    sys.exit(main())
