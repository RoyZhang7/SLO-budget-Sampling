"""live_check.py on a made-up capture and dump."""
import os
import random
import struct
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import live_check as lc  # noqa: E402

T0 = 1_795_500_000_000_000_000
OFFSET = T0 - 5_000_000_000_000         # wall minus monotonic
DELAY = 12_000                          # ns from the capture's tap to the program
MSS = lc.MSS


def h2(sid, payload, ty=0, flags=0):
    return struct.pack(">I", len(payload))[1:] + bytes([ty, flags]) + struct.pack(">I", sid) + payload


def grpc(msg):
    return b"\0" + struct.pack(">I", len(msg)) + msg


def request(xid, before):
    return b"x" * before + lc.KEY + xid.encode() + b'","TraceFlags":"01","TraceState":"","Remote":false}}'


class Conn:
    def __init__(self, port):
        self.port, self.seq, self.sid = port, 1000, 1

    def packet(self, data, flags=0x18, seq=None, dport=lc.PORT, pad=b""):
        s = self.seq if seq is None else seq
        tcp = struct.pack(">HHIIBBHHH", self.port, dport, s & 0xFFFFFFFF, 0, 8 << 4, flags, 0, 0, 0) + b"\0" * 12
        ip = struct.pack(">BBHHHBBH4s4s", 0x45, 0, 20 + 32 + len(data), 0, 0x4000, 64, 6, 0, bytes([192, 168, 1, 7]), bytes([192, 168, 2, 9]))
        if seq is None:
            self.seq += len(data) + (1 if flags & 2 else 0)
        return b"\0" * 12 + b"\x08\x00" + ip + tcp + data + pad


def capture(frames, nano=True):
    """pcap bytes of (time ns, frame)"""
    out = struct.pack("<IHHiIII", 0xA1B23C4D if nano else 0xA1B2C3D4, 2, 4, 0, 0, 262144, 1)
    for t, f in frames:
        out += struct.pack("<IIII", t // 10 ** 9, t % 10 ** 9 // (1 if nano else 1000), len(f), len(f)) + f
    return out


def build(tmp_path, n=400, seed=1, edit=None, slots=1 << 12):
    """requests on two connections: one packet, several in one, split ones, a retransmission; and the dump the program would leave"""
    rng = random.Random(seed)
    conns = [Conn(40001), Conn(40002)]
    frames, t = [], T0
    for c in conns:
        frames.append((t, c.packet(b"", flags=2)))
        t += 1000
        frames.append((t, c.packet(lc.PREFACE + h2(0, b"", 4))))
        t += 1000
    whole, split, sent = [], [], []
    i, again = 0, False
    while i < n:
        c = conns[i % 2]
        t += 5_000_000
        k = rng.choice([1, 1, 1, 2, 4])
        data, ids = b"", []
        for _ in range(k):
            xid = f"{rng.getrandbits(64) | 1:016x}"
            data += h2(c.sid, b"hdr", 1, 4) + h2(c.sid, grpc(request(xid, rng.choice([90, 700, 1500]))), 0, 1)
            c.sid += 2
            ids.append(xid)
        i += k
        if rng.random() < 0.1:                  # cut inside the last one's key or digits
            at = data.rfind(lc.KEY) + rng.choice([4, 12, 20])
            parts = [data[:at], data[at:]]
            split.append(ids.pop())
        elif rng.random() < 0.3 and len(data) > MSS:
            at = data.rfind(lc.KEY) - 40        # two packets, every id whole
            parts = [data[:at], data[at:]]
        else:
            parts = [data]
        for part in parts:
            f = c.packet(part, pad=b"\0" * rng.choice([0, 0, 6]))
            frames.append((t, f))
            sent.append((t, c, part))
            t += 3000
        whole += ids
        if not again and i >= n // 2 and ids and parts[-1].count(lc.KEY):            # a segment again
            again = True
            frames.append((t + 2_000_000, c.packet(parts[-1], seq=c.seq - len(parts[-1]))))
            sent.append((t + 2_000_000, c, parts[-1]))
    frames.append((t, conns[0].packet(b"nothing here", dport=9999)))
    # the program by its own rule, written once more
    table, counters = {}, dict.fromkeys(["packets", "ids", "no_id", "overwritten", "capped", "cut", "bad_id", "repeats", "crowded", "errors", "paged", "paged_ids"], 0)
    counters["packets"], counters["no_id"] = 2, 2               # the prefaces
    for ts, c, part in sorted(sent, key=lambda x: x[0]):
        counters["packets"] += 1
        found, p = [], part.find(lc.KEY)
        while p >= 0:
            x = part[p + 10:p + 26]
            if len(x) == 16:
                found.append(x.decode())
            p = part.find(lc.KEY, p + 1)
        counters["no_id"] += not found
        for x in found:
            counters["ids"] += 1
            s = int(x, 16) & (slots - 1)
            if s in table and table[s][0] == x:
                counters["repeats"] += 1
                continue
            counters["overwritten"] += s in table
            table[s] = (x, ts + DELAY - OFFSET + rng.randrange(0, 3000))
    if edit:
        edit(table, counters)
    (tmp_path / "capture.pcap").write_bytes(capture(sorted(frames, key=lambda x: x[0])))
    lines = [f"clocks,{T0 + 10 ** 10},{T0 + 10 ** 10 - OFFSET}", f"slots,{slots}"] + [f"counter,{k},{v}" for k, v in counters.items()]
    lines += [f"id,{x},{ns}" for x, ns in table.values()]
    (tmp_path / "dump.csv").write_text("\n".join(lines) + "\n")
    (tmp_path / "dump-container.csv").write_text("\n".join(lines) + "\n")
    (tmp_path / "arrivald.log").write_text('not json\n{"msg":"loaded"}\n{"msg":"counters","runs":100,"run_ns":90000}\n{"msg":"counters","runs":300,"run_ns":310000}\n')
    for name, text in (("left.txt", ""), ("before.txt", "qdisc noqueue 0: root refcnt 2\ningress:\negress:"), ("after.txt", "qdisc noqueue 0: root refcnt 2\ningress:\negress:\n"), ("pins.txt", ""),
                       ("pod.txt", "uid docker://a restarts=0\nuid docker://a restarts=0\n")):
        (tmp_path / name).write_text(text)
    return whole, split, table


def test_all_holds(tmp_path):
    whole, split, table = build(tmp_path)
    r = lc.compare(str(tmp_path))
    assert r["problems"] == []
    assert r["requests_by_stream"] == len(whole) + len(split) and r["split_between_packets"] == len(split) > 5
    assert r["ids_whole_in_a_packet"] == len(whole) and r["in_table"] == len(table) == r["expected_in_table"]
    assert r["counters"]["repeats"] >= 1 and r["counters"]["overwritten"] > 5
    q = r["program_minus_capture_ms"]
    assert 0.011 < q["min"] <= q["p50"] <= q["max"] < 0.016
    assert r["split_in_key"] > 0 and r["split_between_packets"] > r["split_in_key"]
    assert r["packets_over_one_segment"] > 0 and r["ids_behind_the_first_segment"] > 0
    assert r["share_matched"] == 1.0
    assert r["share_of_capture"] == r["matched_with_a_right_time"] / (r["expected_in_table"] + r["never_whole"]) and r["never_whole"] == r["split_between_packets"]
    assert r["run_stats"] == {"runs": 400, "ns_per_run": 1000.0} and r["container_sees_the_table"]
    t = lc.report(r)
    assert "All holds" in t and f"{len(whole):,}" in t
    assert lc.main.__module__ == "live_check"


def drop(table, counters):
    table.pop(next(iter(table)))


def add(table, counters):
    table[-1] = ("00000000000000aa", T0 - OFFSET)


def late(table, counters):
    for s, (x, ns) in list(table.items())[:20]:
        table[s] = (x, ns + 3_000_000)


def most_late(table, counters):
    for s, (x, ns) in list(table.items()):
        table[s] = (x, ns + 700_000)


def one_very_late(table, counters):
    s, (x, ns) = list(table.items())[-1]
    table[s] = (x, ns + 40_000_000)


def early(table, counters):
    s, (x, ns) = next(iter(table.items()))
    table[s] = (x, ns - 1_000_000)


def count(table, counters):
    counters["ids"] -= 1


def errors(table, counters):
    counters["errors"] = 1


@pytest.mark.parametrize("edit, says", [(drop, "1 ids of the capture are not in the table"), (add, "1 ids in the table that the capture does not have"), (late, "ms after the capture's"),
                                        (early, "ms after the capture's"), (count, "counter ids"), (errors, "errors 1"),
                                        (most_late, "matched with a right time, under 99 %"), (one_very_late, "at most 40.")])
def test_what_is_wrong(tmp_path, edit, says):
    build(tmp_path, edit=edit)
    r = lc.compare(str(tmp_path))
    assert any(says in p for p in r["problems"]), r["problems"]
    assert "FAILED" in lc.report(r)


@pytest.mark.parametrize("name, text, says", [("after.txt", "qdisc clsact ffff: parent ffff:fff1", "not as it was before"), ("pins.txt", "arrivals", "pins left"),
                                              ("pod.txt", "uid docker://a restarts=0\nuid docker://b restarts=1\n", "the pod changed"),
                                              ("left.txt", "77 /tmp/slobudget-arrivald -pid 1", "left on the node"), ("arrivald.log", "", "counted no runs"),
                                              ("dump-container.csv", "", "agent's rights")])
def test_left_behind(tmp_path, name, text, says):
    build(tmp_path, n=60)
    (tmp_path / name).write_text(text)
    assert any(says in p for p in lc.compare(str(tmp_path))["problems"])


def test_microsecond_capture_and_other_files(tmp_path):
    f = [(T0 + 1500, Conn(1).packet(b"abc"))]
    (tmp_path / "x.pcap").write_bytes(capture(f, nano=False))
    assert [t for t, _, _ in lc.read_pcap(str(tmp_path / "x.pcap"))] == [T0 + 1000]
    (tmp_path / "y.pcap").write_bytes(b"\0" * 40)
    with pytest.raises(ValueError):
        list(lc.read_pcap(str(tmp_path / "y.pcap")))
    (tmp_path / "d.csv").write_text("id,00,1\n")
    with pytest.raises(ValueError):
        lc.read_dump(str(tmp_path / "d.csv"))


def test_connection_from_before_the_capture(tmp_path):
    c = Conn(5)
    fl = lc.Flow()
    for i in range(4):
        xid = f"{i + 1:016x}"
        fl.add(c.seq, h2(1 + 2 * i, b"hdr", 1, 4) + h2(1 + 2 * i, grpc(request(xid, 50)), 0, 1), i)
        c.seq = (fl.next or 0)
    import collections
    assert [r[0] for r in lc.requests(fl, collections.Counter())] == [f"{i + 1:016x}" for i in range(4)]


def test_a_retransmission_must_keep_the_first_time(tmp_path):
    build(tmp_path)
    r = lc.compare(str(tmp_path))
    assert r["problems"] == [] and r["counters"]["repeats"] >= 1
    # the id that came twice, with its second packet's time
    seen, again = set(), None
    import collections
    for _, _, _, _, _, data, _ in lc.packets(str(tmp_path / "capture.pcap"), collections.Counter()):
        for m in lc.FOUND.finditer(data):
            again = again or (m[1].decode() if m[1] in seen else None)
            seen.add(m[1])
    lines = (tmp_path / "dump.csv").read_text().splitlines()
    lines = [f"id,{again},{int(ln.split(',')[2]) + 2_000_000}" if ln.startswith(f"id,{again},") else ln for ln in lines]
    for name in ("dump.csv", "dump-container.csv"):
        (tmp_path / name).write_text("\n".join(lines) + "\n")
    assert any("do not have their first packet's time" in p for p in lc.compare(str(tmp_path))["problems"])
