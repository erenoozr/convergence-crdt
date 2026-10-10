"""Convergence-CRDT: an RGA-based distributed collaborative text editing engine.

Why it converges:
  * Each character's parent (origin_left_id) is fixed at creation and never changes.
  * Siblings are ordered by a TOTAL order on CharID (seq descending, session_id descending).
  * Document = pre-order DFS from the root. The visible text is therefore a function of
    {set of inserted characters} and {set of deleted IDs} only; it does not depend on
    the order in which operations arrive  -> commutative.
  * Inserts are keyed by ID, deletes go into a grow-only tombstone set, and duplicate
    packets are ignored by op_id  -> idempotent.
  * An operation whose dependency (parent / delete target) is missing waits in the
    CausalBuffer; once every operation is delivered, all replicas hold equal sets
    -> equal texts.
"""
from __future__ import annotations

import json
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Union


# ============================================================================
# STAGE 1: Vector clocks
# ============================================================================
class Order(Enum):
    EQUAL = "equal"
    BEFORE = "before"          # self -> other (happens-before)
    AFTER = "after"            # other -> self
    CONCURRENT = "concurrent"  # neither has seen the other
    CONFLICT = "concurrent"    # alias: concurrency = potential conflict


@dataclass
class VectorClock:
    counts: dict[str, int] = field(default_factory=dict)

    def get(self, sid: str) -> int:
        return self.counts.get(sid, 0)

    def tick(self, sid: str) -> "VectorClock":
        self.counts[sid] = self.get(sid) + 1
        return self

    def merge(self, other: "VectorClock") -> None:
        for sid, n in other.counts.items():
            if n > self.get(sid):
                self.counts[sid] = n

    def copy(self) -> "VectorClock":
        return VectorClock(dict(self.counts))

    def compare(self, other: "VectorClock") -> Order:
        keys = self.counts.keys() | other.counts.keys()
        le = all(self.get(k) <= other.get(k) for k in keys)
        ge = all(self.get(k) >= other.get(k) for k in keys)
        if le and ge:
            return Order.EQUAL
        if le:
            return Order.BEFORE
        if ge:
            return Order.AFTER
        return Order.CONCURRENT


# ============================================================================
# STAGE 2: Character-level RGA
# ============================================================================
@dataclass(frozen=True)
class CharID:
    session_id: str
    sequence_number: int

    def key(self) -> tuple[int, str]:
        # Lamport order first, session_id on ties: deterministic total order.
        return (self.sequence_number, self.session_id)


ROOT = CharID("", 0)


@dataclass
class RGAItem:
    id: CharID
    value: str
    origin_left_id: Optional[CharID]
    is_deleted: bool = False


class RGA:
    def __init__(self) -> None:
        self.items: dict[CharID, RGAItem] = {ROOT: RGAItem(ROOT, "", None)}
        self.children: dict[CharID, list[CharID]] = defaultdict(list)
        self.max_seq = 0

    def has(self, cid: CharID) -> bool:
        return cid in self.items

    def insert(self, cid: CharID, origin: CharID, value: str) -> bool:
        if cid in self.items:
            return False
        self.items[cid] = RGAItem(cid, value, origin)
        siblings = self.children[origin]
        siblings.append(cid)
        # Larger ID first: concurrent inserts at the same spot get the same order everywhere.
        siblings.sort(key=CharID.key, reverse=True)
        self.max_seq = max(self.max_seq, cid.sequence_number)
        return True

    def delete(self, cid: CharID) -> None:
        self.items[cid].is_deleted = True  # tombstone: the node stays in the tree so its children keep their position

    def visible_ids(self) -> list[CharID]:
        out, stack = [], [ROOT]
        while stack:
            cid = stack.pop()
            if cid != ROOT and not self.items[cid].is_deleted:
                out.append(cid)
            stack.extend(reversed(self.children.get(cid, ())))
        return out

    def text(self) -> str:
        return "".join(self.items[c].value for c in self.visible_ids())


# ============================================================================
# STAGE 3: Operation-based sync + CausalBuffer
# ============================================================================
@dataclass(frozen=True)
class InsertOp:
    char_id: CharID
    origin_left_id: CharID
    char_value: str
    clock: VectorClock

    @property
    def op_id(self) -> tuple[str, int]:
        sid = self.char_id.session_id
        return (sid, self.clock.get(sid))


@dataclass(frozen=True)
class DeleteOp:
    target_char_id: CharID
    clock: VectorClock
    session_id: str  # session that issued the delete, needed for op_id

    @property
    def op_id(self) -> tuple[str, int]:
        return (self.session_id, self.clock.get(self.session_id))


Op = Union[InsertOp, DeleteOp]


def encode(op: Op) -> str:
    d: dict = {"clock": op.clock.counts}
    if isinstance(op, InsertOp):
        d |= {"t": "ins", "id": [op.char_id.session_id, op.char_id.sequence_number],
              "left": [op.origin_left_id.session_id, op.origin_left_id.sequence_number],
              "v": op.char_value}
    else:
        d |= {"t": "del", "target": [op.target_char_id.session_id, op.target_char_id.sequence_number],
              "sid": op.session_id}
    return json.dumps(d)


def decode(data: str) -> Op:
    d = json.loads(data)
    clock = VectorClock({k: int(v) for k, v in d["clock"].items()})
    if d["t"] == "ins":
        return InsertOp(CharID(*d["id"]), CharID(*d["left"]), d["v"], clock)
    return DeleteOp(CharID(*d["target"]), clock, d["sid"])


class CausalBuffer:
    """Holds operations whose dependency (parent or delete target) has not arrived yet."""

    def __init__(self) -> None:
        self.waiting: dict[CharID, list[Op]] = defaultdict(list)

    def park(self, missing: CharID, op: Op) -> None:
        self.waiting[missing].append(op)

    def release(self, arrived: CharID) -> list[Op]:
        return self.waiting.pop(arrived, [])

    def __len__(self) -> int:
        return sum(map(len, self.waiting.values()))


class Replica:
    def __init__(self) -> None:
        self.rga = RGA()
        self.buffer = CausalBuffer()
        self.log: dict[tuple[str, int], Op] = {}
        self.clock = VectorClock()

    def apply(self, op: Op) -> None:
        if op.op_id in self.log:
            return  # idempotent: duplicate packet
        self.log[op.op_id] = op
        stack = [op]  # explicit stack instead of recursion: no depth limit on long buffered chains
        while stack:
            op = stack.pop()
            dep = op.origin_left_id if isinstance(op, InsertOp) else op.target_char_id
            if not self.rga.has(dep):
                self.buffer.park(dep, op)
                continue
            self.clock.merge(op.clock)
            if isinstance(op, InsertOp):
                self.rga.insert(op.char_id, op.origin_left_id, op.char_value)
                stack.extend(self.buffer.release(op.char_id))
            else:
                self.rga.delete(op.target_char_id)

    def text(self) -> str:
        return self.rga.text()


# ============================================================================
# STAGE 4: Peer session, offline mode and state exchange
# ============================================================================
class PeerSession(Replica):
    def __init__(self, name: str, network: "ChaosNetwork") -> None:
        super().__init__()
        self.name = name
        # uuid5: deterministic, so the same seed reproduces the same result
        self.session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"convergence-crdt:{name}"))
        self.network = network
        self.online = True
        network.peers.append(self)

    def _emit(self, op: Op) -> None:
        self.apply(op)
        if self.online:
            self.network.broadcast(self, encode(op))

    def insert(self, index: int, ch: str) -> None:
        vis = self.rga.visible_ids()
        if not 0 <= index <= len(vis):
            raise IndexError(index)
        left = vis[index - 1] if index else ROOT
        self.clock.tick(self.session_id)
        # max_seq+1: the new ID beats all existing children of the parent -> lands at the requested index
        cid = CharID(self.session_id, self.rga.max_seq + 1)
        self._emit(InsertOp(cid, left, ch, self.clock.copy()))

    def insert_text(self, index: int, s: str) -> None:
        for i, ch in enumerate(s):
            self.insert(index + i, ch)

    def delete(self, index: int) -> None:
        target = self.rga.visible_ids()[index]
        self.clock.tick(self.session_id)
        self._emit(DeleteOp(target, self.clock.copy(), self.session_id))

    def go_offline(self) -> None:
        self.online = False

    def go_online(self) -> None:
        self.online = True
        self.network.anti_entropy(self)

    def on_packet(self, data: str) -> None:
        self.apply(decode(data))

    # State exchange: our set of op_ids (digest) goes to the other side, missing ops come back.
    def digest(self) -> set[tuple[str, int]]:
        return set(self.log)

    def missing_for(self, digest: set[tuple[str, int]]) -> list[Op]:
        return [op for k, op in self.log.items() if k not in digest]


class ChaosNetwork:
    """Network with packet loss, duplication, delays and shuffled/reversed delivery."""

    MODES = ("shuffle", "reverse", "delay")

    def __init__(self, rng: random.Random, drop=0.15, dup=0.2, max_delay=5) -> None:
        self.rng, self.drop, self.dup, self.max_delay = rng, drop, dup, max_delay
        self.peers: list[PeerSession] = []
        self.in_flight: list[tuple[int, PeerSession, str]] = []
        self.stats = defaultdict(int)

    def send(self, dst: PeerSession, data: str) -> None:
        if self.rng.random() < self.drop:
            self.stats["dropped"] += 1
            return
        for _ in range(2 if self.rng.random() < self.dup else 1):
            self.stats["sent"] += 1
            self.in_flight.append((self.rng.randint(0, self.max_delay), dst, data))

    def broadcast(self, src: PeerSession, data: str) -> None:
        for p in self.peers:
            if p is not src and p.online:
                self.send(p, data)

    def anti_entropy(self, peer: PeerSession) -> None:
        for other in self.peers:
            if other is peer or not other.online:
                continue
            for op in other.missing_for(peer.digest()):
                self.send(peer, encode(op))
            for op in peer.missing_for(other.digest()):
                self.send(other, encode(op))

    def tick(self, mode: str) -> None:
        ready = [p for p in self.in_flight if p[0] <= 0]
        self.in_flight = [(d - 1, dst, data) for d, dst, data in self.in_flight if d > 0]
        if mode == "shuffle":
            self.rng.shuffle(ready)
        elif mode == "reverse":
            ready.reverse()
        else:  # delay: hold back some ready packets for another tick
            late = [p for p in ready if self.rng.random() < 0.5]
            ready = [p for p in ready if p not in late]
            self.in_flight += late
        for _, dst, data in ready:
            if dst.online:  # packets to an offline peer are lost; anti-entropy repairs this
                dst.on_packet(data)
                self.stats["delivered"] += 1

    def drain(self) -> None:
        while self.in_flight:
            self.tick(self.rng.choice(self.MODES))

    def converge(self) -> int:
        rounds = 0
        while True:
            self.drain()
            if len({frozenset(p.log) for p in self.peers}) == 1:
                return rounds
            for p in self.peers:
                self.anti_entropy(p)
            rounds += 1


# ============================================================================
# STAGE 5: Chaos simulation and final equality check
# ============================================================================
WORDS = {
    "A": ("Alpha ", "MID-A ", " endA"),
    "B": ("Bravo ", "MID-B ", " endB"),
    "C": ("Charlie ", "MID-C ", " endC"),
    "D": ("Delta ", "MID-D ", " endD"),
}


def chaos_simulation(seed: int, verbose: bool = False) -> str:
    rng = random.Random(seed)
    net = ChaosNetwork(rng)
    peers = [PeerSession(n, net) for n in "ABCD"]

    # 1) Everyone offline, same empty document.
    for p in peers:
        p.go_offline()

    # 2) Write words at the start / middle / end, delete some letters.
    for p in peers:
        start, mid, end = WORDS[p.name]
        p.insert_text(0, start)
        p.insert_text(len(p.text()) // 2, mid)
        p.insert_text(len(p.text()), end)
        for _ in range(rng.randint(2, 4)):
            p.delete(rng.randrange(len(p.text())))
    if verbose:
        print("Offline local texts:")
        for p in peers:
            print(f"  {p.name}: {p.text()!r}")

    # 3) Reconnect in random order; keep editing while online, drop connections
    #    now and then, deliver packets shuffled / reversed / delayed.
    for p in rng.sample(peers, len(peers)):
        p.go_online()
        net.tick(rng.choice(net.MODES))
    for _ in range(60):
        p = rng.choice(peers)
        r = rng.random()
        if r < 0.1:
            p.go_offline() if p.online else p.go_online()
        elif r < 0.65 or not p.text():
            p.insert(rng.randint(0, len(p.text())), rng.choice("xyz*#"))
        else:
            p.delete(rng.randrange(len(p.text())))
        if rng.random() < 0.4:
            net.tick(rng.choice(net.MODES))
    for p in peers:
        if not p.online:
            p.go_online()
    rounds = net.converge()

    # 4) Final equality.
    docA, docB, docC, docD = (p.text() for p in peers)
    assert docA == docB == docC == docD, (docA, docB, docC, docD)
    assert all(len(p.buffer) == 0 for p in peers)

    # Extra evidence: replay the same op set into fresh replicas in 30 permutations (incl. reversed + duplicates).
    ops = list(peers[0].log.values())
    for k in range(30):
        order = ops[::-1] if k == 0 else rng.sample(ops, len(ops))
        order += rng.sample(ops, len(ops) // 3)  # duplicate delivery
        r = Replica()
        for op in order:
            r.apply(op)
        assert r.text() == docA, f"permutation {k} produced a different result"

    if verbose:
        concurrent = sum(
            a.clock.compare(b.clock) is Order.CONCURRENT
            for i, a in enumerate(ops) for b in ops[i + 1:]
        )
        print(f"\nTotal operations: {len(ops)}, concurrent (conflicting) operation pairs: {concurrent}")
        print(f"Network: {dict(net.stats)}, anti-entropy rounds: {rounds}")
        print("\nFinal texts:")
        for p in peers:
            print(f"  {p.name}: {p.text()!r}")
    return docA


def self_test_vector_clock() -> None:
    a = VectorClock().tick("A")
    b = a.copy().tick("B")
    c = a.copy().tick("C")
    assert a.compare(b) is Order.BEFORE and b.compare(a) is Order.AFTER
    assert b.compare(c) is Order.CONCURRENT is Order.CONFLICT
    assert a.compare(a.copy()) is Order.EQUAL
    b.merge(c)
    assert c.compare(b) is Order.BEFORE


if __name__ == "__main__":
    self_test_vector_clock()
    print("Stage 1: VectorClock tests passed.\n")
    chaos_simulation(seed=42, verbose=True)
    n = 200
    for seed in range(n):
        chaos_simulation(seed)
    print(f"\nassert docA == docB == docC == docD -> PASSED in {n}/{n} random chaos scenarios "
          f"(+30 permutation replicas each).")
