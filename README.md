# Convergence-CRDT

A distributed collaborative text editing engine built on an operation-based **RGA (Replicated Growable Array)** CRDT, written in pure Python 3.10+ (standard library only).

Multiple peers edit the same document concurrently — online or offline — and every replica is guaranteed to converge to the exact same text, regardless of the order, duplication or delay of network messages.

## Features

- **Vector clocks** — track causality (happens-before) and classify two events as `EQUAL`, `BEFORE`, `AFTER` or `CONCURRENT`.
- **Character-level RGA** — every character has a unique `CharID(session_id, sequence_number)` and is anchored to its left neighbour (`origin_left_id`). Deletions are tombstones.
- **Deterministic conflict resolution** — concurrent inserts at the same position are ordered by `(sequence_number, session_id)`, so every replica produces the same order.
- **Operation-based sync** — `InsertOp` / `DeleteOp`, serialized as JSON on the wire.
- **Causal buffer** — operations whose parent (or delete target) has not arrived yet are held back and applied as soon as the dependency shows up.
- **Offline mode & anti-entropy** — peers keep editing while disconnected; on reconnect they exchange digests of known operations and send each other what is missing.
- **Chaos network** — packet loss, duplication, random delays, shuffled and reversed delivery.

## Why it converges

1. A character's parent (`origin_left_id`) is fixed when it is created and never changes.
2. Siblings are sorted by a total order on `CharID`.
3. The document is a pre-order traversal of this tree, skipping tombstones.

The visible text is therefore a pure function of *the set of inserted characters* and *the set of deleted IDs* — independent of delivery order (**commutative**). Duplicate operations are ignored by operation ID and tombstones only grow (**idempotent**). Once every replica has received every operation, all texts are identical.

## Stress test

The simulation in `convergence_crdt.py`:

1. Four peers (A, B, C, D) start from the same empty document and go offline.
2. Each writes words at the beginning, middle and end of the text and deletes some letters.
3. Peers reconnect in random order, keep editing, randomly drop offline again, while the network shuffles, reverses, delays, duplicates and drops packets.
4. After anti-entropy, it asserts `docA == docB == docC == docD`.
5. As extra evidence, the full operation set is replayed into fresh replicas in 30 different permutations (reversed, shuffled, with duplicates) and each must yield the same text.

This is repeated for 200 random seeds.

## Usage

```bash
python convergence_crdt.py
```

Example output (seed 42):

```
Total operations: 134, concurrent (conflicting) operation pairs: 3970
Network: {'sent': 711, 'dropped': 84, 'delivered': 609}, anti-entropy rounds: 1

Final texts:
  A: '**##xMxxyvo endBelIyD-D txza  edDhyMD- l*#ez #xnd##lpIA# ha  y#zdyxAzy'
  B: '**##xMxxyvo endBelIyD-D txza  edDhyMD- l*#ez #xnd##lpIA# ha  y#zdyxAzy'
  C: '**##xMxxyvo endBelIyD-D txza  edDhyMD- l*#ez #xnd##lpIA# ha  y#zdyxAzy'
  D: '**##xMxxyvo endBelIyD-D txza  edDhyMD- l*#ez #xnd##lpIA# ha  y#zdyxAzy'

assert docA == docB == docC == docD -> PASSED in 200/200 random chaos scenarios (+30 permutation replicas each).
```

## Limitations

- Sibling lists are re-sorted on each insert and the document is rebuilt by a full traversal on every local edit — fine for small documents, not optimized for very large ones.
- Tombstones are never garbage-collected.
