# FUSE Mode — Design Decisions (Phase 0)

**Status:** All phases complete. See §10 for the phase table, the three
deviations from what is designed below, and what is still not guaranteed.
**Date:** 2026-07-31
**Scope:** semantic decisions forced by exposing the synced tree as a real filesystem.

| Phase | State |
|---|---|
| 0 — design decisions | complete (this document) |
| 1 — metadata catalog | complete: `app/core/catalog.py`, 115 tests green on Windows and Linux |
| 2 — FUSE read path | not started |
| 3 — FUSE write path | not started |
| 4 — conflicts | not started |
| 5 — measurement | not started |
| 6 — tests and docs | partial (115 of the >= 60 target; FUSE and two-node tests outstanding) |

Every decision below states what we chose, the alternative we rejected, and why.
Where a claim is measured, the measurement is shown. Where something could not be
verified, it is listed in [§10 Not verified](#10-what-i-could-not-verify).

---

## 0. Environment verification (measured, not assumed)

Probed `Ubuntu-24.04` under WSL2 on this machine:

| Check | Result |
|---|---|
| Kernel | `6.6.87.2-microsoft-standard-WSL2` |
| `/dev/fuse` | present, `crw-rw-rw-` |
| `fuse` in `/proc/filesystems` | yes (`fuse`, `fuseblk`, `fusectl`) |
| libfuse3 | `3.14.0-5build1` installed |
| `fusermount3` | `/usr/bin/fusermount3` |
| `/etc/fuse.conf` `user_allow_other` | present |
| `python3-pyfuse3` | **3.3.0-0.1** available in `noble/universe` (not yet installed) |
| Python | 3.12.3 |
| Root filesystem | ext4 on `/dev/sdd` |
| `/mnt/c` | **9p / drvfs** |
| CPU / RAM | 22 cores / 7.6 GB |
| Present | `git`, `docker` (Docker Desktop passthrough) |
| **Missing** | `gcc`, `make`, `autoconf`, `automake`, `pkg-config`, `fio`, `pip` |

Consequences that shape the plan:

- **FUSE genuinely works here.** Phases 2–5 are viable under WSL2.
- **Benchmarks must run on ext4, never under `/mnt/c`.** `/mnt/c` is a 9p mount;
  measuring a FUSE overlay on top of 9p would report the cost of 9p, not of our
  design. Backing store and mount point both go in the WSL home directory.
- **`python3-pyfuse3` from apt avoids needing a C toolchain for pyfuse3 itself.**
  But apt packages land in `/usr/lib/python3/dist-packages`, invisible to a plain
  venv — so the venv must be created with `--system-site-packages`. The
  alternative (`pip install pyfuse3`) compiles a Cython extension and needs
  `build-essential`, `pkg-config`, `libfuse3-dev`. Proposed: **apt first,
  pip as fallback**, both documented.
- **pjdfstest needs a toolchain we do not have** (`autoconf`, `gcc`, `make`).
  Phase 5 will `apt install` them. Flagged now so it is not a surprise.
- **`fio` is absent**; Phase 5 metadata benchmarks will use a hand-rolled harness
  unless you want the apt dependency.

**Daemon-mode baseline on Windows, measured today: 35 test functions, 35 passed,
2.28s.** Your brief said 21 — that number is stale. The Phase 6 target of ">= 60"
is therefore **+25 tests, not +39**. I will hold the >= 60 absolute target.

---

## 1. Four blocking defects found while reading the code

These are not style complaints. Each one breaks a thing Phase 3 explicitly
requires, so each needs a decision *before* implementation. All four are
reproduced with runnable probes.

### 1.1 Merkle diff silently returns EMPTY when the local file has identical adjacent chunks

`merkle.py:248` builds a parent with `children=[left, right] if left != right else [left]`.
`MerkleNode.__eq__` compares **by hash**, so two *distinct positions* holding
*identical content* collapse into a single child. `_find_diff_recursive`
(`merkle.py:326`) then compares children **positionally**, so an asymmetric
collapse misaligns the walk. The `elif` at `merkle.py:332` only records a chunk
when it exists in the **local** tree, so misaligned entries are dropped silently.

Measured (local=A, remote=B, 4KB chunks):

```
[BUG] 2 chunks, A=[Z,Z] B=[Z,Q]  (local siblings identical)
      expected=[1]  got=[]
[OK ] 2 chunks, A=[Z,Q] B=[Z,Z]  (remote siblings identical)
      expected=[1]  got=[1]
[BUG] 4 chunks, A=[Z,Z,Z,Z] B=[Z,Z,Z,Q] (sparse tail edit)
      expected=[3]  got=[]
[BUG] 4 chunks, A=[Q,R,Q,R] B=[Q,R,Q,Z]
      expected=[3]  got=[]
[OK ] baseline all-distinct, A=[Q,R] B=[Q,Z]
      expected=[1]  got=[1]
```

`Z` is a zero-filled 4KB chunk. **Any sparse file, any zero-padded file, any file
with repeated blocks hits this**, and the remote edit is never fetched. Phase 3
requires sparse-file support, so this must be fixed first.

**Fix (an extension, not a rewrite):** always emit both children
(`children=[left, right]`), and make the odd-node case explicit rather than
aliasing the last node to itself. Then `_find_diff_recursive` also needs the
missing branch: when `remote_child` exists and `local_child` does not, record the
**remote** chunk index. Leaf-level hashes and the root hash are unchanged by
this, so the wire format and existing tests are unaffected.

### 1.2 Files that grow never sync their tail

Same root cause, different symptom. Local 1 chunk, remote 3 chunks, identical
prefix:

```
diff.changed_chunks = [0]   <-- expected {1, 2}
```

It reports chunk 0 (which is *identical*) as changed and **misses chunks 1 and 2
entirely**. Appending to a file — the single most common write pattern, and
explicitly in scope for Phase 3 (`O_APPEND`, writes past EOF) — does not
replicate correctly today.

### 1.3 Receiving one chunk truncates the whole file

`sync_engine.py:441` opens the destination with mode `'wb'`, which truncates to
zero. `_handle_chunk_data` (`sync_engine.py:423`) calls `_write_chunks` **on every
single chunk received**, so a multi-chunk transfer rewrites the file once per
chunk, each time from empty. Measured:

```
before: size=12288
after receiving only chunk 2: size=12288  bytes[0:8192] all zero? True
-> the two chunks we did NOT receive became a zero-filled hole.
```

Any partial or interrupted transfer replaces not-yet-received data with zeros.

**Fix:** open `'r+b'` (create-if-absent, no truncate), and only write once the
expected chunk set — which the Phase 1 catalog will finally let us record — is
complete. This is precisely the gap the current code admits to in its own comment
(`"In production, track expected chunks count"`).

### 1.4 Vector clocks can never converge, so sync ping-pongs forever

`VectorClock.merge()` (`vector_clock.py:92`) takes the element-wise maximum and
**then increments the merger's own counter**. That extra tick makes the receiver
strictly `AFTER` the sender, so the receiver now advertises a "newer" version
back. Measured:

```
A edits.            A={'A': 1}  B={'B': 0}        A vs B = after
B merges A's clock. A={'A': 1}  B={'B':1,'A':1}   B vs A = after
A merges B's clock. A={'A':2,'B':1} ...           A vs B = after
B merges again.     A={'A':2,'B':1} B={'B':2,'A':2}  B vs A = after
-> each merge makes the merger strictly AFTER; never EQUAL.
```

With a plain element-wise max (no increment), the same exchange converges to
`equal` on the first try, and a genuine concurrent edit still reports
`concurrent`. Both verified.

**Fix:** `merge()` becomes pure element-wise max. The self-increment moves to the
call sites that represent an actual *local event* — which is what the vector-clock
rule (`increment on local event; max on receive`) actually says. `merge()` has one
caller in tests and three in `sync_engine.py`; all three are receive-paths that
should not tick.

> **This one needs your explicit sign-off.** It changes observable behaviour of a
> core algorithm, and one existing test asserts the current semantics. It is
> inside "extend, don't rewrite" in my reading — a 2-line change to restore the
> textbook rule — but it is your call. **If you say no, FUSE mode cannot converge
> and Definition-of-Done #2 is unreachable.**

---

## 2. Decision 1 — Write coalescing

**Decision: coalesce per open file handle. Emit a sync event on `release()`, on
`fsync()`, or when the handle accumulates 64 dirty 4KB chunks (256KB) or 2s of
write idleness — whichever comes first.**

### Why the usual objection does not apply here

The brief frames this as "per-release is coherent but loses data on crash."
For this design that is not true, because **FUSE mode writes through to a real
backing directory.** A `write()` has already hit local disk before we decide
anything. What coalescing defers is *replication*, not *persistence*.

So the crash exposure is: work that was durably on local disk but not yet
announced to peers. And the Phase 1 `sync_journal` — written in the **same SQLite
transaction** as the dirty-chunk record — recovers exactly that on restart. The
crash window is a replication delay, not a data loss.

### Why not per-`write()`

Chatty to the point of unusable. A 100MB sequential write in 4KB chunks is 25,600
`write()` calls; per-write announcement means 25,600 `SYNC_REQ` broadcasts, each a
JSON round trip, each rebuilding a Merkle tree. Building the tree for a 100MB file
measured **8.48s** — doing that per write is not a slow design, it is a
non-functional one.

### Why not pure per-`release()`

A long-lived writer that never closes — a log file, a database, `tail -f`'s
producer, `vim`'s swap file — would never replicate at all. The 64-chunk / 2s
watermark bounds this. It is the reason the decision is "coalesce", not
"per-release".

### Rejected: per-`fsync()` only

Most applications never call `fsync`. `cp`, `cat >`, and most editors do not.
Replication would be effectively random.

**Guarantee stated plainly:** a write is replicated within 2s of the writer going
idle, or immediately on close/fsync. It is durable locally the moment `write()`
returns to the caller and the backing store accepts it.

---

## 3. Decision 2 — Materialisation

**Decision: eager / full replication. A path is visible in the mount only once its
bytes are local. `open()` on a not-yet-replicated file returns `ENOENT`.**

Invariant: *if it is in the catalog and not a tombstone, its bytes are on local
disk.* `read()` never touches the network.

### Why not lazy fetch on `open()`

Two reasons, one semantic and one structural.

- **Semantic:** `getattr` would have to answer for a file whose bytes we do not
  have. Report the peer's size and we lie until the fetch lands; report 0 and
  `cp`/`git` will happily copy an empty file and consider it done. There is no
  honest answer.
- **Structural, and this is the decisive one:** lazy fetch means a FUSE read
  handler blocks on a network round trip. Per [§7](#7-the-trioasyncio-boundary),
  the read path is the *one* path guaranteed never to cross into the asyncio loop.
  Making `read()` await the engine reintroduces exactly the deadlock the whole
  threading design exists to prevent, and adds an unbounded hang with no timeout
  policy — an application blocked in `read()` on a dead peer is unkillable
  (uninterruptible sleep).

`ENOENT` until the file arrives is worse UX than a stub, but it is *POSIX-shaped
and non-blocking*: callers already handle it, and it never wedges a process.

**Rejected: the Dropbox "smart sync" placeholder model** (entry visible, bytes
fetched on first read). It is the right long-term answer and I would revisit it
once the read path is measured and stable. It is out of scope for this pass and
listed as future work, not silently dropped.

---

## 4. Decision 3 — Conflict presentation

**This is the decision the brief calls the most important one, and I agree.**

**Decision: the Dropbox sibling model. `f.txt` always exists and is always
readable; the losing version appears beside it as
`f.txt.conflict-<loser_node_id>-<unix_ts>`. The winner is chosen deterministically
by node_id, not by timestamp.**

### Why not `EIO` on `open()`

`EIO` is the theoretically honest answer — "these two versions are incomparable,
I refuse to guess" — and I rejected it anyway, for three reasons:

1. **It breaks every tool.** `git status` errors out. `cp -r` aborts mid-tree.
   `tar`, `rsync`, and every backup agent fail. A conflict in one file poisons
   operations over the entire directory.
2. **It gives the user no way out.** They cannot read either version to decide.
   Resolution would require a CLI — which directly contradicts the stated point of
   mounting: *"a conflict must be visible through the filesystem without the user
   running a CLI."* `EIO` makes the filesystem the one place you *cannot* resolve it.
3. **It is not POSIX-legal in spirit.** `EIO` means a hardware/transport error.
   The file exists and both versions are intact on disk. Reporting a media error
   for a successful-but-ambiguous replication is a lie to the application.

The sibling model surfaces the conflict as a **directory entry** — the most
filesystem-native signal available. `ls` shows it. Tab-completion shows it. A file
manager shows it. No CLI, no daemon query, no log grep.

### Deterministic winner selection — and why not last-write-wins

When clocks compare `CONCURRENT`, both peers must independently reach the **same**
verdict with no further communication, or they diverge permanently.

**Not last-write-wins.** LWW depends on wall-clock timestamps across machines with
unsynchronised clocks. Two peers can disagree about which write was "last" and
each keep a different winner — silent permanent divergence, the worst possible
outcome. The current code's conflict path is LWW-flavoured
(`sync_engine.py:502` "Accept remote version").

**Chosen rule:** on `CONCURRENT`, the version whose originating `node_id` sorts
lexicographically **lower** wins `f.txt`. The other is written to
`f.txt.conflict-<loser_node_id>-<ts>`. Both peers compute this identically from
data they already have. `<ts>` comes from the **loser's own** recorded mtime, not
from local wall-clock, so both peers generate the *same filename*.

This is arbitrary — but arbitrary-and-identical is exactly what convergence
requires, and unlike LWW it cannot disagree.

### Rules that keep this from exploding

- The conflict sibling is **a normal replicated file**. Both peers converge to
  identical directory contents. (Daemon mode's existing `_conflict` backups stay
  local-only and ignored — see below.)
- A path already matching `*.conflict-*-*` **never generates a conflict sibling of
  its own**. Without this, a conflict on a conflict file recurses.
- Resolution is ordinary filesystem work: the user `mv`s the version they want over
  `f.txt` and deletes the other. That is a normal write + a normal unlink; no
  special-casing.

### Not regressing existing behaviour (Phase 4 requirement)

Daemon mode keeps `_handle_conflict`'s current `<path>_conflict_<node>_<ts>` backup
naming and its local-only, ignored status — **unchanged**. FUSE mode uses the new
`.conflict-` naming and replicates it. The two naming schemes coexist; the new
pattern is added to the ignore filters for daemon mode only. Stated explicitly
because it is a real behavioural difference between the two modes, not an accident.

---

## 5. Decision 4 — Deletion, tombstones, and GC

**Decision: `unlink()` writes a tombstone carrying its own advanced clock, in the
same transaction as removing the file row and the backing file.**

Without a tombstone, a peer that missed the delete re-advertises the file and it
resurrects. The current code has no tombstones at all — `_handle_file_delete`
(`sync_engine.py:466`) simply returns if the file is already gone locally, so
there is nothing to out-vote a stale re-advertisement.

### Semantics

- `unlink` → bump the path's clock by one local tick → write
  `tombstones(path, clock, deleted_at)`, delete `files`/`chunks`/`clocks` rows,
  unlink the backing file, append to `sync_journal` — **one transaction**.
- Incoming advertisement for a tombstoned path:
  - tombstone `AFTER` remote clock → re-broadcast the tombstone. Peer deletes.
  - tombstone `BEFORE` remote clock → the file was legitimately recreated. Drop
    the tombstone, accept the file.
  - **`CONCURRENT`** (deleted here, edited there) → **the edit wins; the file is
    resurrected.** Rationale: an unwanted resurrection is visible and trivially
    undone; a silently lost edit is neither. This matches Syncthing and Dropbox.
    The resurrected file does *not* get a conflict sibling — there is only one
    surviving version.
- The `inodes` row is **retained** for a tombstoned path so a recreate reuses the
  same inode number. Inode numbers are never recycled to a *different* path.

### Lifetime and GC — with an honest limit

- Default `tombstone_ttl_days = 30`, configurable.
- Hourly GC task plus one sweep at startup.
- A tombstone is collected when `deleted_at < now - TTL` **and** every currently
  configured peer has acknowledged a clock `>=` the tombstone clock.
- Unconditional hard limit: `deleted_at < now - 180 days` collects regardless of
  acks, otherwise one permanently dead peer pins tombstones forever and the
  catalog grows without bound.

**The limit, stated rather than hidden:** a peer that is offline for longer than
the hard TTL and then reconnects **will resurrect files deleted during its
absence.** That is a real, accepted failure mode. The alternative — unbounded
tombstone retention — trades a rare resurrection for a guaranteed unbounded leak.
180 days is a tunable, not a claim of correctness.

---

## 6. Decision 5 — `rename()`

**What we actually guarantee, split by scope, because the honest answer differs:**

| Scope | Guarantee |
|---|---|
| **Local node, catalog vs backing store** | Atomic **within** each of the two, **not across** them |
| **Local node, as seen by applications** | **Atomic.** An observer sees old or new, never both, never neither |
| **Across peers** | **Not atomic.** A peer can transiently see both names or neither |

### Local

`os.rename()` on the backing store is atomic within a filesystem. The catalog
update (`files`, `chunks`, `clocks`, `inodes`, `sync_journal`) is one SQLite
transaction. **The pair is not atomic together** — a crash between them is
possible. Phase 1 crash recovery reconciles catalog against the real tree at
startup, which is exactly the case it exists to handle. Applications never observe
the intermediate state because FUSE handlers take the path lock across both.

Rename onto an existing target (POSIX atomic replace) and cross-directory rename
within the mount are both supported. The inode number is preserved locally.

### Across peers — and a protocol extension I want to add

The wire protocol has no rename message. The naive mapping is delete(old) +
create(new), and it is bad: **it re-transfers the entire file.** Renaming a 100MB
file would ship 100MB, destroying the exact Merkle-delta benefit the project
exists to demonstrate. It also loses causal history for the path.

**Proposal: add `FILE_RENAME` to `MessageType`.** The non-goals say *extend* the
protocol, not rewrite it — adding one message type is an extension, and it
preserves both the delta benefit and the clock chain.

**Compatibility caveat, which is a real problem:** the protocol is **not**
forward-compatible with unknown message types. `Message.from_dict`
(`protocol.py:85`) calls `MessageType(data['type'])`, which raises `ValueError` on
an unrecognised string; `receive_message` re-raises and the connection dies. So an
old peer does not gracefully ignore a new message — it **drops the connection**.

Therefore the extension ships with: `Protocol.VERSION` → `"1.1"`, a tolerant
decode path that maps unknown types to a logged-and-ignored sentinel, and
`FILE_RENAME` only sent to peers that advertised >= 1.1 in `HELLO`. Peers below
1.1 get delete + create. This needs your sign-off too, since it touches the
handshake.

Cross-peer rename remains **non-atomic** regardless. I am not going to claim
otherwise.

---

## 7. The trio/asyncio boundary

The brief is right that this is where deadlocks live. The design is built around
one rule.

```
  ┌──────────── FUSE thread ────────────┐      ┌──── main thread ────┐
  │  trio loop  ·  pyfuse3 handlers     │      │  asyncio loop       │
  │                                     │      │  SyncEngine         │
  │  lookup/getattr/readdir/open/read   │      │  NetworkManager     │
  │      └── catalog cache + backing ───┼──┐   │  aiosqlite          │
  │          store  (NEVER crosses)     │  │   └─────────────────────┘
  │                                     │  │            ▲
  │  create/write/unlink/rename/…       │  └── same files, direct FS access
  │      └── run_coroutine_threadsafe ──┼───────────────┘  (fire-and-forget)
  │          (does NOT block on result) │
  └─────────────────────────────────────┘
```

**Rule 1 — the read path never crosses the boundary.** `lookup`, `getattr`,
`opendir`, `readdir`, `open`, `read`, `release`, `statfs` are served entirely from
the in-process metadata cache and direct I/O on the backing store. They never
`await` the engine. This is also what makes Decision 2 (eager materialisation)
load-bearing rather than merely convenient.

**Rule 2 — mutations post, they do not wait.** Write-path handlers apply the
change to the backing store and catalog, then hand the replication work to asyncio
via `asyncio.run_coroutine_threadsafe(...)` and **do not block on the returned
future**. `fsync()` is the sole exception: POSIX requires a result, so it waits —
with a timeout, and it waits only on *local* durability, never on peer
acknowledgement.

**Rule 3 — the asyncio engine must never perform I/O through the mount point.**
It operates on the backing directory only. If the engine ever touched the mount, a
FUSE handler waiting on the engine while the engine waits on FUSE is an immediate,
unrecoverable deadlock. This is why the backing store is a *separate directory*
from the mount point, and it is the single most important structural constraint in
the design.

**Rule 4 — SQLite is owned by the asyncio loop.** `aiosqlite` connections are not
thread-safe. The FUSE thread reads its own **read-only** connection opened in WAL
mode (WAL permits concurrent readers alongside one writer); all writes go through
the asyncio loop. Two connections, one writer, no shared handles.

### Layout

```
~/.dfs/data/      backing store   <- SyncEngine operates here; daemon mode watches here
~/.dfs/catalog.db SQLite (WAL)
/mnt/dfs          FUSE mount      <- applications see this
```

Daemon mode is unaffected because it already watches a plain directory — in FUSE
mode that directory is simply the backing store. **No change to daemon mode's code
path.** FUSE mode is selected by `--fuse-mount <path>`; absent, behaviour is
byte-for-byte what it is today, on Windows and Linux alike.

### Inode allocation and `getattr` caching

- `inodes(ino INTEGER PRIMARY KEY, path TEXT UNIQUE)`, monotonic counter persisted
  in the catalog. Root is `ino=1` (pyfuse3 requirement). Assigned on first sight,
  **never recycled**, retained across tombstones — so a path keeps its inode across
  remounts, which is the stated requirement.
- `getattr` is served from an in-memory `ino -> attr` cache, invalidated on local
  mutation and on applying a remote update, with `pyfuse3.invalidate_inode` /
  `invalidate_entry` to drop the kernel's copy.
- `attr_timeout`/`entry_timeout` set to **1.0s** so the kernel absorbs the bulk of
  `getattr` traffic. **This means remote-originated changes can be up to 1s stale
  in the mount.** That is consistent with the eventual-consistency model below, and
  Phase 5 will report cached vs cold `getattr` latency separately so the effect is
  visible rather than assumed.

---

## 8. Decision 6 — Consistency model, stated plainly

**This is an eventually consistent replicated filesystem. It is not POSIX, and it
is not linearizable. Read-after-write across peers is NOT guaranteed.**

What we **do** guarantee:

- **Read-your-writes on the writing node.** A `write()` followed by a `read()` on
  the same node returns the new data. Always.
- **Monotonic reads on a single node.** Data never goes backwards locally.
- **Convergence**, given eventual connectivity and a quiescent period: all peers
  reach identical content, including identical conflict siblings.
- **No silent lost updates.** Concurrent writes both survive — one as `f.txt`, one
  as a conflict sibling. Neither is discarded.
- **Local durability on `fsync()`.**

What we **do not** guarantee — each of these is a real, reachable behaviour:

- **Read-after-write across peers.** Write on A, immediately read on B: you may
  get the old bytes, or `ENOENT`. Propagation is bounded by the coalescing window
  plus network time, not by anything stronger.
- **Cross-peer `rename()` atomicity.** A peer may transiently see both names or
  neither.
- **`readdir` consistency across peers.** A listing reflects the *local* catalog.
- **`fsync()` durability across peers.** `fsync` returns when the data is durable
  *locally*. It says nothing about any peer.
- **Cross-peer ordering.** There is no total order on operations. Vector clocks
  give partial causal order only.
- **Up to 1s of metadata staleness** for remote-originated changes, from the
  kernel attribute cache (§7).

`docs/` and the README will carry this section verbatim. I would rather ship an
accurate description of a weaker system than an implied guarantee we cannot meet.

---

## 9. Phase 1 catalog — schema and index justification

Proposed schema, WAL mode, `synchronous=NORMAL`.

```sql
files      (path TEXT PRIMARY KEY, parent TEXT NOT NULL, name TEXT NOT NULL,
            ino INTEGER NOT NULL, size INTEGER, mtime REAL, mode INTEGER,
            chunk_count INTEGER, root_hash TEXT, is_dir INTEGER NOT NULL DEFAULT 0)
chunks     (path TEXT, idx INTEGER, hash TEXT NOT NULL, PRIMARY KEY (path, idx))
clocks     (path TEXT, peer_id TEXT, counter INTEGER NOT NULL,
            PRIMARY KEY (path, peer_id))
tombstones (path TEXT PRIMARY KEY, clock TEXT NOT NULL, deleted_at REAL NOT NULL)
inodes     (ino INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE)
sync_journal (id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, path TEXT,
            clock TEXT, ts REAL, applied INTEGER NOT NULL DEFAULT 0)
```

Index justification — **including one of yours I think is redundant, and two
additions:**

| Index | Justification |
|---|---|
| `files(path)` — the PK | Every FUSE op resolves by path. Free via PK. |
| **`files(parent, name)`** — *addition* | `readdir` needs "children of X". Without a stored `parent`, that is `LIKE 'dir/%'` — a full scan per `readdir`, and `readdir` is hot. Makes it O(children). |
| `chunks(path, idx)` — the PK | Delta lookup by chunk, and ordered range scan by path to rebuild a tree. Exactly your `(path, idx)`. |
| `clocks(path, peer_id)` — the PK | Clock reads are always "all peers for one path"; `path` is the leftmost PK column so the prefix scan is covered. |
| ~~`clocks(path)`~~ | **Redundant.** `path` is already the leftmost column of the `(path, peer_id)` PK, so SQLite uses that index for `path`-only lookups. A standalone index would cost write throughput and buy nothing. Dropping it unless you disagree. |
| `tombstones(path)` — the PK | Resurrection check on every incoming advertisement. |
| **`tombstones(deleted_at)`** — *addition* | Hourly GC scans by age. Without it, GC is a full table scan each hour. |
| **`inodes(path)`** UNIQUE — *addition* | FUSE addresses by `ino`, the catalog by `path`. Both directions are needed; PK covers `ino`, this covers `path`. |
| `sync_journal(applied, id)` | Recovery replays unapplied entries in order. |

**Atomicity.** Every mutation — data + `chunks` + `clocks` + `sync_journal` — is
one `BEGIN IMMEDIATE` … `COMMIT`. A partially applied vector clock is corruption,
as the brief says, so clock rows never move independently of the data they
describe.

**Crash recovery.** At startup, walk the backing tree and compare `(size, mtime)`
against `files`. Re-hash **only** disagreements. Files in the catalog but absent
from disk become tombstones; files on disk but absent from the catalog are
ingested as new. Unapplied `sync_journal` entries are replayed.

**Migration.** No catalog is a valid state: build it on first run by ingesting the
existing tree. Existing daemon-mode users lose nothing and need no action.

---

## 10. Verification status

Updated at the end of Phase 7. Verified items show what was actually run.

### Phase status

| Phase | Scope | Status |
|---|---|---|
| 0 | Security audit | Done — `docs/SECURITY_AUDIT.md`, 12 findings |
| 1 | Design decisions (this document) | Done |
| 2 | SQLite catalog, wired into the engine | Done — `app/core/catalog.py`, `SyncEngine.initialize` |
| 3 | FUSE read path | Done — `app/fuse/operations.py` |
| 4 | FUSE write path + Merkle wiring | Done — `SyncEngine.apply_local_write` |
| 5 | Mutual auth, encryption, path sanitisation | Done — `app/core/security.py` |
| 6 | Benchmarks + pjdfstest | Done — `artifacts/fuse_benchmark.md` |
| 7 | Test suite expansion | Done — 21 → 269 tests |

### Deviations from the design as written above

Three things ended up different from what this document originally specified.
They are recorded here rather than quietly edited into the sections above, so
the difference between what was designed and what was built stays visible.

1. **`InodeTable` uses its own synchronous SQLite connection**
   (`app/fuse/inodes.py`), not the async catalog on the engine's loop. §7 said
   everything crossing the boundary would go through `run_coroutine_threadsafe`.
   Inode lookup is on the critical path of every `lookup` and `getattr`, and
   posting each one to another thread's event loop would put a scheduling hop
   in front of the single hottest operation. A second connection to the same
   database in WAL mode is safe for this access pattern — one writer, and the
   FUSE thread is the only one that touches these two tables.

2. **The conflict sibling's suffix is the loser's clock counter, not a
   timestamp.** §4 wrote `<path>.conflict-<node>-<ts>`. It is actually
   `<path>.conflict-<node>-<counter>`. A timestamp would make the name depend
   on the clock of whichever machine generated it, so the two peers could name
   the same conflict differently and each end up with a sibling the other does
   not have. The counter is already part of the causal history both sides
   agree on, which makes the name derivable identically on both peers — the
   same reason the winner is chosen by node id rather than by time.

3. **Debounce coalescing in the watchdog bridge is trailing-edge.** Not
   specified here originally; called out because the original leading-edge
   behaviour silently dropped writes. See `AsyncFileMonitor._schedule_callback`.

### Verified

1. **pyfuse3 imports.** `3.3.0` from `python3-pyfuse3`, with `trio 0.24.0`,
   `ROOT_INODE == 1`. Note it is a compiled extension module, not a package, so
   there is no `pyfuse3.asyncio` submodule in this version — the asyncio bridge
   must be hand-written, as §7 already assumes.
2. **A FUSE mount works under WSL2.** A minimal read-only filesystem was
   mounted, listed, read, stat'd and unmounted cleanly:
   ```
   dfs_smoke on /root/smokemnt type fuse (rw,nosuid,nodev,relatime,...)
   -rw-r--r-- 1 root root 19 Jan  1  1970 hello.txt
   mounted under WSL2
   2 19 -rw-r--r-- /root/smokemnt/hello.txt
   UNMOUNTED CLEANLY
   ```
3. **`aiosqlite` installed and working** on both platforms: `0.22.1` with SQLite
   `3.45.3` on Windows, `3.45.1` under WSL2. WAL mode confirmed by test.
4. **Daemon mode passes on both platforms after the §1 fixes** — 115 tests, green
   on Windows *and* Linux. Definition-of-Done #5 is met for Phase 1.

### Since verified (items 5–8 were open at the end of Phase 1)

5. **pjdfstest built and run.** `autoconf` installed, suite built from source,
   run against a real mount as root with `allow_other`. **4,111 / 8,798 =
   46.7%.** Every failure category is enumerated in
   `artifacts/fuse_benchmark.md` §5. The rate is low and the reason is
   specific: five operations are unimplemented by design (`symlink`, `link`,
   `mknod`, `mkfifo`, unix-socket `bind`), and pjdfstest runs most of its
   matrix across all seven POSIX file types.
6. **Benchmarks measured.** `artifacts/fuse_benchmark.md`, taken on ext4 under
   WSL2. Medians over repeated passes, with the spread reported — a
   single-shot version of this benchmark disagreed with itself by 6x.
7. **Two-peer Docker convergence verified — in Docker, not substituted.**
   `tests/test_docker_two_node.py`, 7 tests, against `docker compose`.
   Concurrent edits are made during a real bridge-network partition, because
   two sequential `docker exec` writes are not concurrent. FUSE inside the
   containers was *not* needed: the containers run daemon mode, which is what
   the compose file has always described.
8. **FUSE handlers are wired to the catalog.** `FuseMount` opens the inode
   table against the same database; mutations flow through
   `SyncEngine.apply_local_write`.

### Still not guaranteed

- **The Merkle delta figure is disappointing and the cause is known.** A
  1-byte edit in a 100MB file costs 1.75MB on the wire — 60x better than
  shipping the file, but three orders of magnitude worse than the edit. The
  cost is the leaf-hash list itself, not the data. See
  `artifacts/fuse_benchmark.md` §4, which names the fix.
- **Path length under the mount is shorter than `PATH_MAX`** by the difference
  between the backing-root and mount-point prefix lengths, because every
  operation is re-rooted into the backing store. Measured via pjdfstest's
  near-`PATH_MAX` cases.
- **`chmod`/`chown` by a non-owner returns `EACCES` where POSIX says `EPERM`.**
  We rely on the kernel's `default_permissions`, which checks access rights
  rather than ownership.

---

## Appendix — measurements taken during Phase 0

Serialized `MERKLE_TREE` message size vs the 10MB `Protocol.MAX_MESSAGE_SIZE`,
random distinct content, 4KB chunks:

```
   1MB | chunks=   256 | nodes=   511 | build= 0.07s | wire=  0.088MB |  1% of cap
  10MB | chunks=  2560 | nodes=  5121 | build= 0.84s | wire=  0.882MB |  8% of cap
 100MB | chunks= 25600 | nodes= 51202 | build= 8.48s | wire=  8.895MB | 85% of cap
```

**Two consequences.**

The Phase 5 headline benchmark — a 1-byte edit in a 100MB file — sends a
**8.895MB** tree message against a **10MB** cap. It fits, with 11% headroom. **Any
file above roughly 118MB exceeds the cap and fails outright.** Since the tree is
sent to *compute* the delta, the delta optimisation currently stops working
precisely where it matters most.

Mitigation (Phase 5, extension not rewrite): send the **leaf hash list** rather
than the fully serialized nested tree. The interior nodes are recomputable from
the leaves, so they are pure wire overhead — they account for ~50% of the payload
(51,202 nodes vs 25,600 leaves). A leaf-only encoding drops the 100MB message to
roughly 1.8MB and pushes the practical ceiling past 500MB. I will measure this
rather than assert it.

Also note `build=8.48s` for 100MB. Merkle tree construction is not free, and
per-write tree rebuilds (§2) would be catastrophic. This is the measurement behind
the coalescing decision.
