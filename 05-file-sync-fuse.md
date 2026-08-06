# Distributed File Sync Engine — FUSE, persistence, security

> Prepend the shared preamble from `00-INDEX.md`.
> Scope: weeks, not a weekend. FUSE FORCES the persistence issue — a mount that
> loses Merkle and vector-clock state on remount is unusable, so the SQLite
> catalog is part of this work, not optional.

## Verified context
`app/algorithms/{merkle,vector_clock}.py`, `app/core/{protocol,config,logger}.py`,
`app/services/{file_monitor,network,sync_engine}.py`. Deps are ONLY `aiofiles`
and `watchdog`. **No database, no persistence of any kind — all state in memory.**
Custom length-prefixed binary TCP protocol using `struct`. 4KB chunks. 21 tests.

## PLATFORM
FUSE is Linux/macOS only. Target Linux under WSL2. The existing watchdog daemon
mode MUST keep working unchanged on both Windows and Linux — FUSE is an
ADDITIONAL mode behind a CLI flag, never a replacement.

## PHASE 0 — Security audit FIRST
1. **I believe there is NO authentication and NO encryption**, so any peer that
   connects can write files. Confirm and rate accordingly.
2. **Path traversal**: are received filenames sanitised? Literally send
   `../../../etc/passwd` and report what happens.
3. **Length-prefix framing**: is there a MAX length check in `core/protocol.py`?
   Unbounded allocation from an attacker-controlled prefix is a trivial DoS.
4. Is file application **atomic** (temp file + rename), or can a crash mid-apply
   leave a corrupted file?
5. Enumerate everything that breaks on restart given zero persistence.

## PHASE 1 — Design decisions (write `docs/FUSE_DESIGN.md`, then STOP)
Decide each explicitly, record the rejected alternative, get sign-off BEFORE
implementing:
1. **Write coalescing**: sync on every `write()`, or only `release()`/`fsync()`?
2. **Materialisation**: does `open()` on a peer-only file fetch lazily or require
   full replication? What does the caller see mid-fetch?
3. **Conflict presentation** — the most important decision here. When vector
   clocks report concurrent edits, what does the FILESYSTEM show?
   `f.txt` plus `f.txt.conflict-<peer>-<ts>` (Dropbox model), or `EIO` on open?
   This is where eventual consistency meets POSIX and the two disagree.
4. **Deletion**: unlink must propagate as a tombstone with its own clock entry,
   or deletes resurrect. Specify tombstone lifetime and GC.
5. **rename()**: POSIX requires atomicity. State what you actually guarantee.
6. **Consistency model**: say plainly that this is eventually consistent and
   read-after-write across peers is NOT guaranteed.

## PHASE 2 — Metadata catalog (prerequisite)
SQLite via aiosqlite, consistent with the asyncio codebase:
```
files(path, size, mtime, mode, chunk_count)
chunks(path, idx, hash)
clocks(path, peer_id, counter)
tombstones(path, clock, deleted_at)
sync_journal(op, path, clock, ts)
```
Indexes on `(path)`, `(path, idx)`, `(path, peer_id)` — justify each. WAL mode.
Atomic clock updates in ONE transaction; a partially applied vector clock is
corruption. On startup, reconcile against the real tree and re-hash ONLY files
whose `(size, mtime)` disagree.

## PHASE 3 — FUSE read path
Use **pyfuse3** (libfuse3, async), NOT fusepy — fusepy is synchronous and would
serialise against your asyncio engine.
- pyfuse3 runs on trio; the engine is asyncio. Run the FUSE loop in a dedicated
  thread with its own trio loop, bridging via `asyncio.run_coroutine_threadsafe`.
  Document this boundary carefully — it is where deadlocks will happen.
- Implement `lookup`, `getattr`, `opendir`/`readdir`, `open`, `read`, `release`,
  `statfs`.
- `getattr` is called constantly — cache inode metadata with explicit
  invalidation, or throughput dies.
- Stable inode allocation: the same path keeps its inode across remounts.

## PHASE 4 — FUSE write path
`create`, `write`, `truncate`, `unlink`, `mkdir`, `rmdir`, `rename`, `fsync`,
`setattr`. Wire writes into the existing Merkle chunker so only changed 4KB
chunks are re-hashed and shipped — do NOT re-hash whole files. Every mutation
bumps the local vector clock and appends to `sync_journal` in the SAME
transaction as the data change. Handle `O_APPEND`, partial writes, sparse files,
writes past EOF.

## PHASE 5 — Security fixes from Phase 0
Mutual authentication between peers (pre-shared key or mTLS), encryption in
transit, path sanitisation on every received filename, bounded length prefix.
A peer must not be able to write outside the synced root — prove by test.

## PHASE 6 — Measurement
`artifacts/fuse_benchmark.md`:
- Sequential read/write throughput vs the native filesystem — this WILL be
  slower; report the honest multiple
- Metadata ops/sec: stat, create, unlink
- `getattr` p50/p95/p99, cached vs cold
- Sync propagation: write on peer A -> visible on peer B, at 4KB / 1MB / 100MB
- **Merkle delta efficiency: bytes on the wire vs bytes changed for a 1-byte edit
  in a 100MB file.** This is the number that justifies the whole design.
- Run **pjdfstest** against the mount. Report the pass rate and enumerate every
  failure with a one-line reason. Do not chase 100% — "412/438, the 26 failures
  are hardlinks and xattrs which we do not implement" is more credible than a
  claim of full compliance.

## PHASE 7 — Tests (21 -> 60+)
Catalog crash mid-transaction and remount recovery; tombstone GC; FUSE tests
against a real temporary mount (not mocks); a two-node Docker integration test
(`docker-compose.yml` already exists) where concurrent edits on both peers
converge and the conflict surfaces correctly.

## Definition of done
1. `mount` works under WSL2; `ls`, `cat`, `vim`, `cp`, `git status` behave
   correctly inside it
2. Two Docker peers converge; a concurrent edit surfaces as a visible conflict
3. `artifacts/fuse_benchmark.md` has real numbers, including unflattering ones
4. pjdfstest pass rate reported with failures enumerated
5. Daemon mode still passes its full suite on Windows
6. Unauthenticated peers cannot write; path traversal blocked — both by test
