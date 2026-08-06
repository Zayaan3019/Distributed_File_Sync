# FUSE Benchmark & POSIX Conformance — Phase 6

Every number here was measured by `benchmarks/fuse_benchmark.py` and
`scripts/pjdfstest.sh` on the machine described below. Nothing is estimated,
scaled from another run, or carried over from an earlier version of this
document. Where a result is unflattering it is reported as measured, and where
I know the cause I say so rather than rounding it away.

## 1. Environment

| | |
|---|---|
| Kernel | `6.6.87.2-microsoft-standard-WSL2` |
| CPU | Intel Core Ultra 7 155H, 22 logical CPUs |
| Memory | 7.5 GiB |
| Python | 3.12.3 |
| pyfuse3 | 3.3.0 (on trio 0.24.0) |
| Backing filesystem | **ext4** |
| Chunk size | 4096 B |

The harness refuses to run under `/mnt/c`:

```python
if "/mnt/c" in str(root.resolve()):
    raise SystemExit(
        "Refusing to benchmark under /mnt/c: it is a 9p mount, and the "
        "numbers would measure 9p rather than this filesystem."
    )
```

`/mnt/c` is a 9p/drvfs mount. Benchmarking a FUSE overlay on top of 9p would
report the cost of 9p, and the result would look far worse than the design
deserves for a reason that has nothing to do with the design.

### On methodology

Throughput and metadata figures are **medians over repeated passes**, with the
full observed range printed beside them. This is not decoration. Two
consecutive single-shot runs of an earlier version of this harness disagreed by
6x on native write throughput at 16MB (368 MB/s, then 62 MB/s), and one of them
made FUSE come out *faster than the filesystem it sits on top of* — a physically
impossible result that a single-run benchmark would have reported with a
straight face. Native and FUSE also alternate *within* each repeat rather than
running as two separate batches, so a machine that gets busier partway through
penalises both equally instead of only whichever went second.

The ranges below are still wide. That is the honest state of the measurement,
not a claim of precision I do not have.

---

## 2. Sequential throughput vs the native filesystem

FUSE is slower. It was always going to be slower: every read and write crosses
into userspace and back, and the handler on the other side is Python.

| Size | native write | FUSE write | **write ×slower** | native read | FUSE read | **read ×slower** |
|---|---|---|---|---|---|---|
| 4 KB | 1.0 MB/s | 0.4 MB/s | **2.87×** | 11.8 MB/s | 2.3 MB/s | **5.11×** |
| 1 MB | 109.2 MB/s | 92.2 MB/s | **1.18×** | 648.8 MB/s | 393.3 MB/s | **1.65×** |
| 16 MB | 579.5 MB/s | 330.7 MB/s | **1.75×** | 1,418.6 MB/s | 807.2 MB/s | **1.76×** |
| 128 MB | 499.0 MB/s | 268.3 MB/s | **1.86×** | 1,434.3 MB/s | 575.1 MB/s | **2.49×** |

Observed ranges over 5 passes each:

| Size | native write | FUSE write | native read | FUSE read |
|---|---|---|---|---|
| 4 KB | 0.8 – 1.4 MB/s | 0.3 – 0.5 MB/s | 9.5 – 14.9 MB/s | 1.3 – 3.5 MB/s |
| 1 MB | 81.2 – 170.5 MB/s | 73.8 – 100.9 MB/s | 488.3 – 738.5 MB/s | 132.5 – 553.1 MB/s |
| 16 MB | 229.6 – 648.1 MB/s | 215.1 – 404.1 MB/s | 1,370.7 – 1,534.6 MB/s | 544.8 – 887.7 MB/s |
| 128 MB | 287.2 – 525.1 MB/s | 230.6 – 508.8 MB/s | 1,298.7 – 1,568.2 MB/s | 283.9 – 619.6 MB/s |

**The honest multiple is roughly 1.2×–2.9× on writes and 1.6×–5.1× on reads,
worst at small sizes.**

The shape is the interesting part. At 4KB the transfer is a single round trip
and the entire cost is the crossing — 5.1× on read is essentially pure
overhead, amortised over nothing. By 1MB the copy dominates and the penalty
falls to 1.2×–1.7×. It rises again at 128MB on reads (2.49×) because the native
read is being served out of page cache at 1.4 GB/s, a rate our handler cannot
approach no matter how well it is written.

**What this means in practice:** the FUSE mount is fine for editing documents
and source trees, and is the wrong tool for a build directory full of tiny
files or a database file.

---

## 3. Metadata operations

This is where FUSE hurts, and where the numbers are worst.

| Operation | native (ops/s) | FUSE (ops/s) | **×slower** |
|---|---|---|---|
| create | 12,427 | 276 | **45.1×** |
| stat | 75,414 | 2,518 | **30.0×** |
| unlink | 45,609 | 1,516 | **30.1×** |

Median of 3 passes over 2,000 files each.

276 creates/second is a genuinely bad number, and it is the one that governs
how the mount *feels*. `git status` on a large repository, `npm install`,
extracting a tarball — all of these are metadata-bound, and all of them will be
noticeably slower under the mount than on the backing directory.

The cause is structural rather than a missing optimisation: every `create`
is a round trip to a Python handler that opens the backing file, allocates an
inode through a SQLite lookup, invalidates a cache entry, and stats the result.

### `getattr` latency: cached vs cold

| | p50 | p95 | p99 |
|---|---|---|---|
| **warm** (kernel attribute cache hit) | 1.63 µs | 2.47 µs | 3.29 µs |
| **cold** (never-seen path, cache miss) | 436.72 µs | 706.47 µs | 919.44 µs |

3,000 warm samples, 1,500 cold. **The cold path is ~267× the warm path at p50.**

Reporting a single blended `getattr` number would have been meaningless. The
kernel's attribute cache absorbs the overwhelming majority of real `stat()`
traffic, so a blended figure would be close to the warm number and would imply
our handler is fast, when what it actually measures is the cache. The two
numbers describe two different things:

- **1.63 µs warm** is the value of the 1-second `ATTR_TIMEOUT`. It is the
  kernel answering without consulting us at all.
- **436 µs cold** is what our handler actually costs when asked.

Cold samples are taken against 1,500 distinct paths the kernel has never seen,
because the entry cache cannot answer for a name it has not resolved.

---

## 4. Merkle delta efficiency — the headline number

The design exists to avoid re-shipping files that barely changed. This is the
measurement that tests that claim, and it is measured by a counter sitting
below the protocol layer — it sees real framed, encrypted bytes on the socket,
including handshake and request overhead. Nothing here is asserted from theory.

**Test:** two authenticated peers converge on a 100MB file. Exactly one byte,
at offset 52,428,800, is flipped. The transfer is then counted to convergence.

| | |
|---|---|
| File size | 104,857,600 B (100 MB) |
| Bytes actually changed | **1** |
| Chunk size | 4,096 B |
| **Bytes on the wire** | **1,747,377 B (1.67 MB)** |
| Time to converge | 28.06 s |
| vs shipping the whole file | **60.0× less** |
| vs the size of the edit | **1,747,377× more** |

**Both of those last two rows are true, and the second one is the one that
matters.**

60× better than naive replication is a real win. But 1.67MB to move one byte is
not a good result, and the design does not get to claim credit for the first
number without owning the second.

### Where the 1.67MB actually goes

The arithmetic closes almost exactly:

| Component | Bytes | Share |
|---|---|---|
| 25,600 leaf hashes, SHA-256 as 64 hex chars | 1,638,400 | 93.8% |
| JSON overhead for the list (quotes + commas) | 76,800 | 4.4% |
| The one 4KB chunk, base64 | 5,464 | 0.3% |
| Sync request, clocks, framing, both directions | ~26,713 | 1.5% |
| **Total** | **1,747,377** | |

**98.2% of the traffic is the leaf-hash list. 0.3% is the data.** The chunking
is doing its job perfectly — exactly one chunk moved. The tree *exchange* is
the problem, and it costs the same 1.67MB whether one byte changed or nothing
did.

### What would fix it

Two changes, in increasing order of both payoff and effort:

1. **Send raw digests instead of hex.** 25,600 × 32 B = 819,200 B, base64 to
   ~1.09MB. Saves ~38% for a serialisation change and no protocol redesign.
2. **Descend the tree instead of shipping its leaves.** Exchange the root
   hash; if it matches, stop — zero bytes, which is the common case. If not,
   exchange the two children and recurse only into subtrees that differ. For a
   single changed chunk in a 25,600-leaf tree that is ~15 levels × 2 hashes ≈
   **under 2 KB**, roughly a 1000× improvement, and it makes the "nothing
   changed" case free. This is what the Merkle structure is *for*; the current
   code builds the tree and then throws away everything above the leaves.

Item 2 is the real answer. It is a protocol change and it is out of scope for
Phase 6, which was to measure — but the measurement is what makes the case for
doing it, and without the measurement the current design would have looked fine.

---

## 5. Sync propagation latency

Two authenticated peers over loopback, AES-256-GCM, cold (the receiver has no
prior version of the file).

| File size | Time to converge | Effective throughput |
|---|---|---|
| 4 KB | 0.059 s | 0.1 MB/s |
| 1 MB | 0.337 s | 3.0 MB/s |
| 100 MB | 39.69 s | 2.5 MB/s |

~2.5–3.0 MB/s for a first-time transfer over **loopback** is slow in absolute
terms, and the reason is visible in the design: chunks are sent one message per
4KB chunk, each base64-encoded (a 33% expansion) and separately framed and
encrypted. A 100MB file is 25,600 such round trips. Batching several chunks per
message, or sending chunk payloads as raw bytes rather than base64 inside JSON,
would both help substantially.

The 4KB case is dominated by fixed cost — 59 ms is the request/response
handshake for the exchange, not the transfer.

---

## 6. pjdfstest POSIX conformance

Built from source (`autoconf` + `make`), run as root against a real mount with
`allow_other`, via `scripts/pjdfstest.sh`.

### Result: **4,111 / 8,798 = 46.7%**

```
Files=238, Tests=8798, 240 wallclock secs
```

That is a low pass rate and I am not going to dress it up. It is also not the
whole picture, so here is the full breakdown rather than a single number.

#### First, a measurement error worth recording

The first run scored **36.7%**. The difference was not a code change — it was
that the mount lacked `allow_other`. pjdfstest performs most of its checks as
unprivileged uids (`-u 65534 ...`), and a FUSE mount without `allow_other` is
visible only to the mounting user, so every one of those checks failed with
`EACCES` before reaching our code. **36.7% was a pass rate for my mount options,
not for the filesystem.** The harness now sets `user_allow_other` and passes
`allow_other`, and refuses to be run the other way.

#### Per-syscall group

| Group | pass | fail | rate |
|---|---|---|---|
| chflags | 14 | 0 | 100.0% |
| granular | 7 | 0 | 100.0% |
| posix_fallocate | 1 | 0 | 100.0% |
| ftruncate | 79 | 10 | 88.8% |
| truncate | 74 | 10 | 88.1% |
| rmdir | 111 | 34 | 76.6% |
| utimensat | 93 | 29 | 76.2% |
| mkdir | 85 | 33 | 72.0% |
| open | 237 | 100 | 70.3% |
| symlink | 48 | 47 | 50.5% |
| rename | 2,450 | 2,407 | 50.4% |
| unlink | 189 | 251 | 43.0% |
| link | 140 | 219 | 39.0% |
| chmod | 120 | 207 | 36.7% |
| mkfifo | 42 | 78 | 35.0% |
| mknod | 54 | 132 | 29.0% |
| chown | 367 | 1,130 | 24.5% |

#### Every failure, by cause

All 4,687 failures, grouped by the errno actually returned:

| errno | count | one-line reason |
|---|---|---|
| `ENOSYS` | 947 | **Not implemented, by design.** `mknod` (341), `symlink` (225), `mkfifo` (166), unix-socket `bind` (158), hardlink `link` (8). |
| `ENOENT` | 2,843 | **Cascade.** Follow-up assertions (`lchown` 569, `unlink` 547, `chmod` 108, `chown` 80, …) against objects the `ENOSYS` calls above never created. |
| *(no errno)* | ~821 | **Cascade.** `stat`/`lstat` value comparisons (mode, uid, nlink) on those same objects, which cannot match because the object does not exist. |
| `ENAMETOOLONG` | 54 | **Real, structural.** Paths near `PATH_MAX` under the mount exceed `PATH_MAX` once re-rooted into the backing store, whose prefix is longer. |
| `EISDIR` | 10 | **Real.** `unlink` on a directory returns `EISDIR` where the test expects `EPERM`. |
| `EEXIST` | 6 | **Cascade.** `O_EXCL` correctly succeeds because the prior `ENOSYS` create left nothing to collide with. |
| `EACCES` | 4 | **Real.** `chmod`/`chown` by a non-owner returns `EACCES` where POSIX says `EPERM` — we rely on the kernel's `default_permissions`, which checks access rights rather than ownership. |
| `EADDRINUSE` | 2 | **Cascade.** Unix-socket tests after a failed `bind`. |

Also present in smaller numbers and genuinely ours:

- **`ENOTDIR` reported as `ENOENT`** (~40 across `mknod`, `link`, `rename`,
  `open`): when a path component exists but is not a directory, `lookup`
  returns `ENOENT` where POSIX requires `ENOTDIR`.

#### Reading the number honestly

**947 direct `ENOSYS` failures come from five operations we do not implement.**
pjdfstest runs most of its matrix across all seven POSIX file types — regular,
directory, fifo, block device, char device, socket, symlink — so five
unimplemented types drag every syscall group down, and each failed setup then
produces several more failures from the assertions that follow it. The ~3,670
`ENOENT` and value-comparison failures are that cascade.

Not implementing symlinks, hardlinks, device nodes and FIFOs in a **file
synchronisation** filesystem is a deliberate position, not an oversight: a
device node has no meaning on the peer that receives it, and a hardlink has no
representation in a model where the unit of replication is a path with a
Merkle tree. Supporting them would mean either replicating something
meaningless or silently degrading them into regular files.

**So: 46.7%, five operations unimplemented by design, three real bugs
(`ENOTDIR` vs `ENOENT`, `EISDIR` vs `EPERM`, `EACCES` vs `EPERM`), and one
structural limit (path length).** That is the honest statement. Chasing 100%
would mean implementing device nodes in a file sync tool, which would be worse
engineering than the current pass rate.

---

## 7. Summary

| Claim | Verdict |
|---|---|
| FUSE reads/writes are slower than native | **Yes** — 1.2×–2.9× write, 1.6×–5.1× read; worst at small sizes |
| Metadata is much slower | **Yes, badly** — 30×–45×; 276 creates/s is the number that governs how it feels |
| The attribute cache earns its keep | **Yes** — 1.63 µs warm vs 436 µs cold, ~267× |
| Merkle sync beats shipping the file | **Yes** — 60× on a 1-byte edit in 100MB |
| Merkle sync is *efficient* | **No** — 1.67MB to move 1 byte; 98.2% of it is the leaf-hash list. §4 names the fix |
| The mount is POSIX-conformant | **Partially** — 46.7%; five operations unimplemented by design, three real errno bugs |

### Reproducing

```bash
# Benchmarks (Linux/WSL2, must be on a real Linux filesystem)
python benchmarks/fuse_benchmark.py --out artifacts/fuse_benchmark.json
python benchmarks/fuse_benchmark.py --quick   # smaller sizes, for iteration

# POSIX conformance (needs root, and pjdfstest built at ~/pjdfstest)
bash scripts/pjdfstest.sh ~/pjdfstest ~/pjdfstest_results.txt
```
