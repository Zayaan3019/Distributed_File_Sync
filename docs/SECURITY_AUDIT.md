# Phase 0 — Security audit

**Date:** 2026-08-01
**Scope:** the network-facing surface of the sync daemon, plus everything that
breaks on restart.
**Method:** every finding below was reproduced by running the code, not by
reading it. The probe drives a real `SyncEngine` + `NetworkManager` on a
loopback port and connects a raw attacker socket that speaks the wire protocol.
Nothing is mocked. Probe output is quoted verbatim.

**Baseline at audit time:** 115 tests, all passing on Windows.
(The brief said 21; that number was already stale — `docs/FUSE_DESIGN.md` §0
recorded 35, and the catalog work took it to 115.)

---

## Summary

| # | Severity | Finding | Label | Status |
|---|---|---|---|---|
| 1 | **CRITICAL** | No authentication — any peer that connects can write files | [FIX] | Fixed |
| 2 | **CRITICAL** | Path traversal — arbitrary file write outside the sync root | [FIX] | Fixed |
| 3 | **HIGH** | No encryption — file contents and paths in cleartext | [FIX] | Fixed |
| 4 | **HIGH** | Apply is not atomic — a crash mid-apply leaves a torn file | [FIX] | Fixed |
| 5 | **HIGH** | Zero persistence — causal history destroyed on every restart | [FIX] | Fixed |
| 6 | **HIGH** | `Catalog` had zero callers — dead code | [FIX] | Fixed |
| 7 | **MEDIUM** | Unsolicited chunks accepted without a clock comparison | [FIX] | Fixed |
| 8 | **MEDIUM** | No inbound connection limit | [FIX] | Fixed |
| 9 | **MEDIUM** | Exception text echoed to remote peers | [FIX] | Fixed |
| 10 | **MEDIUM** | `add_signal_handler` crashes the daemon on Windows | [FIX] | Fixed |
| 11 | **LOW** | Unknown message type kills the connection | [FIX] | Fixed |
| 12 | **LOW** | Length prefix **is** bounded — brief's suspicion not confirmed | [DEFEND] | No change |

Two further defects were found *during* the fix work rather than in the audit,
both by running real software rather than tests. They are recorded in
[§13](#13-defects-found-during-implementation) because they were reachable in
production, not just in tests.

---

## 1. CRITICAL — No authentication

`app/services/network.py:145` (`_handle_client`, pre-fix) accepted any TCP
connection, read one `HELLO`, and registered the sender as a trusted peer on
the strength of a self-declared `node_id`. There was no credential of any kind.

**Probe:**

```
PROBE 1 — Authentication: can an unknown peer connect and be trusted?
  HELLO with no credential whatsoever -> server replied:
    {'type': 'HELLO_ACK', 'payload': {'version': '1.0'}, 'node_id': 'victim_node'}
  RESULT: ACCEPTED (no auth)
```

**Failure scenario.** Any host that can reach the port becomes a full peer. It
can enumerate the tree via `SYNC_REQ`, pull any file's contents via
`CHUNK_REQ`, and overwrite any file via `CHUNK_DATA`. Combined with finding #2
this is unauthenticated remote arbitrary file write.

**Fix.** `app/core/security.py` — `Authenticator`, a mutual challenge-response
over a pre-shared key. Both sides prove knowledge of the key via HMAC-SHA256
over both freshly exchanged nonces plus the prover's node id, under distinct
domain-separation tags. The responder proves first, so a client talking to an
impostor aborts before sending anything of its own. Wired in at
`app/services/network.py:_handle_client` and `_connect_to_peer`; an
unauthenticated socket never reaches a message handler.

The key is never transmitted, so a passive observer cannot replay and an active
one cannot mount an offline dictionary attack without also breaking
HMAC-SHA256. Proof verification uses `hmac.compare_digest`, not `==`, so it is
not a timing oracle.

Security is now **on by default**: `NetworkManager.__init__` raises unless a
PSK is configured or `allow_insecure` is set explicitly.

---

## 2. CRITICAL — Path traversal, exploited

Received filenames were used verbatim. `SyncEngine._write_chunks` (pre-fix)
did `Path(self.config.sync_directory) / relative_path` with no validation.

**Probe — this is a working exploit, not a theoretical one:**

```
PROBE 3 — Path traversal: literally send ../../../etc/passwd
  sent file_path='../VICTIM.txt'
    -> resolves to ...\dfs_probe_lagdhcd5\VICTIM.txt
    -> escaped sync root? True
    -> file written? True  content=b'PWNED-BY-ATTACKER'
  sent file_path='/tmp/dfs_abs_escape.txt'
    -> escaped sync root? True
    -> file written? True  content=b'PWNED-BY-ATTACKER'

  VICTIM.txt (outside sync root) now reads: b'PWNED-BY-ATTACKER'
  RESULT: *** COMPROMISED — arbitrary file write outside sync root ***
```

A file outside the sync root was overwritten with attacker-controlled content,
and an absolute path escaped as well. On a node running as a service account
this is remote code execution by any of the usual routes
(`~/.ssh/authorized_keys`, `~/.bashrc`, a cron file, a systemd unit).

`../../../../../../etc/passwd` failed only because the test host was Windows
and `C:\Users\etc\` does not exist — the traversal itself succeeded.

**Fix.** `app/core/security.py::sanitize_relative_path` + `safe_join`, applied
at every point where a peer-supplied path enters the system
(`SyncEngine._received_path`, `Protocol.decode_chunk_data`,
`FileSyncFs._child_path`). Deny-by-default: absolute paths, drive-qualified
paths, UNC paths, any `..` component, control characters, Windows reserved
device names and trailing dots/spaces are all rejected rather than normalised.

`safe_join` additionally **resolves** the result and re-checks containment.
Textual validation alone is insufficient: a symlink already inside the tree
pointing outside it gives an escape with no `..` anywhere in the path.

**Verified after fix:**

```
ATTACK 3 — authenticate correctly, THEN attempt path traversal
  SECURITY: rejected CHUNK_DATA: parent traversal rejected: '../VICTIM.txt'
  SECURITY: rejected CHUNK_DATA: parent traversal rejected: '../../../../../../etc/passwd'
  SECURITY: rejected CHUNK_DATA: absolute path rejected: '/tmp/dfs_abs_escape.txt'
  SECURITY: rejected CHUNK_DATA: drive-qualified path rejected: 'C:\Windows\evil.txt'
  SECURITY: rejected CHUNK_DATA: parent traversal rejected: 'ok/../../escape.txt'
  SECURITY: rejected CHUNK_DATA: reserved device name: 'COM1'
  SECURITY: rejected CHUNK_DATA: parent traversal rejected: 'sub/../../../out.txt'

  VICTIM.txt still reads: b'original-content'
  RESULT: CONTAINED — no escape
```

Regression tests: `tests/test_security.py::TestPathSanitisation`,
`tests/test_transfer_integrity.py::TestPathContainmentInEngine`, and
`tests/test_two_node.py::...::test_authenticated_peer_still_cannot_escape_the_root`
— the last of which proves that authentication is not authorisation to write
anywhere.

---

## 3. HIGH — No encryption in transit

Every frame was plain JSON over plain TCP. File contents crossed the wire
base64-encoded but not encrypted, as did every path name.

**Probe:**

```
PROBE 2 — Encryption: is the wire cleartext?
  The HELLO_ACK above was read as plain JSON off the socket:
    {"type": "HELLO_ACK", "payload": {"version": "1.0"}, "node_id": "victim_node"}
  RESULT: CLEARTEXT — no TLS, no handshake negotiation, no MAC.
```

**Fix.** `app/core/security.py::SecureChannel` — AES-256-GCM per frame, with
per-connection **and per-direction** keys derived from the handshake nonces.
Reusing one key in both directions with a counter nonce would repeat a
(key, IV) pair, which is a total break of GCM.

The IV is **not transmitted**; it is a monotonic per-direction counter both
sides track independently. That saves 12 bytes a frame but matters far more
for integrity: a replayed frame decrypts under the wrong counter and fails
authentication, whereas with a transmitted IV it would be silently accepted.
The frame length is bound in as associated data, so frames cannot be truncated
or spliced.

Tests: `tests/test_security.py::TestSecureChannel`, including
`test_ciphertext_does_not_contain_plaintext`, `test_tampered_frame_fails_authentication`
and `test_replayed_frame_fails`.

---

## 4. HIGH — File application is not atomic

**Probe:**

```
PROBE 5 — atomicity of apply, by reading the code
    'tempfile'     present in _write_chunks source? False
    'os.replace'   present in _write_chunks source? False
    'os.rename'    present in _write_chunks source? False
    'fsync'        present in _write_chunks source? False
    "'r+b'"        present in _write_chunks source? True
```

Chunks were written straight into the destination inode. A crash between the
first and last chunk leaves a file that is neither the old version nor the new
one — and because there was no persistence (finding #5), nothing on restart
knew the file was mid-update.

**Fix.** `SyncEngine._apply_transfer` builds the result in a temp file under
`.dfs/tmp/`, `fsync`s it, verifies the assembled content against the peer's
advertised Merkle leaves, and only then publishes with `os.replace`. A crash at
any point leaves either the old file or the new one.

**Cost, stated plainly:** seeding the temp file copies the unchanged portion,
so applying a 1-byte delta to a 100MB file does 100MB of *local* I/O. That is a
deliberate trade — the scarce resource is network bandwidth, not local disk —
and both numbers are reported in `artifacts/fuse_benchmark.md`.

The verification step is new and independently valuable: content that does not
hash to what the peer advertised is discarded rather than published, so a bug
or a tampered frame cannot corrupt the tree and then replicate the corruption
onward. Test: `test_transfer_integrity.py::test_mismatched_content_is_not_published`.

---

## 5. HIGH — Everything breaks on restart

**Probe:**

```
PROBE 6 — what a restart destroys
    after 4 local edits : clock={'N1': 4}
    after restart       : clock={'N1': 1}
    -> counter went 4 -> 1: causal history destroyed.
```

Enumerated, as the brief asks:

1. **Vector clocks reset.** A peer holding `{'N1': 4}` now compares `AFTER` the
   restarted node, so the node's *newest* content is silently overwritten by
   the peer's *older* content. This is silent data loss, not a delay.
2. **Every file re-hashed at startup.** Measured at 8.48s for a single 100MB
   file (`FUSE_DESIGN.md` appendix). Startup cost is linear in total bytes.
3. **In-flight transfers lost.** `pending_chunks` / `pending_expected` were
   in-memory, so an interrupted transfer restarted from zero — and left the
   torn file from finding #4 torn.
4. **Deletions resurrect.** With no tombstone there is nothing to out-vote a
   peer that missed the delete and re-advertises the file.
5. **Conflict history lost.** A resolved conflict could re-fire after a
   restart, because the merged clock that settled it was gone.
6. **Inode numbers unstable** — fatal for FUSE, which is why the catalog is a
   prerequisite for the mount rather than an optimisation.

**Fix.** The catalog is now wired in (finding #6). Startup calls
`Catalog.reconcile`, which stats every file and re-hashes **only** those whose
`(size, mtime)` disagree. Tests in `tests/test_persistence.py` assert both the
clock survival and that unchanged files are not re-hashed.

---

## 6. HIGH — `Catalog` was dead code

The shared preamble is explicit that zero callers is a defect regardless of
code quality. `app/core/catalog.py` — 1007 lines, 53 passing tests — had **no
callers anywhere in `app/`**:

```
$ grep -rl "Catalog\|catalog" --include=*.py .
app/core/catalog.py
tests/test_catalog.py
```

So the persistence work described as "complete" in `docs/FUSE_DESIGN.md` §0 was
complete as a *module* and absent as a *behaviour*. Every consequence in
finding #5 was still live. This is the clearest case in the repo of docs and
code disagreeing, and per the preamble both are reported: the design doc's
status table has been corrected.

**Fix.** `SyncEngine` now owns a `Catalog`, opens it in `initialize()`, and
records every mutation. `docs/FUSE_DESIGN.md` §0 updated.

---

## 7. MEDIUM — Unsolicited chunks were accepted

`_handle_chunk_data` (pre-fix) wrote any `CHUNK_DATA` it received into
`pending_chunks` regardless of whether that transfer had been requested. A peer
could therefore push content into a path without any clock comparison ever
happening — bypassing conflict detection entirely.

**Fix.** A chunk is dropped unless a matching `PendingTransfer` exists.
Test: `test_transfer_integrity.py::test_unsolicited_chunk_is_dropped`.

---

## 8. MEDIUM — No inbound connection limit

Bounding a single message to 10MB accomplishes little if an attacker can open
ten thousand sockets and hold 10MB in each.

**Fix.** `NetworkManager.MAX_INBOUND_CONNECTIONS = 64`, enforced **before**
authentication, plus a 10s handshake timeout so a half-open handshake cannot
pin a slot.

---

## 9. MEDIUM — Exception text echoed to peers

`_message_loop` sent `str(e)` back to the remote peer on any handler error.
Exception text routinely carries absolute local paths and internal state.

**Fix.** The reply now names only the message type that failed.

---

## 10. MEDIUM — Daemon crashed at startup on Windows

`app/main.py` called `loop.add_signal_handler(...)`, which raises
`NotImplementedError` on the Windows `ProactorEventLoop`. Definition-of-Done #5
requires the daemon to run on Windows; as written, `python -m app.main` could
not start there.

**Fix.** Fall back to `signal.signal()` with `call_soon_threadsafe`, which
works on both platforms.

---

## 11. LOW — Unknown message types killed the connection

`Message.from_dict` called `MessageType(data['type'])`, which raises
`ValueError` on an unrecognised string; `receive_message` re-raised and the
connection died. A newer peer using a message type we do not know could
therefore disconnect an older one, making any protocol addition a flag-day
upgrade.

**Fix.** `UnknownMessageType` is raised, caught in `receive_message`, logged,
and the frame skipped. This is what makes the `FILE_RENAME` addition
(protocol 1.1) safe to deploy incrementally.

---

## 12. LOW — The length prefix was already bounded [DEFEND]

The brief suspected unbounded allocation from an attacker-controlled prefix.
**That suspicion is not confirmed.** `app/core/protocol.py` already checked
`message_length > MAX_MESSAGE_SIZE` *before* reading the body:

```
PROBE 4 — Length prefix: unbounded allocation?
  Declared length 0xFFFFFFFF (4 GiB) after a valid HELLO.
  Server MAX_MESSAGE_SIZE = 10485760 bytes
  RESULT: BOUNDED — allocation refused.
```

Reported as a non-finding rather than quietly counted as a fix. The check has
been *restated* at the new transport layer (`SecureChannel.receive`, and a
tighter 4096-byte limit on handshake frames) so the bound holds regardless of
what runs above it, but the original code was correct here.

---

## 13. Defects found during implementation

Both were reachable in production and neither was caught by any unit test —
recording them because how they were found is the point.

### 13.1 Every dotfile was rejected

`sanitize_relative_path` used `part.strip(" .")` to catch Windows' trailing-dot
stripping. `strip()` removes from **both** ends, so `.git`, `.gitignore` and
`.bashrc` were all rejected as invalid names. Inside the mount, `git init`
failed and Definition-of-Done #1 was unreachable.

Found by running real `git` against a real mount in `scripts/fuse_smoke.sh`. A
Python-only test would never have created a `.git` directory. Fixed to
`rstrip`; regression test `test_dotfiles_are_allowed`.

### 13.2 Self-deadlock on the sync lock

`_handle_sync_request` takes `SyncEngine._sync_lock`, and on a concurrent-edit
verdict called `_handle_conflict` → `_process_local_file`, which takes the same
lock. `asyncio.Lock` is not reentrant, so the task parked forever with no
error, no traceback and no timeout — the two-node suite simply hung.

Fixed by splitting `_process_local_file_locked` out of `_process_local_file`.
Covered by `tests/test_two_node.py::TestConflictPresentation`, which hung
before the fix and passes in ~1s after it.

### 13.3 Files above ~1MB silently never arrived

Found by running the Phase 6 benchmark, not by a test. An 8MB file never
reached the peer while 4KB and 1MB always did.

Three separate defects, each masked by the one in front of it:

1. **`_register` resolved duplicate connections by keeping the newest.** Peers
   dial each other symmetrically, so a pair always briefly holds two TCP
   connections. Node A was 1024 chunks into streaming a transfer when its own
   retry loop finally reached B, and B closed the in-flight connection in
   favour of the brand-new idle one. Under ~1MB the transfer finished before
   the redial landed, which is exactly why every existing test passed.
   *Fixed:* keep the connection whose dialler has the lexicographically lower
   node id. Both sides compute the same answer, so the survivor is chosen by
   identity rather than by timing.

2. **`_message_loop` deregistered by peer id without checking identity.** With
   the tie-break in place, the *losing* connection's teardown then removed the
   *winner's* entry, leaving both nodes believing they had no peer while a
   perfectly good socket sat open between them. `_handle_client` already had
   this guard; it was useless because `_message_loop` runs first and had none.

3. **A transfer stranded by a dead connection was never retried.**
   `PendingTransfer.started_at` was documented "for stall detection" and had
   zero readers — the detection was never implemented. A stream that stopped
   left the file missing with no error, no retry and no symptom other than an
   absence. *Fixed:* `SyncEngine.on_connection_lost` restarts affected
   transfers the instant their socket dies, and `reap_stalled_transfers` (run
   every 15s by the daemon) catches stalls that end no connection.

Regression tests: `test_multi_megabyte_file_propagates`,
`test_peers_converge_on_exactly_one_connection`,
`test_transfer_recovers_immediately_when_its_connection_dies`.

### 13.4 Peers never reconnected after a dropped connection

`_connect_to_peer` was called once per peer at startup and returned for good
once its message loop ended. The first transient fault therefore partitioned
the cluster **permanently** — and since both peers share the implementation,
neither would ever dial again. Every existing test used a connection that was
never interrupted.

*Fixed:* `_supervise_peer` wraps the dial in a supervision loop. A key mismatch
still fails once and stops, because that is a configuration error rather than a
transient fault. Regression test:
`test_peers_reconnect_after_the_connection_drops`.

### 13.5 The watchdog bridge silently dropped writes

`AsyncFileMonitor._schedule_callback` built the coroutine, then discarded it if
the event fell inside the 0.5s debounce window. Two consequences: every
debounced event leaked an un-awaited coroutine, and — the real damage — a file
modified twice inside the window was hashed once, at its *first* content. The
second edit never propagated at all.

Found as a `RuntimeWarning: coroutine 'SyncEngine.on_file_changed' was never
awaited` buried in Docker container logs. The module had **no tests at all**.

*Fixed:* the window is now trailing-edge — a duplicate sets a dirty flag and one
more run is scheduled when the window closes, so the file's final state is
always processed. `tests/test_file_monitor.py` (9 tests) now covers it.

### 13.6 Authentication failure gave actively wrong advice

A handshake that failed for any reason advised "Check that both peers share the
same --psk". When another service on the machine held the port, the first four
bytes of its HTTP response decoded as a 1,213,486,160-byte frame length — and
the operator was sent looking at their key instead of at the port. Found by
running the QUICKSTART commands verbatim.

*Fixed:* a printable-ASCII frame header now reports that the peer is not
speaking this protocol, and quotes the bytes.

---

## What is still not guaranteed

Stated so the fixes are not read as broader than they are.

- **The PSK is symmetric and cluster-wide.** Any authenticated peer is a fully
  trusted peer: it can read and write any file in the synced tree. There is no
  per-peer authorisation, no read-only role, and no revocation short of
  rotating the key on every node. mTLS with per-node certificates would fix
  this and is not implemented.
- **No forward secrecy.** Session keys derive from the long-term PSK, so an
  attacker who later learns the PSK and has recorded traffic can decrypt it. A
  Diffie-Hellman exchange would fix this; it is not implemented.
- **No protection against a malicious authenticated peer** beyond path
  containment. Such a peer can still delete files, manufacture conflicts, or
  advertise bogus clocks.
- **The catalog is not encrypted at rest.**
- **`--allow-insecure` still exists.** It is off by default and logs loudly,
  but it is a real footgun and is documented as one.
