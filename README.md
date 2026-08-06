# Distributed File Synchronization Engine

A production-grade, peer-to-peer distributed file synchronization system built with Python. This system uses **Merkle Trees** for bandwidth-optimized synchronization and **Vector Clocks** for conflict detection and resolution.

## 🎯 Features

- **Merkle Tree-based Synchronization**: Only syncs changed file chunks (4KB blocks), minimizing bandwidth usage
- **Vector Clock Conflict Resolution**: Detects concurrent edits and handles conflicts intelligently
- **Async I/O**: Non-blocking architecture using Python's `asyncio` for high performance
- **Custom Binary Protocol**: Length-prefixed TCP protocol to handle network fragmentation
- **Real-time File Monitoring**: Uses `watchdog` for efficient file system event detection
- **Peer-to-Peer Architecture**: Decentralized synchronization across multiple nodes
- **Mutual Authentication + Encryption**: PSK handshake and AES-256-GCM on every connection
- **Automatic Retries**: Exponential backoff, and reconnection after a dropped link
- **Conflict Siblings**: Concurrent edits are preserved side by side, never silently merged
- **Optional FUSE Mount** (Linux): expose the synced tree as a real filesystem

## 📋 Requirements

- Python 3.11+
- `aiofiles`, `watchdog`, `aiosqlite`, `cryptography` — see `requirements.txt`
- **FUSE mode only, Linux only:** `pyfuse3`, `trio`, and the system packages
  `libfuse3-dev fuse3 pkg-config` — see `requirements-fuse.txt`

## 🚀 Installation

### Using pip

```bash
# Clone the repository
git clone <repository-url>
cd Distributed_File_Sync

# Install dependencies
pip install -r requirements.txt

# Optional: FUSE mode (Linux/WSL2 only; will not install on Windows)
sudo apt install libfuse3-dev fuse3 pkg-config
pip install -r requirements-fuse.txt
```

## 🔐 Security: read this before the usage examples

**A node will not start without either `--psk` or `--allow-insecure`.** This is
deliberate. Before the security work, any process that could reach the port
could push a file to any path a peer would accept, and everything crossed the
wire in cleartext — `docs/SECURITY_AUDIT.md` has the transcripts.

- `--psk <key>` (or `SYNC_PSK`) — all peers in a cluster must share it.
  Enables mutual challenge-response authentication and AES-256-GCM with
  per-connection, per-direction keys.
- `--allow-insecure` — no authentication, no encryption. For a loopback demo
  only. It is a flag rather than a default so that running without protection
  is a decision someone made, not one they inherited.

A mismatched key fails immediately and loudly rather than retrying, because a
key mismatch is a configuration error and backoff would only hide it.

### Using Docker

```bash
# Two-node cluster, the one exercised by tests/test_docker_two_node.py
docker compose up -d --build

# Or a single node by hand
docker build -t file-sync .
docker run -e SYNC_PSK=your-shared-key -v /path/to/sync:/sync \
  file-sync --node-id node1 --dir /sync --port 8000
```

## 📖 Usage

### Command-Line Interface

#### Basic Usage

```bash
# Start a node with command-line arguments
python -m app.main --node-id node1 --dir /path/to/sync --port 8001 \
  --peer localhost:8002 --peer localhost:8003 --psk your-shared-key
```

#### Using Configuration File

```bash
# Create config.json
cat > config.json << EOF
{
  "node_id": "node1",
  "sync_directory": "/path/to/sync",
  "listen_host": "127.0.0.1",
  "listen_port": 8001,
  "peers": [
    {"host": "localhost", "port": 8002},
    {"host": "localhost", "port": 8003}
  ]
}
EOF

# Start node with config
python -m app.main --config config.json
```

#### Using Environment Variables

```bash
export SYNC_NODE_ID=node1
export SYNC_DIRECTORY=/path/to/sync
export SYNC_LISTEN_PORT=8001
export SYNC_PEERS=localhost:8002,localhost:8003
export SYNC_PSK=your-shared-key

python -m app.main
```

### Multi-Node Setup Example

Every node in a cluster must be given the same key.

#### Terminal 1 - Node 1
```bash
python -m app.main --node-id node1 --dir ./sync1 --port 8001 \
  --peer localhost:8002 --psk shared-key
```

#### Terminal 2 - Node 2
```bash
python -m app.main --node-id node2 --dir ./sync2 --port 8002 \
  --peer localhost:8001 --psk shared-key
```

#### Terminal 3 - Node 3
```bash
python -m app.main --node-id node3 --dir ./sync3 --port 8003 \
  --peer localhost:8001 --peer localhost:8002 --psk shared-key
```

Now any file changes in `./sync1`, `./sync2`, or `./sync3` will automatically sync across all nodes!

### FUSE mode (Linux / WSL2 only)

```bash
python -m app.main --node-id node1 --dir ./backing --port 8001 \
  --peer localhost:8002 --psk shared-key --fuse-mount ./mnt
```

`--dir` is the **backing store**; `--fuse-mount` is where the tree appears.
They must be different directories and neither may contain the other — if the
engine's own file I/O went back through the mount it would deadlock against
itself, so the constructor refuses the configuration outright.

In FUSE mode the mount is the source of truth for local changes and the
watchdog observer is not started; every mutation reaches the engine through
the filesystem handlers instead. Omit `--fuse-mount` and you get the original
watchdog daemon, unchanged, on Linux and Windows alike.

## 🧭 Consistency model

Eventually consistent, and specifically **not** last-write-wins.

- Causality is tracked with vector clocks. If one version happened strictly
  after another, the later one wins and the older is discarded.
- If two versions are **concurrent** — neither happened before the other —
  that is a conflict, and no automatic merge is attempted. The peer with the
  lexicographically **lower** node id keeps the path; the other version is
  written beside it as `<path>.conflict-<loser-node>-<counter>`.
- Both peers reach that verdict independently from data they already hold, so
  they agree without an extra round trip. The suffix uses the loser's clock
  counter rather than a timestamp precisely so that both sides compute the
  same name.
- A local write is durable as soon as the filesystem says so. Replication is
  asynchronous and is *not* part of that promise. A write acknowledged locally
  and then propagated is the normal case; a write acknowledged locally while
  the peer is unreachable is also normal, and converges when it returns.
- Deletions leave tombstones so that a peer which was offline during the
  delete cannot resurrect the file when it reconnects. Tombstones are garbage
  collected on a soft/hard TTL.

## 🏗️ Architecture

### Core Components

```
┌─────────────────────────────────────────────┐
│           File Sync Application             │
├─────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────────┐    │
│  │ File Monitor │  │   Sync Engine    │    │
│  │  (Watchdog)  │─▶│  (Orchestrator)  │    │
│  └──────────────┘  └────────┬─────────┘    │
│                              │               │
│  ┌──────────────────────────▼─────────┐    │
│  │      Network Manager               │    │
│  │  (Async TCP Server/Client)         │    │
│  └──────────────────────────┬─────────┘    │
│                              │               │
│  ┌──────────────┬───────────▼──────────┐   │
│  │ Merkle Trees │   Vector Clocks      │   │
│  │ (Bandwidth)  │   (Conflict Detect)  │   │
│  └──────────────┴──────────────────────┘   │
└─────────────────────────────────────────────┘
```

### Algorithms

#### 1. **Merkle Trees** (Bandwidth Optimization)

Files are divided into 4KB chunks. Each chunk is hashed (SHA-256), and hashes are combined in a binary tree structure:

```
                Root Hash
               /          \
          Hash(L)      Hash(R)
         /      \      /      \
    Hash(C1) Hash(C2) Hash(C3) Hash(C4)
       |        |        |        |
    Chunk1   Chunk2   Chunk3   Chunk4
```

When synchronizing:
1. Compare root hashes
2. If different, recursively compare subtrees
3. Only transfer chunks where leaf hashes differ

**Result**: Bandwidth usage proportional to changes, not file size!

#### 2. **Vector Clocks** (Conflict Detection)

Each node maintains a vector of logical timestamps:

```python
Node1: {node1: 5, node2: 3, node3: 2}  # Node1's view
Node2: {node1: 4, node2: 4, node3: 2}  # Node2's view
```

Comparison rules:
- **Before**: All timestamps ≤ other, at least one <
- **After**: All timestamps ≥ other, at least one >
- **Concurrent**: Some >, some < → **CONFLICT!**

### Protocol

Custom binary protocol with length-prefixing:

```
[4 bytes: length][N bytes: JSON message]
```

Message types:
- `HELLO` / `HELLO_ACK`: Handshake
- `SYNC_REQ`: Initiate sync for a file
- `MERKLE_TREE`: Exchange tree structure
- `CHUNK_REQ` / `CHUNK_DATA`: Request/send file chunks
- `VCLOCK_UPDATE`: Vector clock synchronization
- `FILE_DELETE`: Notify file deletion
- `HEARTBEAT`: Keep-alive

## 🧪 Testing

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_vector_clock.py -v

# Run with coverage
pytest tests/ --cov=app --cov-report=html
```

Tests that need something the platform cannot provide **skip loudly** rather
than being quietly dropped, so the count tells you what was actually verified:

| Suite | What it needs | Windows | WSL2 |
|---|---|---|---|
| `test_fuse.py` | Linux + `/dev/fuse` + pyfuse3 | skipped | 41 run, against real mounts |
| `test_docker_two_node.py` | a running Docker daemon | run | run |
| `test_two_node.py` | nothing — two real peers over loopback | run | run |
| everything else | nothing | run | run |

`test_fuse.py` mounts a real filesystem and drives it through the ordinary
`os` module; it does not mock pyfuse3. `test_docker_two_node.py` builds the
image and runs `docker compose`, and creates a genuine bridge-network partition
to make concurrent edits actually concurrent.

Additional harnesses that are not pytest:

```bash
# Definition-of-Done #1: real ls/cat/cp/vim/git/truncate/mv/df against a mount
bash scripts/fuse_smoke.sh

# POSIX conformance (root; needs pjdfstest built)
bash scripts/pjdfstest.sh
```

## 📁 Project Structure

```
Distributed_File_Sync/
├── app/
│   ├── algorithms/
│   │   ├── merkle.py           # Merkle tree, leaf-only wire encoding
│   │   └── vector_clock.py     # Vector clock logic
│   ├── core/
│   │   ├── config.py           # Configuration management
│   │   ├── protocol.py         # Length-prefixed message protocol
│   │   ├── security.py         # PSK auth, AES-GCM, path sanitisation
│   │   ├── catalog.py          # SQLite metadata catalog (aiosqlite, WAL)
│   │   └── logger.py           # Async logging
│   ├── fuse/                   # Linux only; imports are deferred
│   │   ├── operations.py       # pyfuse3 read + write handlers
│   │   ├── mount.py            # trio/asyncio boundary, session thread
│   │   ├── inodes.py           # Inodes stable across remounts
│   │   └── attrcache.py        # Attribute cache
│   ├── services/
│   │   ├── file_monitor.py     # Watchdog -> asyncio bridge
│   │   ├── network.py          # Async TCP, auth, reconnection
│   │   └── sync_engine.py      # Core sync orchestration
│   └── main.py                 # Application entry point
├── benchmarks/fuse_benchmark.py
├── scripts/                    # WSL sync, FUSE smoke test, pjdfstest
├── artifacts/fuse_benchmark.md # Measured results
├── docs/
│   ├── FUSE_DESIGN.md          # Design decisions and deviations
│   └── SECURITY_AUDIT.md       # Pre-hardening findings, with transcripts
├── tests/                      # 269 tests
├── Dockerfile
├── docker-compose.yml          # Two-node cluster used by the integration test
├── requirements.txt
├── requirements-fuse.txt       # Linux-only FUSE extras
└── README.md
```

## ⚙️ Configuration

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `node_id` | string | *required* | Unique identifier for this node |
| `sync_directory` | string | *required* | Directory to synchronize |
| `listen_host` | string | `127.0.0.1` | Host address to bind server |
| `listen_port` | int | `8000` | TCP port for server |
| `peers` | list | `[]` | List of peer addresses (host:port) |
| `chunk_size` | int | `4096` | File chunk size in bytes |
| `max_retries` | int | `5` | Max connection retry attempts |
| `retry_delay` | float | `1.0` | Initial retry delay (seconds) |
| `heartbeat_interval` | float | `30.0` | Heartbeat interval (seconds) |
| `sync_interval` | float | `5.0` | Sync check interval (seconds) |
| `psk` | string | `None` | Pre-shared key. Required unless `allow_insecure` |
| `allow_insecure` | bool | `false` | Run with no auth and no encryption |
| `catalog_path` | string | `<sync_dir>/.dfs/catalog.db` | SQLite metadata catalog |
| `enable_catalog` | bool | `true` | Persist metadata across restarts |
| `fuse_mount` | string | `None` | Mount point. Linux only; enables FUSE mode |
| `fuse_allow_other` | bool | `false` | Pass `allow_other` to libfuse |
| `tombstone_ttl_days` | int | `30` | Soft TTL before a tombstone is collectable |

`max_retries` bounds how many times a node chases a peer that is not
answering. It does **not** bound reconnections over the node's lifetime: once a
connection has been established and later drops, the retry budget starts fresh.

### Example Configuration File

```json
{
  "node_id": "primary-node",
  "sync_directory": "/data/shared",
  "listen_host": "0.0.0.0",
  "listen_port": 8001,
  "peers": [
    {"host": "10.0.1.5", "port": 8001},
    {"host": "10.0.1.6", "port": 8001}
  ],
  "chunk_size": 4096,
  "max_retries": 10,
  "heartbeat_interval": 15.0
}
```

## 🔍 How It Works

### Synchronization Flow

1. **File Change Detection**
   - `watchdog` detects file modification
   - Sync engine computes new Merkle tree
   - Vector clock is incremented

2. **Broadcast Sync Request**
   - Node sends `SYNC_REQ` with root hash and vector clock
   - All connected peers receive the request

3. **Peer Processing**
   - Each peer compares vector clocks:
     - **Remote newer**: Request Merkle tree
     - **Local newer**: Send our Merkle tree
     - **Concurrent**: Detect conflict, backup local version

4. **Merkle Tree Comparison**
   - Trees are compared recursively
   - Only differing chunks are identified
   - `CHUNK_REQ` sent for needed chunks

5. **Chunk Transfer**
   - Peer sends `CHUNK_DATA` messages
   - Receiving node writes chunks to file
   - Merkle tree recomputed and verified

6. **Conflict Resolution**
   - On concurrent edits, local file is backed up as `{file}_conflict_{node_id}_{timestamp}`
   - Remote version is accepted (Last-Write-Wins strategy)

## 🚨 Conflict Handling

When two nodes edit the same file offline and then reconnect:

```
Node1: {n1:5, n2:3} → file.txt (version A)
Node2: {n1:3, n2:5} → file.txt (version B)

Vector clocks are CONCURRENT → Conflict!
```

Resolution strategy:
1. Both peers compare node ids. The **lower** id keeps the path — here `node1`.
2. `node2`'s version is written beside it as
   `file.txt.conflict-node2-5`, where `5` is `node2`'s clock counter.
3. Both files are visible to `ls`. Nothing is merged and nothing is discarded.
4. The user resolves it by deleting or renaming whichever they want.

Both peers reach step 1 independently, from data they already have, so they
agree without an extra round trip. The suffix uses the loser's **clock
counter** rather than a timestamp on purpose: a timestamp would depend on
whichever machine generated the name, so the two peers could name the same
conflict differently and each end up with a sibling the other lacks.

## 🐛 Troubleshooting

### Connection Issues

```bash
# Check if port is available
netstat -an | grep 8001

# Test connectivity
telnet localhost 8001
```

### Synchronization Not Working

1. Check logs: `logs/file_sync.log`
2. Verify peers are connected: Look for "Connected to peer" messages
3. Ensure firewall allows TCP connections
4. Verify sync directories have proper permissions

### Performance Tuning

- **Large files**: Increase `chunk_size` (e.g., 1MB for videos)
- **High latency**: Increase `heartbeat_interval`
- **Frequent changes**: Decrease `sync_interval`

## 📊 Performance

Measured, not estimated. Full methodology, ranges and unflattering results in
[`artifacts/fuse_benchmark.md`](artifacts/fuse_benchmark.md).

### Merkle delta — 1-byte edit in a 100MB file

| | |
|---|---|
| Bytes changed | 1 |
| **Bytes on the wire** | **1,747,377 (1.67 MB)** |
| vs shipping the whole file | 60× less |
| vs the size of the edit | 1,747,377× more |

Both rows are true and the second one matters. 60× beats naive replication, but
**98.2% of that 1.67MB is the leaf-hash list, not data** — the chunking moved
exactly one 4KB chunk, and the tree *exchange* is what costs. The fix (descend
the tree instead of shipping its leaves, ~2 KB instead of 1.67 MB) is described
in `artifacts/fuse_benchmark.md` §4 and is not yet implemented.

### FUSE overhead vs the native filesystem

| | 4 KB | 1 MB | 16 MB | 128 MB |
|---|---|---|---|---|
| write | 2.87× slower | 1.18× | 1.75× | 1.86× |
| read | 5.11× slower | 1.65× | 1.76× | 2.49× |

Metadata is far worse: **create 45× slower (276 ops/s), stat 30×, unlink 30×.**
`getattr` is 1.63 µs on a kernel cache hit and 436 µs cold — a 267× difference,
which is why the two are reported separately.

The mount is well suited to editing documents and source trees, and poorly
suited to build directories full of small files or to database files.

### POSIX conformance

**pjdfstest: 4,111 / 8,798 = 46.7%.** `symlink`, `link`, `mknod`, `mkfifo` and
unix-socket `bind` are unimplemented by design — a device node has no meaning
on the peer that receives it — and account for 947 direct failures plus most of
the ~3,670 cascading ones. Three real errno bugs and one structural path-length
limit are enumerated in `artifacts/fuse_benchmark.md` §6.

## 🔒 Security

Every connection is mutually authenticated and encrypted unless the operator
explicitly opts out with `--allow-insecure`. `docs/SECURITY_AUDIT.md` records
what the system looked like before this work, with the probe transcripts.

**Implemented:**

- **Mutual authentication** — HMAC challenge/response over a pre-shared key,
  stretched with PBKDF2-HMAC-SHA256 (200k iterations). The dialling side
  verifies the *server's* proof first, so a node never hands its identity and
  file list to a host that cannot prove cluster membership. Proofs are compared
  with `hmac.compare_digest`.
- **Encryption in transit** — AES-256-GCM with per-connection, per-direction
  keys. The IV is a monotonic counter that is never transmitted, so a replayed
  frame fails authentication rather than being accepted.
- **Path sanitisation on every received name** — deny-by-default. Traversal,
  absolute paths, drive letters, NT device names, NUL bytes and symlink escapes
  are all rejected, and containment is re-checked after `resolve()` so a
  symlink with no `..` in it cannot escape either. A peer cannot write outside
  the sync root even *with* the correct key; both properties are asserted by
  test in `tests/test_two_node.py`.
- **Bounded input** — length-prefixed frames capped at 16MB, inbound
  connections capped at 64, handshake frames capped at 4KB and timed out at
  10s.
- **Identity binding** — the authenticated identity is authoritative. A peer
  that authenticates as A cannot then claim to be B in `HELLO` and inherit B's
  causal history.

**Not implemented — know these before deploying:**

- No PKI. A single shared key per cluster means any authenticated peer is
  fully trusted; there is no per-peer revocation short of rotating the key
  everywhere.
- No authorisation. Every authenticated peer may read and write every synced
  path within the root.
- No encryption at rest. Files sit in plaintext in the backing directory.
- The PSK is passed on the command line by default, which puts it in the
  process table. Prefer `SYNC_PSK` in the environment or a secrets mount.

## 🤝 Contributing

Contributions welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch
3. Write tests for new features
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details

## 👥 Authors

Distributed Systems Team

## 🙏 Acknowledgments

- Inspired by rsync and Dropbox's sync protocol
- Built on lessons from distributed systems research
- Uses battle-tested algorithms (Merkle Trees, Vector Clocks)

## 📚 Further Reading

- [Merkle Trees Explained](https://en.wikipedia.org/wiki/Merkle_tree)
- [Vector Clocks for Distributed Systems](https://en.wikipedia.org/wiki/Vector_clock)
- [AsyncIO Best Practices](https://docs.python.org/3/library/asyncio.html)

---

**Happy Syncing! 🚀**
