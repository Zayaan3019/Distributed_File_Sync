# Directory Structure

```
Distributed_File_Sync/
│
├── app/                                 # Main application package
│   ├── __init__.py                     # Package initialization
│   ├── main.py                         # Entry point (380 lines)
│   │
│   ├── algorithms/                     # Core algorithms
│   │   ├── __init__.py                # Algorithm exports
│   │   ├── vector_clock.py            # Vector Clock implementation (250 lines)
│   │   └── merkle.py                  # Merkle Tree implementation (450 lines)
│   │
│   ├── core/                           # Core infrastructure
│   │   ├── __init__.py                # Core exports
│   │   ├── config.py                  # Configuration management
│   │   ├── protocol.py                # Length-prefixed message protocol
│   │   ├── security.py                # PSK auth, AES-GCM, path sanitisation
│   │   ├── catalog.py                 # SQLite catalog (aiosqlite, WAL)
│   │   └── logger.py                  # Async logging
│   │
│   ├── fuse/                           # FUSE mode -- Linux only
│   │   ├── __init__.py                # Defers pyfuse3 import so this
│   │   │                              #   package imports on Windows
│   │   ├── operations.py              # pyfuse3 read + write handlers
│   │   ├── mount.py                   # trio/asyncio boundary, session thread
│   │   ├── inodes.py                  # Inodes stable across remounts
│   │   └── attrcache.py               # Attribute cache
│   │
│   └── services/                       # Service layer
│       ├── __init__.py                # Service exports
│       ├── network.py                 # TCP, auth, reconnect supervision
│       ├── file_monitor.py            # Watchdog -> asyncio bridge
│       └── sync_engine.py             # Sync orchestration
│
├── benchmarks/
│   └── fuse_benchmark.py               # Phase 6 measurement harness
│
├── scripts/
│   ├── wsl_sync.sh                     # Copy checkout to ext4 for FUSE work
│   ├── fuse_smoke.sh                   # DoD #1: real ls/cat/cp/vim/git/df
│   └── pjdfstest.sh                    # POSIX conformance run
│
├── artifacts/
│   └── fuse_benchmark.md               # Measured results
│
├── docs/
│   ├── FUSE_DESIGN.md                  # Design decisions and deviations
│   └── SECURITY_AUDIT.md               # Pre-hardening findings + transcripts
│
├── tests/                              # Test suite -- 297 tests, 14 files
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_security.py               # 70 -- auth, crypto, path traversal
│   ├── test_catalog.py                # 53 -- catalog, WAL, crash recovery
│   ├── test_fuse.py                   # 41 -- against REAL mounts (Linux)
│   ├── test_two_node.py               # 23 -- two peers over real sockets
│   ├── test_vector_clock.py           # 19
│   ├── test_merkle_diff.py            # 15
│   ├── test_tombstones.py             # 13
│   ├── test_merkle_tree.py            # 12
│   ├── test_transfer_integrity.py     # 10 -- atomic apply, partial transfers
│   ├── test_file_monitor.py           #  9 -- debounce coalescing
│   ├── test_persistence.py            #  9
│   ├── test_sync_engine.py            #  8
│   ├── test_protocol.py               #  8
│   └── test_docker_two_node.py        #  7 -- real containers, real partition
│
├── logs/                               # Log directory (generated at runtime)
│   └── file_sync.log                  # Application logs
│
├── README.md                           # Main documentation
├── QUICKSTART.md                       # Quick start guide
├── ARCHITECTURE.md                     # Technical deep dive
├── PROJECT_SUMMARY.md                  # Implementation summary (pre-FUSE)
├── VERIFICATION.md                     # Corrected test inventory
│
├── examples.py                         # Usage examples
│
├── requirements.txt                    # Python dependencies
├── requirements-fuse.txt               # FUSE extras -- Linux only
├── pyproject.toml                      # Pytest configuration
├── .gitignore                          # Git ignore rules
│
├── Dockerfile                          # Container definition
└── docker-compose.yml                  # Two-node cluster for the
                                        #   integration test

```

Docker node data lives in **named volumes**, not `./data/`. The catalog is
SQLite in WAL mode, whose shared-memory locking a bind mount from a Windows
host does not reliably provide.

## Breakdown by Directory

#### `app/algorithms/` (2 files)
- Vector Clock: distributed causality tracking
- Merkle Tree: bandwidth-optimised sync, leaf-only wire encoding

#### `app/core/` (5 files)
- Config: configuration management
- Protocol: length-prefixed message protocol, version negotiation
- Security: PSK authentication, AES-256-GCM, path sanitisation
- Catalog: SQLite metadata store (aiosqlite, WAL)
- Logger: structured logging

#### `app/fuse/` (4 files, Linux only)
- Operations: pyfuse3 read and write handlers
- Mount: trio/asyncio boundary and the FUSE session thread
- Inodes: inode allocation stable across remounts
- AttrCache: attribute caching

#### `app/services/` (3 files)
- Network: async TCP, authentication, reconnection supervision
- File Monitor: watchdog → asyncio bridge with coalescing debounce
- Sync Engine: core synchronisation logic

#### `app/main.py`
- Application bootstrapping, CLI parsing, component lifecycle
- Background loops: tombstone GC, stalled-transfer reaper

#### `tests/` (14 files, 297 tests)
- Real mounts, real sockets, real containers — see `VERIFICATION.md`

### Documentation Files
- **README.md**: 600+ lines
- **QUICKSTART.md**: 300+ lines
- **ARCHITECTURE.md**: 700+ lines
- **PROJECT_SUMMARY.md**: 400+ lines
- **Total Documentation**: 2,000+ lines

### Configuration Files
- **Dockerfile**: Multi-stage build
- **docker-compose.yml**: 3-node cluster
- **requirements.txt**: Dependencies
- **pyproject.toml**: Test configuration
- **.gitignore**: Exclusion rules

## Key Features by File

### Core Algorithms

**vector_clock.py**
- ✅ VectorClock class
- ✅ ClockRelation enum (BEFORE, AFTER, CONCURRENT, EQUAL)
- ✅ increment(), merge(), compare()
- ✅ Serialization (to_dict, from_dict, to_json, from_json)
- ✅ Full type hints and docstrings

**merkle.py**
- ✅ MerkleTree class
- ✅ MerkleNode dataclass
- ✅ TreeDiff dataclass
- ✅ Async file chunking (4KB blocks)
- ✅ SHA-256 hashing
- ✅ Recursive diff algorithm
- ✅ Chunk extraction
- ✅ Serialization

### Core Infrastructure

**config.py**
- ✅ SyncConfig dataclass
- ✅ PeerConfig dataclass
- ✅ Multiple loading methods (file, env, CLI)
- ✅ Validation and defaults
- ✅ Peer management

**protocol.py**
- ✅ MessageType enum (10+ types)
- ✅ Message dataclass
- ✅ Protocol class
- ✅ Length-prefixed framing
- ✅ send_message(), receive_message()
- ✅ Helper methods for common messages

**logger.py**
- ✅ ColoredFormatter for console
- ✅ RotatingFileHandler
- ✅ setup_logger(), get_logger()
- ✅ Async-safe logging

### Services

**network.py**
- ✅ NetworkManager class
- ✅ PeerConnection dataclass
- ✅ Async TCP server/client
- ✅ Connection pooling
- ✅ Exponential backoff
- ✅ Heartbeat mechanism
- ✅ broadcast(), send_to_peer()

**file_monitor.py**
- ✅ AsyncFileMonitor class
- ✅ FileMonitorManager class
- ✅ Watchdog integration
- ✅ Event debouncing
- ✅ Async callback bridge
- ✅ Smart filtering

**sync_engine.py**
- ✅ SyncEngine class
- ✅ FileMetadata dataclass
- ✅ Merkle tree computation
- ✅ Vector clock management
- ✅ Conflict detection
- ✅ Conflict resolution
- ✅ Chunk transfer
- ✅ Message handling

### Application

**main.py**
- ✅ FileSyncApp class
- ✅ Argument parsing
- ✅ Configuration loading
- ✅ Component initialization
- ✅ Signal handling
- ✅ Graceful shutdown

## Dependencies

### Runtime Dependencies
```
aiofiles==23.2.1          # Async file I/O
watchdog==3.0.0           # File system monitoring
typing-extensions==4.8.0  # Type hints
```

### Development Dependencies
```
pytest==7.4.3             # Testing framework
pytest-asyncio==0.21.1    # Async test support
pytest-cov==4.1.0         # Coverage reporting
```

### Built-in Dependencies (No Install Required)
- asyncio: Async I/O
- hashlib: SHA-256 hashing
- struct: Binary packing
- json: Serialization
- logging: Logging
- pathlib: Path handling
- dataclasses: Data structures
- enum: Enumerations
- signal: Signal handling

## Docker Files

**Dockerfile**
- Multi-stage build
- Python 3.11 slim base
- Non-root user (syncuser)
- Health check
- Volume mount (/sync)
- Expose port 8000

**docker-compose.yml**
- 3 nodes (node1, node2, node3)
- Isolated network (sync_network)
- Volume mounts for data and logs
- Port mapping (8001, 8002, 8003)
- Auto-restart policy

## Total Project Size

- **Source Code**: 3,500+ lines
- **Tests**: 600+ lines
- **Documentation**: 2,000+ lines
- **Configuration**: 200+ lines
- **Total**: ~6,300 lines

---

**This structure provides a complete, production-ready distributed file synchronization system.**
