# Architecture Deep Dive

This document provides an in-depth technical analysis of the Distributed File Sync Engine's architecture, algorithms, and design decisions.

## Table of Contents

1. [System Overview](#system-overview)
2. [Core Algorithms](#core-algorithms)
3. [Network Protocol](#network-protocol)
4. [Concurrency Model](#concurrency-model)
5. [Conflict Resolution](#conflict-resolution)
6. [Performance Characteristics](#performance-characteristics)
7. [Design Decisions](#design-decisions)

## System Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     File Sync Application                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                   Application Layer                      │  │
│  │                      (main.py)                           │  │
│  └─────────────────────┬────────────────────────────────────┘  │
│                        │                                        │
│  ┌─────────────────────▼────────────────────────────────────┐  │
│  │              Service Layer                               │  │
│  │  ┌──────────────┐  ┌─────────────┐  ┌───────────────┐  │  │
│  │  │File Monitor  │  │Sync Engine  │  │Network Manager│  │  │
│  │  │(watchdog)    │─▶│(orchestrator)│◀─│(TCP async)   │  │  │
│  │  └──────────────┘  └─────────────┘  └───────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                        │
│  ┌─────────────────────▼────────────────────────────────────┐  │
│  │              Algorithm Layer                             │  │
│  │  ┌──────────────────┐        ┌──────────────────┐       │  │
│  │  │  Merkle Trees    │        │  Vector Clocks   │       │  │
│  │  │  (bandwidth opt) │        │  (causality)     │       │  │
│  │  └──────────────────┘        └──────────────────┘       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                        │
│  ┌─────────────────────▼────────────────────────────────────┐  │
│  │              Core Layer                                  │  │
│  │  ┌──────────────┐  ┌─────────────┐  ┌───────────────┐  │  │
│  │  │Config        │  │Protocol     │  │Logger         │  │  │
│  │  │Management    │  │(binary msg) │  │(async)        │  │  │
│  │  └──────────────┘  └─────────────┘  └───────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

#### 1. **File Monitor** (`file_monitor.py`)
- Watches sync directory using `watchdog`
- Detects: create, modify, delete, move events
- Debounces rapid events (0.5s window)
- Bridges sync events to async event loop
- Filters system files and temporary files

#### 2. **Sync Engine** (`sync_engine.py`)
- Central orchestrator for synchronization logic
- Maintains file metadata (Merkle trees + Vector clocks)
- Handles incoming sync requests from peers
- Implements conflict detection and resolution
- Manages chunk-based file transfers

#### 3. **Network Manager** (`network.py`)
- Manages TCP server and client connections
- Implements connection pooling
- Handles peer discovery and reconnection
- Exponential backoff for failed connections
- Heartbeat mechanism for connection health

## Core Algorithms

### Merkle Trees

#### Purpose
Efficiently detect which parts of a file have changed, minimizing bandwidth usage.

#### Implementation

```python
File: "example.txt" (10 KB)

Step 1: Chunk the file
┌────────┬────────┬────────┐
│Chunk 0 │Chunk 1 │Chunk 2 │  (Each 4KB)
│(4KB)   │(4KB)   │(2KB)   │
└────────┴────────┴────────┘

Step 2: Hash each chunk
H(C0) = sha256(Chunk 0) = "a1b2..."
H(C1) = sha256(Chunk 1) = "c3d4..."
H(C2) = sha256(Chunk 2) = "e5f6..."

Step 3: Build tree bottom-up
                 Root
              H(H0+H1+H2)
                /      \
           H(H0+H1)    H2
           /      \
         H0        H1

Step 4: Compare trees
- If root hashes match → files identical
- If different → recursively traverse to find differing chunks
```

#### Optimization Techniques

1. **Chunk Size Selection**: 4KB chosen for:
   - Matches typical filesystem block size
   - Balances tree depth vs. granularity
   - Optimal for SSD random reads

2. **SHA-256 Hashing**: 
   - Cryptographically secure (collision resistant)
   - Fast on modern CPUs (hardware acceleration)
   - 256-bit output suitable for tree nodes

3. **Tree Balancing**:
   - Binary tree structure for O(log n) traversal
   - Automatically handles odd number of chunks

#### Bandwidth Savings

Example: 100MB file, 1MB changed

**Traditional sync**: 100MB transferred
**Merkle sync**: ~1MB transferred (99% reduction!)

```
Chunks in 100MB file (4KB chunks): 25,600
Changed chunks: 256 (1MB / 4KB)
Transfer ratio: 256/25,600 = 1%
```

### Vector Clocks

#### Purpose
Track causality between events across distributed nodes to detect conflicts.

#### Implementation

Each node maintains a vector of logical timestamps:

```python
# Initial state (3 nodes)
Node1: {node1: 0, node2: 0, node3: 0}
Node2: {node1: 0, node2: 0, node3: 0}
Node3: {node1: 0, node2: 0, node3: 0}

# Node1 edits file → increment its counter
Node1: {node1: 1, node2: 0, node3: 0}

# Node1 syncs to Node2 → Node2 merges and increments
Node2: {node1: 1, node2: 1, node3: 0}

# Meanwhile, Node3 edits offline → concurrent!
Node3: {node1: 0, node2: 0, node3: 1}

# When Node1 and Node3 sync:
Node1: {node1: 1, node2: 0, node3: 0}
Node3: {node1: 0, node2: 0, node3: 1}

Comparison: node1 > node3 for 'node1', but node1 < node3 for 'node3'
Result: CONCURRENT → Conflict!
```

#### Comparison Algorithm

```python
def compare(clock_a, clock_b):
    less_than = any(clock_a[i] < clock_b[i] for i in all_nodes)
    greater_than = any(clock_a[i] > clock_b[i] for i in all_nodes)
    
    if not less_than and not greater_than:
        return EQUAL
    elif less_than and not greater_than:
        return BEFORE  # A happened before B
    elif greater_than and not less_than:
        return AFTER   # A happened after B
    else:
        return CONCURRENT  # Conflict!
```

#### Properties

- **Happened-Before Relation**: If A → B, then VC(A) < VC(B)
- **Concurrent Detection**: If VC(A) ⊥ VC(B), then A and B are concurrent
- **Monotonicity**: Vector clocks only increase

## Network Protocol

### Wire Format

```
┌─────────────────────────────────────────────────┐
│              Protocol Frame                     │
├─────────────────────────────────────────────────┤
│  Length Prefix (4 bytes, network byte order)   │
│  ┌───────────────────────────────────────────┐ │
│  │  0x00 0x00 0x01 0x2C (300 bytes)          │ │
│  └───────────────────────────────────────────┘ │
├─────────────────────────────────────────────────┤
│  Message Body (N bytes, JSON)                  │
│  ┌───────────────────────────────────────────┐ │
│  │ {                                         │ │
│  │   "type": "SYNC_REQ",                     │ │
│  │   "node_id": "node1",                     │ │
│  │   "payload": {                            │ │
│  │     "file_path": "test.txt",              │ │
│  │     "root_hash": "a1b2c3...",             │ │
│  │     "vector_clock": {...}                 │ │
│  │   }                                       │ │
│  │ }                                         │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

### Message Types and Flow

#### 1. **Connection Establishment**

```
Node A                              Node B
  │                                   │
  ├─────────── HELLO ────────────────▶│
  │   {node_id: "nodeA"}              │
  │                                   │
  │◀────────── HELLO_ACK ─────────────┤
  │   {node_id: "nodeB"}              │
  │                                   │
  │    Connection Established         │
```

#### 2. **File Synchronization**

```
Node A (file modified)              Node B
  │                                   │
  ├────────── SYNC_REQ ──────────────▶│
  │  {file, root_hash, vclock}        │
  │                                   │
  │◀───── MERKLE_TREE (request) ──────┤
  │                                   │
  ├────────── MERKLE_TREE ───────────▶│
  │  {tree structure}                 │
  │                                   │
  │◀─────── CHUNK_REQ ────────────────┤
  │  {chunk_indices: [3, 7, 12]}      │
  │                                   │
  ├──────── CHUNK_DATA ──────────────▶│
  │  {index: 3, data: ...}            │
  ├──────── CHUNK_DATA ──────────────▶│
  │  {index: 7, data: ...}            │
  ├──────── CHUNK_DATA ──────────────▶│
  │  {index: 12, data: ...}           │
  │                                   │
  │        Sync Complete              │
```

#### 3. **Heartbeat**

```
Every 30 seconds:

Node A ─────── HEARTBEAT ──────────▶ Node B
       ◀────── (implicit ACK) ───────

If no heartbeat for 90s → consider peer disconnected
```

### Protocol Features

1. **Length Prefixing**: Solves TCP stream fragmentation
2. **JSON Payload**: Human-readable, easy to debug
3. **Base64 for Binary**: Chunks encoded for JSON compatibility
4. **Max Message Size**: 10MB limit prevents memory exhaustion

## Concurrency Model

### AsyncIO Event Loop

```python
┌─────────────────────────────────────────────────┐
│           AsyncIO Event Loop                    │
├─────────────────────────────────────────────────┤
│                                                 │
│  ┌─────────────────────────────────────────┐   │
│  │      I/O Tasks (Non-blocking)           │   │
│  │  • TCP accept() connections             │   │
│  │  • TCP read()/write() data              │   │
│  │  • File read()/write() operations       │   │
│  │  • Sleep() for timers/delays            │   │
│  └─────────────────────────────────────────┘   │
│                    ▲                            │
│                    │ yield control              │
│                    │                            │
│  ┌─────────────────▼───────────────────────┐   │
│  │    Task Scheduler (coroutines)          │   │
│  │  • Server accept handler                │   │
│  │  • Client connection handler            │   │
│  │  • Message processor                    │   │
│  │  • File monitor callbacks               │   │
│  │  • Heartbeat sender                     │   │
│  └─────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

### Task Management

```python
# Each connection gets its own task
async def handle_client(reader, writer):
    while True:
        msg = await receive_message(reader)  # Yields to event loop
        await process_message(msg)           # Yields to event loop
        
# Multiple connections run concurrently
tasks = [
    handle_client(reader1, writer1),  # Task 1
    handle_client(reader2, writer2),  # Task 2
    handle_client(reader3, writer3),  # Task 3
    heartbeat_loop(),                  # Task 4
]
await asyncio.gather(*tasks)
```

### Benefits

1. **Single-threaded**: No race conditions, simpler reasoning
2. **Efficient**: No thread context switching overhead
3. **Scalable**: Can handle thousands of connections
4. **Cooperative**: Explicit yield points (`await`)

## Conflict Resolution

### Detection Strategy

```python
def detect_conflict(local_vclock, remote_vclock):
    relation = local_vclock.compare(remote_vclock)
    
    if relation == CONCURRENT:
        # Conflict detected!
        return True
    return False
```

### Resolution Strategy: Last-Write-Wins with Backup

```python
Step 1: Detect concurrent edit
  Local:  {n1:5, n2:3} → "Hello World"
  Remote: {n1:3, n2:5} → "Goodbye World"
  Result: CONCURRENT

Step 2: Backup local version
  Save as: "file.txt_conflict_n1_1738012345"

Step 3: Accept remote version
  Overwrite local with remote

Step 4: Merge vector clocks
  Local: {n1:6, n2:6}  (max of each component + 1)
```

### Alternative Strategies (Not Implemented)

1. **Operational Transformation**: Track actual edit operations
2. **Three-Way Merge**: Use common ancestor as base
3. **CRDT**: Conflict-Free Replicated Data Types
4. **Manual Resolution**: Prompt user to choose version

## Performance Characteristics

### Time Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Build Merkle Tree | O(n) | n = number of chunks |
| Compare Trees | O(log n) | Binary tree traversal |
| Vector Clock Compare | O(m) | m = number of nodes |
| Vector Clock Merge | O(m) | Linear in node count |
| Hash Chunk | O(k) | k = chunk size (constant) |

### Space Complexity

| Component | Space | Notes |
|-----------|-------|-------|
| Merkle Tree | O(n) | One node per chunk |
| Vector Clock | O(m) | One entry per node |
| Chunk Buffer | O(k) | Temporary during transfer |
| Connection State | O(p) | p = number of peers |

### Bandwidth Usage

For file of size F with C changed bytes:

- **Traditional**: F bytes
- **Merkle-based**: C bytes + O(log n) tree metadata

Example: 1GB file, 10MB changed:
- Traditional: 1GB transfer
- Merkle: ~10MB transfer (100x improvement!)

## Design Decisions

### Why Python?

**Pros:**
- Excellent async support (asyncio)
- Rich library ecosystem
- Rapid prototyping
- Easy to understand and maintain

**Cons:**
- Slower than Rust/Go for CPU-intensive tasks
- GIL limits true parallelism
- Higher memory usage

**Mitigation**: Network I/O is the bottleneck, not CPU

### Why AsyncIO over Threading?

**Threading challenges:**
- Race conditions
- Deadlocks
- Complex synchronization
- Higher memory per thread

**AsyncIO benefits:**
- Single-threaded simplicity
- Explicit control flow
- Lower memory footprint
- Better debugging

### Why 4KB Chunks?

**Considerations:**
- Too small: High tree depth, many network messages
- Too large: Poor granularity, wasted bandwidth

**4KB chosen:**
- Matches typical filesystem block size
- ~256 chunks per 1MB
- Tree depth ~8 for 1GB file
- Good balance for network latency

### Why JSON Protocol?

**Alternatives:**
- Protocol Buffers: More efficient, harder to debug
- MessagePack: Binary JSON, slightly better
- Custom binary: Most efficient, most complex

**JSON chosen:**
- Human-readable for debugging
- Easy to extend
- Good enough for metadata (not bulk data)
- Future: Hybrid approach for chunk transfers

### Why TCP over UDP?

**TCP chosen:**
- Reliable, ordered delivery
- Built-in congestion control
- Simpler programming model
- Connection-oriented (matches use case)

**UDP drawbacks:**
- Would require implementing reliability
- Out-of-order packet handling
- More complex error recovery

## Future Enhancements

### 1. Security

Since implemented — see `app/core/security.py` and `docs/SECURITY_AUDIT.md`:

- ✅ Encryption in transit (AES-256-GCM, per-connection and per-direction keys)
- ✅ Mutual authentication (HMAC challenge/response over a PBKDF2-stretched
  pre-shared key)
- ✅ Path sanitisation on every received filename, with containment re-checked
  after symlink resolution

Still outstanding:

- Certificate-based authentication / mTLS, so peers can be revoked
  individually instead of by rotating one shared key everywhere
- Access control lists — every authenticated peer can currently read and write
  every synced path
- Encryption at rest

### 2. Performance

The measured numbers in `artifacts/fuse_benchmark.md` make the priority order
concrete rather than speculative:

- **Descend the Merkle tree instead of shipping its leaves.** Measured: a
  1-byte edit in a 100MB file costs 1.67MB on the wire, of which **98.2% is
  the hex-encoded leaf-hash list** and 0.3% is data. Exchanging the root and
  recursing only into differing subtrees would make that ~2KB. This is the
  single highest-value change in the project.
- **Raw digests instead of hex** in the tree encoding — ~38% off the above for
  a serialisation change alone.
- **Batch chunks per message, and stop base64-ing payloads inside JSON.**
  Measured: a first-time 100MB transfer over *loopback* runs at 2.5 MB/s
  across 25,600 separately framed and encrypted messages.
- Parallel chunk transfers
- Compression (gzip/lz4)
- Memory-mapped file I/O

### 3. Features
- Selective sync (ignore patterns)
- Bandwidth throttling
- Conflict resolution UI
- Web dashboard
- Mobile clients

### 4. Reliability
- Persistent metadata (SQLite)
- Transaction log
- Crash recovery
- Network partition handling

---

This architecture provides a solid foundation for a distributed file synchronization system, balancing simplicity with production-grade features.
