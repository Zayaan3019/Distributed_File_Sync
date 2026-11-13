"""
Core Synchronization Engine.

This module implements the main synchronization logic:
- Computes Merkle trees for changed files
- Manages vector clocks for conflict detection
- Coordinates file chunk transfers
- Handles conflict resolution
- Persists all of the above to the SQLite catalog

Changes made in the FUSE hardening pass, each traceable to a Phase 0 finding
or a Phase 1 design decision:

* **Every path received from a peer goes through `sanitize_relative_path`.**
  The audit demonstrated a working arbitrary-file-write: a peer sending
  ``file_path="../VICTIM.txt"`` overwrote a file outside the sync root.
* **Transfers are applied atomically** via a temp file plus `os.replace`,
  with an `fsync` before the rename. Previously chunks were written straight
  into the destination inode, so a crash mid-apply left a file that was
  neither the old nor the new version.
* **The catalog is now wired in.** It existed but had zero callers, so all
  Merkle and vector-clock state still died with the process. Restarting a
  node reset its clock from 4 to 1 in the audit probe, which makes a stale
  peer look causally newer and silently overwrite good data.
* **Deletions write tombstones** instead of just forgetting the file.
* **Conflicts produce a deterministic sibling file** rather than a local-only
  backup, so both peers converge on identical directory contents.
"""

import asyncio
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
import aiofiles
import shutil

from ..algorithms.merkle import MerkleTree, TreeDiff, CHUNK_SIZE
from ..algorithms.vector_clock import VectorClock, ClockRelation
from ..core.catalog import Catalog, normalize_path
from ..core.config import SyncConfig
from ..core.protocol import Message, MessageType, Protocol
from ..core.security import (
    PathTraversalError,
    safe_join,
    sanitize_relative_path,
)
from ..services.network import NetworkManager, PeerConnection

logger = logging.getLogger(__name__)

#: Directory, relative to the sync root, holding the catalog and scratch files.
#: Excluded from sync: replicating our own database to peers would be both
#: wasteful and actively harmful.
INTERNAL_DIR = ".dfs"

#: Marker used for conflict siblings. Matches the Dropbox-style presentation
#: chosen in docs/FUSE_DESIGN.md section 4.
CONFLICT_INFIX = ".conflict-"

#: Seconds a transfer may go without receiving a chunk before it is presumed
#: dead and restarted. Generous, because a large file over a slow link is not
#: a stall -- progress is measured per chunk, not for the transfer as a whole,
#: so this only fires when the stream has genuinely stopped.
TRANSFER_STALL_TIMEOUT = 60.0

#: Sends of one tree request before giving up on it. A peer that does not
#: have the file never replies, so retrying forever would loop.
MAX_TREE_REQUEST_ATTEMPTS = 3


def conflict_sibling_path(path: str, loser_node_id: str, loser_counter: int) -> str:
    """
    Build the path of the conflict sibling for a losing version.

    Both peers must independently derive the *same* filename with no further
    communication, or they diverge permanently. That rules out anything drawn
    from local state: wall-clock time differs between machines, and so does
    mtime, because we do not replicate it byte-for-byte.

    The loser's own vector-clock counter is used instead. It is the one value
    both peers demonstrably agree on -- each learned it from the same
    advertisement -- and it is monotonic, so successive conflicts on the same
    path do not collide.

    Args:
        path: The contested path, relative to the sync root.
        loser_node_id: Node id of the version that loses `path`.
        loser_counter: That node's counter in the losing version's clock.

    Returns:
        Relative path for the conflict sibling.

    Example:
        >>> conflict_sibling_path("notes.txt", "node_b", 7)
        'notes.txt.conflict-node_b-7'
    """
    return f"{path}{CONFLICT_INFIX}{loser_node_id}-{loser_counter}"


def is_conflict_sibling(path: str) -> bool:
    """
    Report whether a path is itself a conflict sibling.

    A conflict on a conflict file would recurse without bound, so siblings
    never generate siblings of their own.

    Args:
        path: Relative path to test.

    Returns:
        True if the path was produced by `conflict_sibling_path`.
    """
    return CONFLICT_INFIX in path


@dataclass
class FileMetadata:
    """
    Metadata for a synchronized file.

    Attributes:
        file_path: Relative path to the file.
        merkle_tree: Merkle tree of the file.
        vector_clock: Vector clock for version tracking.
        last_modified: Timestamp of last modification.
        size: File size in bytes, as last recorded.
    """
    file_path: str
    merkle_tree: MerkleTree
    vector_clock: VectorClock
    last_modified: float = field(default_factory=time.time)
    size: int = 0


@dataclass
class TreeRequest:
    """
    A Merkle-tree request that has been sent but not yet answered.

    The request is the first message of every transfer, and it is the one most
    exposed to a dying socket: it is sent the moment a change is noticed, which
    is exactly when a peer pair may still be settling a duplicate connection.
    If it goes out on the connection that then loses the tie-break, the peer
    closes that socket without reading it and the request is gone without an
    error on either side. Until the reply arrives the request is the only trace
    that a transfer should happen, so it is kept here and re-sent when its
    connection dies (`SyncEngine.on_connection_lost`) or goes unanswered
    (`SyncEngine.reap_stalled_transfers`).

    Attributes:
        peer_id: Peer the request was sent to.
        source_path: Path as the peer knows it.
        conn: The connection it was sent on.
        sent_at: Monotonic timestamp of the latest send.
        attempts: Sends so far, bounded by `MAX_TREE_REQUEST_ATTEMPTS`.
    """
    peer_id: str
    source_path: str
    conn: Any
    sent_at: float
    attempts: int = 1


@dataclass
class PendingTransfer:
    """
    State for one in-flight chunk transfer.

    A transfer is only applied when `expected` empties. Applying it
    incrementally is what let a partial transfer destroy the destination file.

    The distinction between `source_path` and `dest_path` is what makes
    conflict siblings work: the peer is sending us its version of ``f.txt``,
    but we may be writing it to ``f.txt.conflict-<peer>-<n>`` because our own
    version won the path.

    Attributes:
        source_path: Path the peer is sending, as it knows it.
        dest_path: Path we will write locally.
        expected: Chunk indices still outstanding.
        chunks: Chunk data received so far.
        remote_clock: Clock to adopt once the transfer completes.
        remote_size: Size the finished file should have.
        remote_leaves: Remote leaf hashes, for verifying what we assembled.
        peer_id: Peer we asked for the chunks, so a stalled transfer can be
            re-requested from the same source.
        started_at: Monotonic timestamp of creation.
        last_progress: Monotonic timestamp of the most recent chunk. Read by
            `SyncEngine.reap_stalled_transfers`; a transfer whose sender died
            mid-stream would otherwise sit here forever and the file would
            never converge.
    """
    source_path: str
    dest_path: str
    expected: Set[int] = field(default_factory=set)
    chunks: Dict[int, bytes] = field(default_factory=dict)
    remote_clock: Optional[VectorClock] = None
    remote_size: int = 0
    remote_leaves: List[str] = field(default_factory=list)
    peer_id: Optional[str] = None
    started_at: float = field(default_factory=time.monotonic)
    last_progress: float = field(default_factory=time.monotonic)


class SyncEngine:
    """
    Main synchronization engine.

    This class orchestrates the entire synchronization process:
    1. Monitors local file changes via file monitor callbacks
    2. Computes Merkle trees and updates vector clocks
    3. Broadcasts sync requests to peers
    4. Handles incoming sync requests and resolves conflicts
    5. Transfers file chunks efficiently
    6. Persists metadata so a restart does not lose causal history

    Attributes:
        config: Node configuration.
        network: Network manager for peer communication.
        catalog: Durable metadata store, or None when persistence is disabled.
        file_metadata: In-memory metadata index, keyed by relative path.
        transfers: In-flight chunk transfers, keyed by destination path.
    """

    def __init__(self, config: SyncConfig, network: NetworkManager,
                 catalog: Optional[Catalog] = None):
        """
        Initialize the sync engine.

        Args:
            config: Node configuration.
            network: Network manager instance.
            catalog: Optional pre-opened catalog. When omitted and
                `config.enable_catalog` is set, one is created on
                `initialize()`.
        """
        self.config = config
        self.network = network
        self.catalog = catalog
        self.file_metadata: Dict[str, FileMetadata] = {}
        self.transfers: Dict[str, PendingTransfer] = {}
        # Tree requests sent but not yet answered, by destination path.
        self._awaiting_trees: Dict[str, TreeRequest] = {}
        self._sync_lock = asyncio.Lock()
        self._initializing = False
        self._owns_catalog = False
        #: Paths whose local mutation we should not echo back as a change
        #: event. Applying a peer's data writes to the backing store, which
        #: the watchdog observer sees as a local edit; without this the node
        #: would tick its own clock and advertise the peer's own data back.
        self._suppress_events: Dict[str, float] = {}

        # Recover in-flight transfers the moment their connection dies rather
        # than waiting for the stall timeout to notice.
        network.connection_lost_handler = self.on_connection_lost

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    def _get_absolute_path(self, relative_path: str) -> str:
        """
        Convert relative path to absolute path within sync directory.

        Args:
            relative_path: Relative file path.

        Returns:
            Absolute file path.
        """
        return str(Path(self.config.sync_directory) / relative_path)

    def _safe_absolute_path(self, relative_path: str) -> Path:
        """
        Resolve a peer-supplied relative path inside the sync root.

        Args:
            relative_path: Path as received from a peer.

        Returns:
            Absolute path guaranteed to be within the sync root.

        Raises:
            PathTraversalError: If the path escapes the root.
        """
        return safe_join(self.config.sync_directory, relative_path)

    def _internal_dir(self) -> Path:
        """
        Return the private scratch/catalog directory, creating it if needed.

        Returns:
            Absolute path of the internal directory.
        """
        internal = Path(self.config.sync_directory) / INTERNAL_DIR
        internal.mkdir(parents=True, exist_ok=True)
        return internal

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """
        Initialize the sync engine, restoring state from the catalog.

        With a catalog, startup costs one `stat` per file plus a re-hash of
        only those whose (size, mtime) disagree. Without one, every file is
        re-hashed unconditionally -- which measured 8.5s for a single 100MB
        file and is the reason the catalog exists.
        """
        logger.info("Initializing sync engine...")
        self._initializing = True

        try:
            if self.catalog is None and self.config.enable_catalog:
                self.catalog = Catalog(
                    self.config.catalog_path,
                    tombstone_ttl_days=self.config.tombstone_ttl_days,
                )
                await self.catalog.open()
                self._owns_catalog = True

            if self.catalog is not None:
                await self._initialize_from_catalog()
            else:
                await self._initialize_by_scanning()

            logger.info(f"Initialized {len(self.file_metadata)} files")
        finally:
            self._initializing = False

    async def _initialize_from_catalog(self) -> None:
        """
        Restore metadata from the catalog, re-hashing only what changed.
        """
        report = await self.catalog.reconcile(
            self.config.sync_directory,
            self.config.node_id,
            ignore=self._should_ignore_relative,
        )
        logger.info(f"Catalog reconcile: {report.summary()}")

        for path in await self.catalog.all_paths():
            record = await self.catalog.get_file(path)
            if record is None or record.is_dir:
                continue

            chunk_hashes = await self.catalog.get_chunk_hashes(path)
            clock = await self.catalog.get_clock(path, self.config.node_id)
            if clock is None:
                clock = VectorClock(self.config.node_id)

            tree = MerkleTree.from_leaf_hashes(
                chunk_hashes, record.size, self.config.chunk_size,
                self._get_absolute_path(path),
            )

            self.file_metadata[path] = FileMetadata(
                file_path=path,
                merkle_tree=tree,
                vector_clock=clock,
                last_modified=record.mtime,
                size=record.size,
            )

    async def _initialize_by_scanning(self) -> None:
        """
        Restore metadata by re-hashing the whole tree (no catalog).

        This is the original behaviour, retained for `enable_catalog=False`.
        """
        sync_dir = Path(self.config.sync_directory)

        for file_path in sync_dir.rglob('*'):
            if file_path.is_file() and not self._should_ignore(str(file_path)):
                relative_path = normalize_path(
                    str(file_path.relative_to(sync_dir))
                )
                try:
                    await self._process_local_file(relative_path, notify=False)
                except Exception as e:
                    logger.error(f"Error initializing file {relative_path}: {e}")

    async def close(self) -> None:
        """Release the catalog if this engine opened it."""
        if self.catalog is not None and self._owns_catalog:
            await self.catalog.close()
            self.catalog = None
            self._owns_catalog = False

    # ------------------------------------------------------------------
    # Ignore rules
    # ------------------------------------------------------------------

    def _should_ignore_relative(self, relative_path: str) -> bool:
        """
        Check whether a relative path is excluded from sync.

        Args:
            relative_path: Path relative to the sync root.

        Returns:
            True if the path should be skipped.
        """
        canonical = normalize_path(relative_path)
        if not canonical:
            return True

        first = canonical.split("/")[0]
        if first == INTERNAL_DIR:
            return True

        ignored_patterns = {'.tmp', '.temp', '.swp', '__pycache__', '.git',
                            '_conflict'}
        return any(pattern in canonical for pattern in ignored_patterns)

    def _should_ignore(self, file_path: str) -> bool:
        """
        Check if an absolute path should be ignored.

        Args:
            file_path: Absolute or relative file path.

        Returns:
            True if the path should be skipped.
        """
        text = str(file_path).replace("\\", "/")
        if f"/{INTERNAL_DIR}/" in text or text.endswith(f"/{INTERNAL_DIR}"):
            return True
        ignored_patterns = {'.tmp', '.temp', '.swp', '__pycache__', '.git',
                            '_conflict'}
        return any(pattern in text for pattern in ignored_patterns)

    def _suppress(self, relative_path: str) -> None:
        """
        Mark a path as remotely written, so the resulting watchdog event is
        not mistaken for a local edit.

        Args:
            relative_path: Path we are about to write on a peer's behalf.
        """
        self._suppress_events[relative_path] = time.monotonic()

    def _is_suppressed(self, relative_path: str, window: float = 5.0) -> bool:
        """
        Check and consume a suppression marker.

        Args:
            relative_path: Path being reported as changed.
            window: Seconds a marker stays valid. Bounded so a marker whose
                event never arrives cannot suppress a genuine later edit.

        Returns:
            True if this change originated from a remote apply.
        """
        stamp = self._suppress_events.get(relative_path)
        if stamp is None:
            return False
        if time.monotonic() - stamp > window:
            self._suppress_events.pop(relative_path, None)
            return False
        self._suppress_events.pop(relative_path, None)
        return True

    # ------------------------------------------------------------------
    # Local events
    # ------------------------------------------------------------------

    async def on_file_changed(self, relative_path: str) -> None:
        """
        Handle local file change event.

        Args:
            relative_path: Relative path to changed file.
        """
        relative_path = normalize_path(relative_path)

        if self._should_ignore_relative(relative_path):
            return

        if self._is_suppressed(relative_path):
            logger.debug(f"Ignoring echo of remote write: {relative_path}")
            return

        logger.info(f"Processing file change: {relative_path}")

        try:
            await self._process_local_file(relative_path, notify=True)
        except Exception as e:
            logger.error(f"Error processing file change {relative_path}: {e}",
                         exc_info=True)

    async def on_file_deleted(self, relative_path: str) -> None:
        """
        Handle local file deletion event.

        Writes a tombstone carrying an advanced clock. Without one, a peer
        that missed the delete re-advertises the file and it resurrects --
        there is nothing to out-vote a stale advertisement.

        Args:
            relative_path: Relative path to deleted file.
        """
        relative_path = normalize_path(relative_path)

        if self._should_ignore_relative(relative_path):
            return

        if self._is_suppressed(relative_path):
            return

        logger.info(f"Processing file deletion: {relative_path}")

        try:
            async with self._sync_lock:
                metadata = self.file_metadata.get(relative_path)
                if metadata is None:
                    return

                clock = metadata.vector_clock
                clock.increment()

                if self.catalog is not None:
                    await self.catalog.delete_file(relative_path, clock)

                del self.file_metadata[relative_path]

                if not self._initializing:
                    delete_msg = Message(
                        msg_type=MessageType.FILE_DELETE,
                        payload={
                            'file_path': relative_path,
                            'vector_clock': clock.to_dict()
                        },
                        node_id=self.config.node_id
                    )
                    await self.network.broadcast(delete_msg)

                logger.info(f"File {relative_path} deleted and tombstoned")

        except Exception as e:
            logger.error(f"Error processing file deletion {relative_path}: {e}",
                         exc_info=True)

    async def on_file_renamed(self, old_path: str, new_path: str) -> None:
        """
        Handle a local rename.

        Sent as a single FILE_RENAME to peers that speak protocol >= 1.1.
        Falling back to delete + create re-transmits the whole file, which for
        a 100MB file means shipping 100MB to move it -- exactly the cost the
        Merkle delta design exists to avoid.

        Args:
            old_path: Previous relative path.
            new_path: New relative path.
        """
        old_path = normalize_path(old_path)
        new_path = normalize_path(new_path)

        if self._should_ignore_relative(new_path):
            return

        async with self._sync_lock:
            metadata = self.file_metadata.pop(old_path, None)
            if metadata is None:
                # We never knew the source; treat the destination as new.
                pass
            else:
                metadata.file_path = new_path
                metadata.vector_clock.increment()
                metadata.merkle_tree.file_path = self._get_absolute_path(new_path)
                self.file_metadata[new_path] = metadata

                if self.catalog is not None:
                    await self.catalog.rename(old_path, new_path,
                                              metadata.vector_clock)

        if metadata is None:
            await self._process_local_file(new_path, notify=True)
            return

        if self._initializing:
            return

        clock_dict = metadata.vector_clock.to_dict()
        rename_msg = Protocol.create_rename_message(
            old_path, new_path, clock_dict, self.config.node_id
        )

        for peer_id, conn in list(self.network.connections.items()):
            if conn.supports_rename():
                await self.network.send_to_peer(peer_id, rename_msg)
            else:
                # Legacy peer: emulate with delete + advertise.
                logger.info(
                    f"Peer {peer_id} speaks protocol {conn.protocol_version}; "
                    f"emulating rename as delete + create"
                )
                await self.network.send_to_peer(peer_id, Message(
                    msg_type=MessageType.FILE_DELETE,
                    payload={'file_path': old_path, 'vector_clock': clock_dict},
                    node_id=self.config.node_id,
                ))
                await self.network.send_to_peer(peer_id, Protocol.create_sync_request(
                    new_path, metadata.merkle_tree.root.hash, clock_dict,
                    self.config.node_id,
                ))

    async def _process_local_file(self, relative_path: str, notify: bool = True,
                                  local_event: bool = True,
                                  remote_clock: Optional[VectorClock] = None,
                                  known_tree: Optional[MerkleTree] = None) -> None:
        """
        Process a file change (compute Merkle tree, update vector clock).

        Args:
            relative_path: Relative path to file.
            notify: Whether to notify peers of the change.
            local_event: True when this node originated the change. Only then
                does the local counter tick. Rebuilding metadata after applying
                a peer's chunks is NOT a local event: ticking there would make
                this node strictly AFTER the peer that just sent us the data,
                so we would immediately advertise a "newer" version back and
                the two peers would ping-pong forever.
            remote_clock: Clock to adopt when applying a remote update.
            known_tree: Pre-computed tree, to skip re-hashing a file we just
                assembled chunk by chunk.
        """
        async with self._sync_lock:
            await self._process_local_file_locked(
                relative_path, notify=notify, local_event=local_event,
                remote_clock=remote_clock, known_tree=known_tree,
            )

    async def _process_local_file_locked(self, relative_path: str,
                                         notify: bool = True,
                                         local_event: bool = True,
                                         remote_clock: Optional[VectorClock] = None,
                                         known_tree: Optional[MerkleTree] = None) -> None:
        """
        Body of `_process_local_file`, assuming the sync lock is already held.

        This split exists because `asyncio.Lock` is **not reentrant**.
        `_handle_sync_request` takes the lock, and on a concurrent-edit
        verdict calls `_handle_conflict`, which needs to record the conflict
        sibling. Calling the locking variant there deadlocked the engine
        permanently -- the task simply never woke up, and the whole two-node
        suite hung with no error.

        Args:
            relative_path: Relative path to file.
            notify: Whether to notify peers of the change.
            local_event: True when this node originated the change.
            remote_clock: Clock to adopt when applying a remote update.
            known_tree: Pre-computed tree, to skip re-hashing.
        """
        absolute_path = self._get_absolute_path(relative_path)

        if not Path(absolute_path).exists():
            logger.warning(f"File no longer exists: {relative_path}")
            return

        if known_tree is not None:
            merkle_tree = known_tree
        else:
            merkle_tree = await MerkleTree.from_file(
                absolute_path, self.config.chunk_size
            )

        stat = Path(absolute_path).stat()

        metadata = self.file_metadata.get(relative_path)
        if metadata is not None:
            if local_event:
                metadata.vector_clock.increment()
            metadata.merkle_tree = merkle_tree
            metadata.last_modified = stat.st_mtime
            metadata.size = stat.st_size
        else:
            vclock = VectorClock(self.config.node_id)
            if local_event:
                vclock.increment()
            metadata = FileMetadata(
                file_path=relative_path,
                merkle_tree=merkle_tree,
                vector_clock=vclock,
                last_modified=stat.st_mtime,
                size=stat.st_size,
            )
            self.file_metadata[relative_path] = metadata

        if remote_clock is not None:
            metadata.vector_clock.merge(remote_clock)

        if self.catalog is not None:
            await self.catalog.record_file(
                relative_path,
                size=stat.st_size,
                mtime=stat.st_mtime,
                mode=stat.st_mode,
                chunk_hashes=merkle_tree.leaf_hashes(),
                root_hash=merkle_tree.root.hash,
                clock=metadata.vector_clock,
                op="write" if local_event else "apply",
            )

        logger.info(
            f"File {relative_path} processed: "
            f"hash={merkle_tree.root.hash[:16]}..."
        )

        if notify and not self._initializing:
            sync_req = Protocol.create_sync_request(
                relative_path,
                merkle_tree.root.hash,
                metadata.vector_clock.to_dict(),
                self.config.node_id
            )
            await self.network.broadcast(sync_req)

    async def apply_local_write(self, relative_path: str,
                                dirty_chunks: Set[int]) -> None:
        """
        Record a local write, re-hashing only the chunks that changed.

        This is the entry point the FUSE write path uses, and the reason it
        exists is measurement: building a Merkle tree for a 100MB file takes
        **8.48s**. Re-hashing the whole file on every flush would make a
        sequential write to a large file quadratic and the mount unusable.
        Re-hashing only the dirtied 4KB chunks and rebuilding the root from
        the amended leaf list is O(dirty chunks) plus O(total chunks) of
        cheap in-memory hashing.

        Args:
            relative_path: Path that was written, relative to the sync root.
            dirty_chunks: Chunk indices touched since the last flush.
        """
        relative_path = normalize_path(relative_path)

        if self._should_ignore_relative(relative_path):
            return

        absolute_path = Path(self._get_absolute_path(relative_path))
        if not absolute_path.exists():
            logger.debug(f"Write flush for vanished path {relative_path}")
            return

        try:
            async with self._sync_lock:
                stat = absolute_path.stat()
                total_chunks = max(
                    1, (stat.st_size + self.config.chunk_size - 1) // self.config.chunk_size
                )

                metadata = self.file_metadata.get(relative_path)
                if metadata is None:
                    leaves: List[str] = []
                    previous_size = 0
                else:
                    leaves = list(metadata.merkle_tree.leaf_hashes())
                    previous_size = metadata.size

                dirty_chunks = set(dirty_chunks)

                # Grow or shrink the leaf list to match the new file length
                # before patching. A file that shrank keeps stale trailing
                # hashes otherwise, and the root would describe bytes that are
                # no longer there.
                if len(leaves) > total_chunks:
                    leaves = leaves[:total_chunks]
                elif len(leaves) < total_chunks:
                    # Newly appended chunks are dirty by definition.
                    dirty_chunks |= set(range(len(leaves), total_chunks))
                    leaves.extend([""] * (total_chunks - len(leaves)))

                if previous_size != stat.st_size:
                    # Any size change makes the final chunk's *length* change,
                    # and a chunk hash covers its length. Truncating 1MB to
                    # 5000 bytes leaves chunk 1 holding 904 bytes where it
                    # previously held 4096, so its recorded hash is stale even
                    # though the caller never "wrote" to it. Measured as a
                    # real mismatch against a full re-hash before this line
                    # existed.
                    dirty_chunks.add(total_chunks - 1)
                    old_tail = previous_size // self.config.chunk_size
                    if 0 <= old_tail < total_chunks:
                        dirty_chunks.add(old_tail)

                targets = sorted(
                    index for index in dirty_chunks if 0 <= index < total_chunks
                )
                # A leaf we never hashed must be read even if the caller did
                # not list it, or the root would be built over an empty string.
                targets = sorted(
                    set(targets) | {i for i, h in enumerate(leaves) if not h}
                )

                if stat.st_size == 0:
                    tree = MerkleTree.from_leaf_hashes(
                        [], 0, self.config.chunk_size, str(absolute_path)
                    )
                else:
                    async with aiofiles.open(absolute_path, "rb") as handle:
                        for index in targets:
                            await handle.seek(index * self.config.chunk_size)
                            data = await handle.read(self.config.chunk_size)
                            leaves[index] = MerkleTree._compute_hash(data)

                    tree = MerkleTree.from_leaf_hashes(
                        leaves, stat.st_size, self.config.chunk_size,
                        str(absolute_path),
                    )

                if metadata is None:
                    clock = VectorClock(self.config.node_id)
                    clock.increment()
                    metadata = FileMetadata(
                        file_path=relative_path,
                        merkle_tree=tree,
                        vector_clock=clock,
                        last_modified=stat.st_mtime,
                        size=stat.st_size,
                    )
                    self.file_metadata[relative_path] = metadata
                else:
                    if metadata.merkle_tree.root.hash == tree.root.hash:
                        # The bytes are identical to what we already
                        # advertised -- a rewrite with the same content. Do
                        # not tick the clock; that would manufacture a version
                        # peers must fetch for no benefit.
                        return
                    metadata.vector_clock.increment()
                    metadata.merkle_tree = tree
                    metadata.last_modified = stat.st_mtime
                    metadata.size = stat.st_size

                if self.catalog is not None:
                    await self.catalog.record_file(
                        relative_path,
                        size=stat.st_size,
                        mtime=stat.st_mtime,
                        mode=stat.st_mode,
                        chunk_hashes=tree.leaf_hashes(),
                        root_hash=tree.root.hash,
                        clock=metadata.vector_clock,
                        op="fuse-write",
                    )

                logger.info(
                    f"FUSE write {relative_path}: re-hashed {len(targets)} of "
                    f"{total_chunks} chunks, root={tree.root.hash[:16]}..."
                )

                sync_req = Protocol.create_sync_request(
                    relative_path, tree.root.hash,
                    metadata.vector_clock.to_dict(), self.config.node_id,
                )

            await self.network.broadcast(sync_req)

        except Exception as e:
            logger.error(f"Error applying local write to {relative_path}: {e}",
                         exc_info=True)

    # ------------------------------------------------------------------
    # Message dispatch
    # ------------------------------------------------------------------

    async def handle_message(self, message: Message,
                             peer_conn: PeerConnection) -> None:
        """
        Handle incoming message from a peer.

        Args:
            message: Received message.
            peer_conn: Connection to the peer.
        """
        try:
            if message.msg_type == MessageType.SYNC_REQ:
                await self._handle_sync_request(message, peer_conn)
            elif message.msg_type == MessageType.MERKLE_TREE:
                await self._handle_merkle_tree(message, peer_conn)
            elif message.msg_type == MessageType.CHUNK_REQ:
                await self._handle_chunk_request(message, peer_conn)
            elif message.msg_type == MessageType.CHUNK_DATA:
                await self._handle_chunk_data(message, peer_conn)
            elif message.msg_type == MessageType.FILE_DELETE:
                await self._handle_file_delete(message, peer_conn)
            elif message.msg_type == MessageType.FILE_RENAME:
                await self._handle_file_rename(message, peer_conn)
            else:
                logger.warning(f"Unhandled message type: {message.msg_type}")

        except PathTraversalError as e:
            # A peer sending an escaping path is either broken or hostile.
            # Either way the message is dropped and the event is recorded at
            # WARNING so it shows up in operational logs.
            logger.warning(
                f"SECURITY: rejected {message.msg_type.value} from "
                f"{peer_conn.peer_id}: {e}"
            )
        except Exception as e:
            logger.error(f"Error handling message {message.msg_type}: {e}",
                         exc_info=True)

    def _received_path(self, message: Message, key: str = 'file_path') -> str:
        """
        Extract and sanitise a path from a message payload.

        Args:
            message: The received message.
            key: Payload key holding the path.

        Returns:
            Canonical relative path.

        Raises:
            PathTraversalError: If the path escapes the sync root.
        """
        raw = message.payload.get(key)
        if raw is None:
            raise PathTraversalError(f"message missing {key}")
        return sanitize_relative_path(raw)

    # ------------------------------------------------------------------
    # SYNC_REQ
    # ------------------------------------------------------------------

    async def _handle_sync_request(self, message: Message,
                                   peer_conn: PeerConnection) -> None:
        """
        Handle SYNC_REQ from a peer.

        Compare vector clocks and Merkle roots to decide action.
        """
        file_path = self._received_path(message)
        remote_hash = message.payload['root_hash']
        remote_vclock = VectorClock.from_dict(message.payload['vector_clock'])

        logger.info(f"Received sync request for {file_path} from {peer_conn.peer_id}")

        # A tombstone we hold may out-rank the advertisement.
        if await self._tombstone_wins(file_path, remote_vclock, peer_conn):
            return

        async with self._sync_lock:
            local_metadata = self.file_metadata.get(file_path)

            if not local_metadata:
                logger.info(f"File {file_path} not found locally, requesting from peer")
                await self._request_merkle_tree(file_path, peer_conn)
                return

            relation = local_metadata.vector_clock.compare(remote_vclock)

            if relation == ClockRelation.EQUAL:
                if local_metadata.merkle_tree.root.hash != remote_hash:
                    logger.warning(
                        f"Clock mismatch for {file_path}: same clock but different hash"
                    )
                else:
                    logger.debug(f"File {file_path} already in sync")
                return

            elif relation == ClockRelation.BEFORE:
                # Remote is newer, request update. The clock is adopted in
                # _apply_transfer once the data has actually landed -- merging
                # it here would claim we hold the remote version while the
                # transfer is still in flight, and a failed transfer would
                # leave us advertising content we never received.
                logger.info(f"Remote version of {file_path} is newer, requesting update")
                await self._request_merkle_tree(file_path, peer_conn)

            elif relation == ClockRelation.AFTER:
                logger.info(f"Local version of {file_path} is newer, sending to peer")
                await self._send_merkle_tree(file_path, local_metadata, peer_conn)

            elif relation == ClockRelation.CONCURRENT:
                logger.warning(f"Conflict detected for {file_path}")
                await self._handle_conflict(
                    file_path, local_metadata, remote_vclock, peer_conn
                )

    # ------------------------------------------------------------------
    # Merkle tree exchange
    # ------------------------------------------------------------------

    async def _request_merkle_tree(self, file_path: str,
                                   peer_conn: PeerConnection,
                                   dest_path: Optional[str] = None) -> None:
        """
        Request a Merkle tree from a peer.

        Args:
            file_path: Path as the peer knows it.
            peer_conn: Connection to the peer.
            dest_path: Local path to write the result to, when it differs
                from `file_path` (conflict siblings).
        """
        dest = dest_path or file_path
        request = Message(
            msg_type=MessageType.MERKLE_TREE,
            payload={
                'file_path': file_path,
                'request': True,
                'dest_hint': dest,
            },
            node_id=self.config.node_id
        )

        previous = self._awaiting_trees.get(dest)
        attempts = 1
        if previous is not None and previous.peer_id == peer_conn.peer_id \
                and previous.source_path == file_path:
            attempts = previous.attempts + 1
        self._awaiting_trees[dest] = TreeRequest(
            peer_id=peer_conn.peer_id, source_path=file_path,
            conn=peer_conn, sent_at=time.monotonic(), attempts=attempts,
        )

        try:
            await Protocol.send_message(peer_conn.channel, request)
        except (ConnectionError, OSError):
            # The usual cause is a duplicate connection that just lost the
            # tie-break: the peer is still there, on the connection that
            # replaced this one. Anything else is left to the reconnect and
            # the reaper, which still have the record above.
            current = self._live_connection(peer_conn.peer_id)
            if current is None or current is peer_conn:
                raise
            logger.info(
                f"Connection to {peer_conn.peer_id} was replaced; re-sending "
                f"the tree request for {dest} on the new one"
            )
            self._awaiting_trees[dest].conn = current
            await Protocol.send_message(current.channel, request)

    def _live_connection(self, peer_id: str) -> Optional[PeerConnection]:
        """The connection currently registered for a peer, if any."""
        network = getattr(self, "network", None)
        connections = getattr(network, "connections", None)
        return connections.get(peer_id) if connections is not None else None

    async def _send_merkle_tree(self, file_path: str, metadata: FileMetadata,
                                peer_conn: PeerConnection,
                                dest_hint: Optional[str] = None) -> None:
        """
        Send a Merkle tree to a peer, in the compact leaf-only encoding.

        The requester's `dest_hint` is echoed back verbatim. That echo is what
        makes conflict siblings work on the *winning* side: the winner asks
        for the peer's version of ``f.txt`` but needs it written to
        ``f.txt.conflict-<peer>-<n>``, and without the echo the reply carries
        no destination and the peer's version lands on ``f.txt`` -- destroying
        the very version that just won.

        Args:
            file_path: Path being described.
            metadata: Local metadata for the file.
            peer_conn: Connection to the peer.
            dest_hint: Destination the requester asked us to tag the reply
                with. Defaults to `file_path`.
        """
        tree_msg = Message(
            msg_type=MessageType.MERKLE_TREE,
            payload={
                'file_path': file_path,
                'tree': metadata.merkle_tree.to_wire(),
                'vector_clock': metadata.vector_clock.to_dict(),
                'dest_hint': dest_hint or file_path,
            },
            node_id=self.config.node_id
        )
        await Protocol.send_message(peer_conn.channel, tree_msg)

    async def _handle_merkle_tree(self, message: Message,
                                  peer_conn: PeerConnection) -> None:
        """Handle a received Merkle tree, or a request for ours."""
        file_path = self._received_path(message)

        if message.payload.get('request'):
            requested_hint = message.payload.get('dest_hint')
            local_metadata = self.file_metadata.get(file_path)
            if local_metadata:
                await self._send_merkle_tree(
                    file_path, local_metadata, peer_conn,
                    dest_hint=sanitize_relative_path(requested_hint)
                    if requested_hint else None,
                )
            else:
                logger.debug(f"Peer asked for unknown file {file_path}")
            return

        remote_tree = MerkleTree.from_wire(message.payload['tree'])
        remote_vclock = VectorClock.from_dict(message.payload['vector_clock'])

        # The peer echoes back the destination we asked it about, so a tree
        # requested for a conflict sibling lands in the sibling.
        dest_path = file_path
        hint = message.payload.get('dest_hint')
        if hint:
            dest_path = sanitize_relative_path(hint)

        logger.info(f"Received Merkle tree for {file_path} -> {dest_path}")
        self._awaiting_trees.pop(dest_path, None)

        local_metadata = self.file_metadata.get(dest_path)

        transfer = PendingTransfer(
            source_path=file_path,
            dest_path=dest_path,
            remote_clock=remote_vclock,
            remote_size=remote_tree.file_size,
            remote_leaves=remote_tree.leaf_hashes(),
            peer_id=peer_conn.peer_id,
        )

        if local_metadata is None:
            total_chunks = len(transfer.remote_leaves) if remote_tree.file_size else 0
            needed = set(range(total_chunks))
        else:
            diff = MerkleTree.get_diff(local_metadata.merkle_tree, remote_tree)
            needed = set(diff.changed_chunks)

        if not needed:
            logger.info(f"No chunks to sync for {dest_path}")
            if local_metadata is not None:
                local_metadata.vector_clock.merge(remote_vclock)
                if self.catalog is not None:
                    await self._persist(dest_path, local_metadata)
            elif remote_tree.file_size == 0:
                # An empty file still has to exist locally.
                self.transfers[dest_path] = transfer
                await self._apply_transfer(dest_path)
            return

        transfer.expected = set(needed)
        self.transfers[dest_path] = transfer

        logger.info(f"Requesting {len(needed)} chunks for {dest_path}")
        chunk_req = Message(
            msg_type=MessageType.CHUNK_REQ,
            payload={
                'file_path': file_path,
                'chunk_indices': sorted(needed),
                'dest_hint': dest_path,
            },
            node_id=self.config.node_id
        )
        await Protocol.send_message(peer_conn.channel, chunk_req)

    # ------------------------------------------------------------------
    # Chunk exchange
    # ------------------------------------------------------------------

    async def _handle_chunk_request(self, message: Message,
                                    peer_conn: PeerConnection) -> None:
        """Handle a request for file chunks."""
        file_path = self._received_path(message)
        raw_indices = message.payload.get('chunk_indices', [])

        if not isinstance(raw_indices, list):
            raise ValueError("chunk_indices must be a list")

        # Bound the request. A peer asking for a million indices would
        # otherwise make us read and base64 a million chunks into memory.
        if len(raw_indices) > 100_000:
            raise ValueError(f"chunk request too large: {len(raw_indices)}")

        chunk_indices = {int(i) for i in raw_indices if isinstance(i, int) and i >= 0}
        dest_hint = message.payload.get('dest_hint') or file_path

        logger.info(
            f"Received chunk request for {file_path}: {len(chunk_indices)} chunks"
        )

        local_metadata = self.file_metadata.get(file_path)
        if not local_metadata:
            logger.warning(f"Cannot send chunks for unknown file {file_path}")
            return

        try:
            chunks = await local_metadata.merkle_tree.get_chunks(chunk_indices)

            for chunk_index, chunk_data in sorted(chunks.items()):
                import base64
                msg = Message(
                    msg_type=MessageType.CHUNK_DATA,
                    payload={
                        'file_path': file_path,
                        'chunk_index': chunk_index,
                        'chunk_data': base64.b64encode(chunk_data).decode('ascii'),
                        'chunk_size': len(chunk_data),
                        'dest_hint': dest_hint,
                    },
                    node_id=self.config.node_id,
                )
                await Protocol.send_message(peer_conn.channel, msg)
                logger.debug(f"Sent chunk {chunk_index} of {file_path}")

        except Exception as e:
            logger.error(f"Error sending chunks for {file_path}: {e}")

    async def _handle_chunk_data(self, message: Message,
                                 peer_conn: PeerConnection) -> None:
        """Handle a received file chunk."""
        file_path, chunk_index, chunk_data = Protocol.decode_chunk_data(message)

        dest_path = file_path
        hint = message.payload.get('dest_hint')
        if hint:
            dest_path = sanitize_relative_path(hint)

        transfer = self.transfers.get(dest_path)
        if transfer is None:
            # We never asked for this. Accepting it would let any authenticated
            # peer push arbitrary content into arbitrary paths without us ever
            # having compared clocks.
            logger.warning(
                f"Dropping unsolicited chunk {chunk_index} for {dest_path} "
                f"from {peer_conn.peer_id}"
            )
            return

        logger.debug(f"Received chunk {chunk_index} for {dest_path}")

        transfer.chunks[chunk_index] = chunk_data
        transfer.expected.discard(chunk_index)
        transfer.last_progress = time.monotonic()

        if transfer.expected:
            logger.debug(
                f"{dest_path}: {len(transfer.expected)} chunks still outstanding"
            )
            return

        await self._apply_transfer(dest_path)

    async def _apply_transfer(self, dest_path: str) -> None:
        """
        Apply a completed chunk transfer atomically.

        The assembled file is built in a temp file inside the internal
        directory, fsync'd, then moved into place with `os.replace`. That
        gives the POSIX all-or-nothing guarantee the previous in-place write
        did not: a crash at any point leaves either the old file or the new
        one, never a half-patched mixture.

        The cost is a full copy of the unchanged portion. That is deliberate:
        the scarce resource is network bandwidth, not local disk bandwidth,
        and the Merkle delta still means only the changed chunks cross the
        wire. `artifacts/fuse_benchmark.md` reports both numbers.

        Args:
            dest_path: Local path to write.
        """
        transfer = self.transfers.get(dest_path)
        if transfer is None:
            return

        absolute_path = self._safe_absolute_path(dest_path)
        temp_dir = self._internal_dir() / "tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_path = temp_dir / f"apply-{uuid.uuid4().hex}"

        try:
            absolute_path.parent.mkdir(parents=True, exist_ok=True)

            # Seed the temp file with the current contents so unchanged chunks
            # -- the ones the Merkle diff correctly decided not to transfer --
            # survive. Without this the untransferred regions become zeroes.
            if absolute_path.exists():
                shutil.copyfile(absolute_path, temp_path)
            else:
                temp_path.touch()

            with open(temp_path, 'r+b') as handle:
                for chunk_index in sorted(transfer.chunks):
                    handle.seek(chunk_index * self.config.chunk_size)
                    handle.write(transfer.chunks[chunk_index])

                # The peer's size is authoritative: a shrink is not visible in
                # the chunk list, only in the file size.
                handle.truncate(transfer.remote_size)

                handle.flush()
                os.fsync(handle.fileno())

            # Verify before publishing. A transfer that assembled into
            # something other than what the peer advertised means a bug or a
            # tampered frame, and publishing it would corrupt the tree.
            assembled = await MerkleTree.from_file(
                str(temp_path), self.config.chunk_size
            )
            if transfer.remote_leaves and assembled.leaf_hashes() != transfer.remote_leaves:
                raise ValueError(
                    f"assembled content does not match peer's Merkle leaves "
                    f"for {dest_path}"
                )

            # Suppress the watchdog echo *before* the file appears, not after.
            self._suppress(dest_path)
            os.replace(temp_path, absolute_path)

            logger.info(
                f"Applied {len(transfer.chunks)} chunks to {dest_path} "
                f"({transfer.remote_size} bytes)"
            )

            self.transfers.pop(dest_path, None)

            assembled.file_path = str(absolute_path)
            await self._process_local_file(
                dest_path,
                notify=False,
                local_event=False,
                remote_clock=transfer.remote_clock,
                known_tree=assembled,
            )

        except Exception as e:
            logger.error(f"Error applying transfer to {dest_path}: {e}",
                         exc_info=True)
            self.transfers.pop(dest_path, None)
        finally:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

    # ------------------------------------------------------------------
    # Deletion and tombstones
    # ------------------------------------------------------------------

    async def _tombstone_wins(self, file_path: str, remote_vclock: VectorClock,
                              peer_conn: PeerConnection) -> bool:
        """
        Decide whether a tombstone we hold out-ranks a peer's advertisement.

        Per docs/FUSE_DESIGN.md section 5:
          * tombstone AFTER the remote clock -> the peer missed our delete;
            re-broadcast the tombstone so it deletes too.
          * tombstone BEFORE -> the file was legitimately recreated; drop the
            tombstone and accept the file.
          * CONCURRENT (deleted here, edited there) -> the edit wins and the
            file is resurrected. An unwanted resurrection is visible and
            trivially undone; a silently lost edit is neither.

        Args:
            file_path: Path being advertised.
            remote_vclock: The peer's clock for it.
            peer_conn: Connection to the peer.

        Returns:
            True if the advertisement was handled and should be ignored.
        """
        if self.catalog is None:
            return False

        tombstone = await self.catalog.get_tombstone(file_path, self.config.node_id)
        if tombstone is None:
            return False

        relation = tombstone.clock.compare(remote_vclock)

        if relation == ClockRelation.AFTER:
            logger.info(
                f"Tombstone for {file_path} is newer than {peer_conn.peer_id}'s "
                f"version; re-broadcasting the delete"
            )
            await self.network.send_to_peer(peer_conn.peer_id, Message(
                msg_type=MessageType.FILE_DELETE,
                payload={
                    'file_path': file_path,
                    'vector_clock': tombstone.clock.to_dict(),
                },
                node_id=self.config.node_id,
            ))
            return True

        logger.info(
            f"Accepting {file_path} from {peer_conn.peer_id} over our tombstone "
            f"(relation={relation.value})"
        )
        return False

    async def _handle_file_delete(self, message: Message,
                                  peer_conn: PeerConnection) -> None:
        """Handle a file deletion notification."""
        file_path = self._received_path(message)
        remote_vclock = VectorClock.from_dict(message.payload['vector_clock'])

        logger.info(f"Received delete notification for {file_path}")

        async with self._sync_lock:
            local_metadata = self.file_metadata.get(file_path)

            if not local_metadata:
                # Record the tombstone anyway. We may not have the file now,
                # but without the tombstone a third peer could re-advertise it
                # to us later and we would happily accept it back.
                if self.catalog is not None:
                    await self.catalog.delete_file(file_path, remote_vclock)
                logger.debug(f"File {file_path} already absent; tombstone recorded")
                return

            relation = local_metadata.vector_clock.compare(remote_vclock)

            if relation == ClockRelation.AFTER:
                logger.info(
                    f"Ignoring stale delete for {file_path}: our version is newer"
                )
                return

            if relation == ClockRelation.CONCURRENT:
                # Deleted there, edited here. The edit wins; re-advertise so
                # the deleting peer gets our version back.
                logger.info(
                    f"Concurrent delete/edit on {file_path}: keeping our edit"
                )
                local_metadata.vector_clock.merge(remote_vclock)
                await self._send_merkle_tree(file_path, local_metadata, peer_conn)
                return

            absolute_path = self._safe_absolute_path(file_path)
            try:
                merged = local_metadata.vector_clock
                merged.merge(remote_vclock)

                self._suppress(file_path)
                absolute_path.unlink(missing_ok=True)

                if self.catalog is not None:
                    await self.catalog.delete_file(file_path, merged)

                del self.file_metadata[file_path]
                logger.info(f"Deleted local file {file_path}")
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")

    async def gc_tombstones(self) -> List[str]:
        """
        Collect expired tombstones.

        Returns:
            Paths whose tombstones were removed.
        """
        if self.catalog is None:
            return []
        collected = await self.catalog.gc_tombstones()
        if collected:
            logger.info(f"Collected {len(collected)} expired tombstones")
        return collected

    # ------------------------------------------------------------------
    # Stalled transfers
    # ------------------------------------------------------------------

    async def on_connection_lost(self, peer_id: str) -> None:
        """
        Restart transfers that were being served over a connection that died.

        A chunk stream only completes when its last chunk arrives, so a socket
        that dies mid-stream strands the transfer permanently: no future
        message will ever satisfy it, and the file simply never appears.

        The stall reaper eventually catches this, but "eventually" is 60s, and
        here we already know the exact moment the stream became impossible. A
        peer pair briefly holds two connections (both sides dial), one of them
        loses the duplicate tie-break and closes, and if a transfer happened to
        be running over the loser it would hang for the full timeout -- which
        is what made a 4MB file intermittently fail to arrive while a 1MB file
        always made it, the smaller one simply finishing before the tie-break
        could fire.

        Recovery restarts from the Merkle tree rather than re-requesting the
        outstanding chunk indices, because by now the peer's file may have
        changed and those indices may no longer mean what they did.

        Args:
            peer_id: Node whose connection ended.
        """
        await self._resend_orphaned_tree_requests(peer_id)

        affected = [
            transfer for transfer in self.transfers.values()
            if transfer.peer_id == peer_id and transfer.expected
        ]
        if not affected:
            return

        # If the peer is still reachable -- a displaced duplicate, or it has
        # already reconnected -- re-ask right away. Otherwise drop the partial
        # state and let the next connection or the reaper pick it up.
        peer_conn = self.network.connections.get(peer_id)

        for transfer in affected:
            self.transfers.pop(transfer.dest_path, None)
            logger.info(
                f"Connection to {peer_id} ended with {len(transfer.expected)} "
                f"chunks outstanding for {transfer.dest_path}; "
                f"{'re-requesting' if peer_conn else 'will retry on reconnect'}"
            )
            if peer_conn is None:
                continue
            try:
                await self._request_merkle_tree(
                    transfer.source_path, peer_conn,
                    dest_path=transfer.dest_path,
                )
            except Exception as exc:
                logger.warning(
                    f"Could not re-request {transfer.dest_path} from "
                    f"{peer_id}: {exc}"
                )

    async def _resend_orphaned_tree_requests(self, peer_id: str) -> None:
        """
        Re-send tree requests whose connection to `peer_id` is gone.

        Only requests sent on a connection other than the live one are
        re-sent; one already on the live connection is still in flight. With
        no live connection the record stays, and the reaper retries it once
        the peer is back.
        """
        current = self._live_connection(peer_id)
        if current is None:
            return
        orphaned = [
            (dest, req) for dest, req in self._awaiting_trees.items()
            if req.peer_id == peer_id and req.conn is not current
        ]
        for dest, req in orphaned:
            logger.info(
                f"Tree request for {dest} was sent on a connection to "
                f"{peer_id} that has closed; re-sending"
            )
            try:
                await self._request_merkle_tree(
                    req.source_path, current, dest_path=dest,
                )
            except Exception as exc:
                logger.warning(f"Could not re-send the request for {dest}: {exc}")

    async def reap_stalled_transfers(
            self, timeout: float = TRANSFER_STALL_TIMEOUT) -> List[str]:
        """
        Abandon and retry transfers that stopped receiving chunks.

        A transfer only completes when its last chunk arrives, so anything
        that kills the sender mid-stream -- a dropped connection, a peer
        restart, a sender that hit an unreadable file halfway through the set
        -- leaves a `PendingTransfer` that no future message will ever
        satisfy. The file then never converges, silently, and the only
        symptom is that it is missing.

        This was not hypothetical: a duplicate-connection race used to close
        the socket mid-stream, and a 4MB file simply never arrived while a
        1MB file did, because the smaller one finished before the race could
        fire. The race is fixed in `NetworkManager._register`, but any real
        network provides its own reasons for a stream to stop, so the retry
        exists independently of that fix.

        Recovery restarts the exchange from the Merkle tree rather than
        re-requesting the missing chunk indices: by the time we notice, the
        peer's file may have changed again, and the indices we are missing
        may no longer mean what they meant when we asked.

        Args:
            timeout: Seconds without a chunk before a transfer is considered
                dead.

        Returns:
            Destination paths that were abandoned.
        """
        now = time.monotonic()

        # Tree requests that never got a reply (lost with their connection, or
        # the peer was away). Re-ask a bounded number of times.
        for dest, req in list(self._awaiting_trees.items()):
            if now - req.sent_at <= timeout:
                continue
            if req.attempts >= MAX_TREE_REQUEST_ATTEMPTS:
                logger.warning(
                    f"No reply to the tree request for {dest} from "
                    f"{req.peer_id} after {req.attempts} attempts; giving up"
                )
                self._awaiting_trees.pop(dest, None)
                continue
            conn = self._live_connection(req.peer_id)
            if conn is None:
                continue
            try:
                await self._request_merkle_tree(req.source_path, conn, dest_path=dest)
            except Exception as exc:
                logger.warning(f"Could not re-send the request for {dest}: {exc}")

        stalled = [
            transfer for transfer in self.transfers.values()
            if transfer.expected and now - transfer.last_progress > timeout
        ]

        abandoned: List[str] = []
        for transfer in stalled:
            waited = now - transfer.last_progress
            logger.warning(
                f"Transfer of {transfer.dest_path} stalled: "
                f"{len(transfer.expected)} of "
                f"{len(transfer.expected) + len(transfer.chunks)} chunks "
                f"outstanding after {waited:.1f}s. Abandoning and retrying."
            )
            # Drop the partial data. It is never written anywhere, so there
            # is nothing to clean up on disk -- that is the whole point of
            # assembling into a temp file and publishing with os.replace.
            self.transfers.pop(transfer.dest_path, None)
            abandoned.append(transfer.dest_path)

            peer_conn = self.network.connections.get(transfer.peer_id)
            if peer_conn is None:
                logger.info(
                    f"No connection to {transfer.peer_id}; {transfer.dest_path} "
                    f"will be retried when the peer returns"
                )
                continue

            try:
                await self._request_merkle_tree(
                    transfer.source_path, peer_conn,
                    dest_path=transfer.dest_path,
                )
            except Exception as exc:
                logger.warning(
                    f"Could not re-request {transfer.dest_path} from "
                    f"{transfer.peer_id}: {exc}"
                )

        return abandoned

    # ------------------------------------------------------------------
    # Rename
    # ------------------------------------------------------------------

    async def _handle_file_rename(self, message: Message,
                                  peer_conn: PeerConnection) -> None:
        """
        Handle a FILE_RENAME from a peer (protocol >= 1.1).

        Moving the local file is O(1) and ships no bytes, versus re-fetching
        the entire file under a new name.
        """
        old_path = self._received_path(message, 'old_path')
        new_path = self._received_path(message, 'new_path')
        remote_vclock = VectorClock.from_dict(message.payload['vector_clock'])

        logger.info(f"Received rename {old_path} -> {new_path} from {peer_conn.peer_id}")

        async with self._sync_lock:
            metadata = self.file_metadata.get(old_path)
            if metadata is None:
                # We do not hold the source. Ask for the destination outright.
                logger.info(f"Rename source {old_path} unknown; requesting {new_path}")
                await self._request_merkle_tree(new_path, peer_conn)
                return

            source = self._safe_absolute_path(old_path)
            target = self._safe_absolute_path(new_path)

            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                self._suppress(old_path)
                self._suppress(new_path)
                os.replace(source, target)
            except OSError as e:
                logger.error(f"Rename {old_path} -> {new_path} failed: {e}")
                return

            del self.file_metadata[old_path]
            metadata.file_path = new_path
            metadata.merkle_tree.file_path = str(target)
            metadata.vector_clock.merge(remote_vclock)
            self.file_metadata[new_path] = metadata

            if self.catalog is not None:
                await self.catalog.rename(old_path, new_path, metadata.vector_clock)

    # ------------------------------------------------------------------
    # Conflicts
    # ------------------------------------------------------------------

    async def _handle_conflict(self, file_path: str, local_metadata: FileMetadata,
                               remote_vclock: VectorClock,
                               peer_conn: PeerConnection) -> None:
        """
        Resolve a concurrent edit by producing a deterministic conflict sibling.

        Both peers must reach the *same* verdict with no further communication
        or they diverge permanently. Last-write-wins cannot do that: it depends
        on wall-clock timestamps from unsynchronised machines, so two peers can
        disagree about which write was "last" and each keep a different winner.

        The rule is therefore purely structural: **the lexicographically lower
        node_id keeps the contested path.** Arbitrary, but arbitrary-and-
        identical is exactly what convergence requires.

        Args:
            file_path: The contested path.
            local_metadata: Our version's metadata.
            remote_vclock: The peer's clock for its version.
            peer_conn: Connection to the peer.
        """
        local_node = self.config.node_id
        remote_node = peer_conn.peer_id

        if is_conflict_sibling(file_path):
            # A conflict on a conflict file would recurse without bound. Merge
            # clocks and let the ordinary newest-wins path settle it.
            logger.warning(
                f"Concurrent edit on conflict sibling {file_path}; "
                f"merging clocks without generating a further sibling"
            )
            local_metadata.vector_clock.merge(remote_vclock)
            await self._persist(file_path, local_metadata)
            return

        winner = min(local_node, remote_node)
        loser = max(local_node, remote_node)

        if loser == local_node:
            loser_counter = local_metadata.vector_clock.clock.get(local_node, 0)
        else:
            loser_counter = remote_vclock.clock.get(remote_node, 0)

        sibling = conflict_sibling_path(file_path, loser, loser_counter)

        logger.warning(
            f"Conflict on {file_path}: {winner} keeps the path, "
            f"{loser}'s version goes to {sibling}"
        )

        if winner == local_node:
            # We keep f.txt. Fetch the peer's version into the sibling so both
            # peers end up with identical directory contents.
            await self._request_merkle_tree(file_path, peer_conn, dest_path=sibling)
        else:
            # We lose the path. Move our version aside first -- it must not be
            # destroyed by the incoming write -- then accept the peer's.
            try:
                source = self._safe_absolute_path(file_path)
                target = self._safe_absolute_path(sibling)
                if source.exists():
                    self._suppress(sibling)
                    shutil.copy2(source, target)
                    logger.info(f"Preserved our version as {sibling}")
                    # The non-locking variant: our caller
                    # (_handle_sync_request) already holds the sync lock, and
                    # asyncio.Lock is not reentrant.
                    await self._process_local_file_locked(
                        sibling, notify=False, local_event=False,
                        remote_clock=local_metadata.vector_clock.copy(),
                    )
            except Exception as e:
                logger.error(f"Failed to preserve conflicting version: {e}")

            await self._request_merkle_tree(file_path, peer_conn)

        # Merge clocks. This must be a plain element-wise maximum: both peers
        # resolve the same conflict independently, and if each ticked its own
        # counter here the two results would again be CONCURRENT and the
        # conflict would never settle.
        local_metadata.vector_clock.merge(remote_vclock)
        await self._persist(file_path, local_metadata)

    async def _persist(self, path: str, metadata: FileMetadata) -> None:
        """
        Write current metadata for a path to the catalog.

        Args:
            path: Relative path.
            metadata: Metadata to persist.
        """
        if self.catalog is None:
            return

        absolute = Path(self._get_absolute_path(path))
        if not absolute.exists():
            return

        stat = absolute.stat()
        await self.catalog.record_file(
            path,
            size=stat.st_size,
            mtime=stat.st_mtime,
            mode=stat.st_mode,
            chunk_hashes=metadata.merkle_tree.leaf_hashes(),
            root_hash=metadata.merkle_tree.root.hash,
            clock=metadata.vector_clock,
            op="clock",
        )
