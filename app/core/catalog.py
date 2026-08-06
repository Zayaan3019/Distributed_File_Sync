"""
Persistent Metadata Catalog (SQLite).

The sync engine originally held all file metadata -- Merkle trees, vector
clocks, chunk hashes -- in memory only. That has two consequences the FUSE
mode cannot live with:

  * A restart re-hashes every file in the tree. Measured cost of building a
    Merkle tree for a single 100MB file is ~8.5s, so this does not scale.
  * All causal history is lost on restart. Vector clocks start empty, so a
    restarted node cannot tell "the peer is newer" from "we edited
    concurrently", and every restart looks like a fresh node.

This module adds durable metadata backed by SQLite via aiosqlite, using raw
SQL (no ORM, per the project's constraints). It is additive: the existing
watchdog daemon mode does not use it and is unaffected.

Design notes are in docs/FUSE_DESIGN.md section 9.

Concurrency contract (see FUSE_DESIGN.md section 7): the catalog is owned by
the asyncio event loop. WAL mode permits concurrent readers alongside a single
writer, so the FUSE thread may open its own separate read-only connection, but
connection objects are never shared across threads.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Sequence, Tuple

import aiosqlite

from ..algorithms.vector_clock import VectorClock

# Inode reserved for the filesystem root. Required by FUSE, which always
# addresses the root as inode 1.
ROOT_INODE = 1

# First inode handed out to a real file. 1 is the root; 2 onwards are files.
FIRST_FILE_INODE = 2

# Default tombstone retention. A tombstone must outlive the longest expected
# peer partition, or a peer that was offline during the delete will
# re-advertise the file and resurrect it.
DEFAULT_TOMBSTONE_TTL_DAYS = 30.0

# Unconditional upper bound on tombstone retention. Without this, a single
# permanently dead peer that never acknowledges the delete pins every
# tombstone forever and the catalog grows without limit. The tradeoff is
# stated plainly in docs/FUSE_DESIGN.md section 5: a peer offline for longer
# than this WILL resurrect files deleted during its absence.
DEFAULT_TOMBSTONE_HARD_TTL_DAYS = 180.0


SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    path        TEXT    PRIMARY KEY,
    parent      TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    ino         INTEGER NOT NULL,
    size        INTEGER NOT NULL DEFAULT 0,
    mtime       REAL    NOT NULL DEFAULT 0,
    mode        INTEGER NOT NULL DEFAULT 0,
    chunk_count INTEGER NOT NULL DEFAULT 0,
    root_hash   TEXT,
    is_dir      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chunks (
    path TEXT    NOT NULL,
    idx  INTEGER NOT NULL,
    hash TEXT    NOT NULL,
    PRIMARY KEY (path, idx)
);

CREATE TABLE IF NOT EXISTS clocks (
    path    TEXT    NOT NULL,
    peer_id TEXT    NOT NULL,
    counter INTEGER NOT NULL,
    PRIMARY KEY (path, peer_id)
);

CREATE TABLE IF NOT EXISTS tombstones (
    path       TEXT PRIMARY KEY,
    clock      TEXT NOT NULL,
    deleted_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS inodes (
    ino  INTEGER PRIMARY KEY,
    path TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS sync_journal (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    op      TEXT    NOT NULL,
    path    TEXT    NOT NULL,
    clock   TEXT    NOT NULL,
    ts      REAL    NOT NULL,
    applied INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- readdir needs "every child of directory X". Without a stored parent column
-- that is a LIKE 'dir/%' scan of the whole table on every readdir, and
-- readdir is on the hot path. This makes it O(children).
CREATE INDEX IF NOT EXISTS idx_files_parent ON files (parent, name);

-- Tombstone GC scans by age on a timer; without this it is a full table scan.
CREATE INDEX IF NOT EXISTS idx_tombstones_deleted_at ON tombstones (deleted_at);

-- Crash recovery replays unapplied journal entries in insertion order.
CREATE INDEX IF NOT EXISTS idx_journal_applied ON sync_journal (applied, id);

-- Note: no separate index on clocks(path). It is already the leftmost column
-- of the (path, peer_id) primary key, so SQLite uses that index for
-- path-only lookups. A standalone index would cost write throughput and buy
-- nothing.
"""


def normalize_path(path: str) -> str:
    """
    Convert a path to the catalog's canonical form.

    Paths are stored as POSIX-style relative paths with forward slashes. The
    daemon derives relative paths with pathlib, which yields backslashes on
    Windows and forward slashes on Linux, so the same file would otherwise key
    differently on different peers and never converge.

    Args:
        path: A relative path, in either separator style.

    Returns:
        Canonical relative path using forward slashes, no leading separator.
    """
    text = str(path).replace("\\", "/").strip("/")
    if not text or text == ".":
        return ""

    # Collapse '.' segments without resolving '..' against the real
    # filesystem, which would escape the sync root.
    parts = [part for part in text.split("/") if part not in ("", ".")]
    return "/".join(parts)


def split_path(path: str) -> Tuple[str, str]:
    """
    Split a canonical path into (parent, name).

    Args:
        path: Canonical relative path.

    Returns:
        Tuple of parent path ("" for top level) and final component.
    """
    canonical = normalize_path(path)
    if "/" not in canonical:
        return "", canonical

    parent, _, name = canonical.rpartition("/")
    return parent, name


@dataclass
class FileRecord:
    """A row from the files table."""

    path: str
    parent: str
    name: str
    ino: int
    size: int
    mtime: float
    mode: int
    chunk_count: int
    root_hash: Optional[str]
    is_dir: bool

    @classmethod
    def from_row(cls, row: aiosqlite.Row) -> "FileRecord":
        """Build a record from a database row."""
        return cls(
            path=row["path"],
            parent=row["parent"],
            name=row["name"],
            ino=row["ino"],
            size=row["size"],
            mtime=row["mtime"],
            mode=row["mode"],
            chunk_count=row["chunk_count"],
            root_hash=row["root_hash"],
            is_dir=bool(row["is_dir"]),
        )


@dataclass
class Tombstone:
    """A row from the tombstones table."""

    path: str
    clock: VectorClock
    deleted_at: float


@dataclass
class JournalEntry:
    """A row from the sync_journal table."""

    id: int
    op: str
    path: str
    clock: VectorClock
    ts: float
    applied: bool


@dataclass
class ReconcileReport:
    """
    Outcome of reconciling the catalog against the real directory tree.

    Attributes:
        rehashed: Paths whose (size, mtime) disagreed and were re-hashed.
        unchanged: Paths that matched and were skipped.
        ingested: Paths on disk but absent from the catalog.
        vanished: Paths in the catalog but absent from disk.
        dirs: Directory paths recorded.
    """

    rehashed: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    ingested: List[str] = field(default_factory=list)
    vanished: List[str] = field(default_factory=list)
    dirs: List[str] = field(default_factory=list)

    def summary(self) -> str:
        """One-line summary for logging."""
        return (
            f"{len(self.unchanged)} unchanged, {len(self.rehashed)} re-hashed, "
            f"{len(self.ingested)} ingested, {len(self.vanished)} vanished, "
            f"{len(self.dirs)} dirs"
        )


class Catalog:
    """
    Durable metadata store for the synced tree.

    Every mutation writes the file row, its chunk hashes, its vector clock and
    a journal entry inside a single transaction. A partially applied vector
    clock is corruption -- it would make this node claim causal knowledge it
    does not have -- so clock rows never move independently of the data they
    describe.

    Example:
        >>> catalog = Catalog("/var/lib/dfs/catalog.db")
        >>> await catalog.open()
        >>> await catalog.record_file("notes.txt", size=12, mtime=1.0,
        ...                           mode=0o100644, chunk_hashes=["ab"],
        ...                           root_hash="ab", clock=clock)
        >>> record = await catalog.get_file("notes.txt")
        >>> await catalog.close()
    """

    def __init__(self, db_path: str,
                 tombstone_ttl_days: float = DEFAULT_TOMBSTONE_TTL_DAYS,
                 tombstone_hard_ttl_days: float = DEFAULT_TOMBSTONE_HARD_TTL_DAYS):
        """
        Initialize the catalog.

        Args:
            db_path: Path to the SQLite database file.
            tombstone_ttl_days: Retention before a tombstone may be collected,
                subject to peer acknowledgement.
            tombstone_hard_ttl_days: Retention after which a tombstone is
                collected regardless of acknowledgement.
        """
        self.db_path = str(db_path)
        self.tombstone_ttl_days = tombstone_ttl_days
        self.tombstone_hard_ttl_days = tombstone_hard_ttl_days
        self._db: Optional[aiosqlite.Connection] = None
        self._write_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def open(self) -> None:
        """
        Open the database, applying schema and pragmas.

        Creates the file and schema if absent, which is also the migration
        path for existing daemon-mode users: no catalog is a valid starting
        state, and the first run builds one.
        """
        parent = Path(self.db_path).parent
        if str(parent):
            parent.mkdir(parents=True, exist_ok=True)

        # isolation_level=None gives us manual transaction control: we issue
        # BEGIN IMMEDIATE / COMMIT explicitly so a multi-statement mutation is
        # genuinely atomic. It must be passed to connect() rather than
        # assigned afterwards -- aiosqlite drives the real sqlite3 connection
        # from its own worker thread, and sqlite3 objects may only be touched
        # from the thread that created them.
        self._db = await aiosqlite.connect(self.db_path, isolation_level=None)
        self._db.row_factory = aiosqlite.Row

        # WAL lets the FUSE thread read while the engine writes.
        await self._db.execute("PRAGMA journal_mode=WAL")
        # NORMAL is safe under WAL: a crash can lose the last commits but
        # cannot corrupt the database, and recovery re-derives them from disk.
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.execute("PRAGMA foreign_keys=ON")

        await self._db.executescript(SCHEMA)
        await self._ensure_root_inode()

    async def close(self) -> None:
        """Close the database connection."""
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def __aenter__(self) -> "Catalog":
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    @property
    def db(self) -> aiosqlite.Connection:
        """The open connection, or raise if the catalog is closed."""
        if self._db is None:
            raise RuntimeError("Catalog is not open; call await catalog.open()")
        return self._db

    async def _ensure_root_inode(self) -> None:
        """Reserve inode 1 for the sync root, as FUSE requires."""
        await self.db.execute(
            "INSERT OR IGNORE INTO inodes (ino, path) VALUES (?, ?)",
            (ROOT_INODE, ""),
        )

    # ------------------------------------------------------------------
    # Inode allocation
    # ------------------------------------------------------------------

    async def inode_for_path(self, path: str, allocate: bool = True) -> Optional[int]:
        """
        Return the stable inode number for a path.

        Inode numbers are persisted, so a path keeps its inode across
        remounts. They are never recycled to a different path, and they
        survive a tombstone, so deleting and recreating a file reuses the same
        inode rather than handing a stale number to a different file.

        Args:
            path: Relative path within the sync root.
            allocate: Assign a new inode if the path has none yet.

        Returns:
            The inode number, or None when unknown and allocate is False.
        """
        canonical = normalize_path(path)

        async with self.db.execute(
            "SELECT ino FROM inodes WHERE path = ?", (canonical,)
        ) as cursor:
            row = await cursor.fetchone()

        if row is not None:
            return row["ino"]
        if not allocate:
            return None

        async with self._write_lock:
            # Re-check under the lock: a concurrent caller may have raced us.
            async with self.db.execute(
                "SELECT ino FROM inodes WHERE path = ?", (canonical,)
            ) as cursor:
                row = await cursor.fetchone()
            if row is not None:
                return row["ino"]

            async with self.db.execute("SELECT MAX(ino) AS m FROM inodes") as cursor:
                max_row = await cursor.fetchone()

            next_ino = max(
                FIRST_FILE_INODE,
                (max_row["m"] or ROOT_INODE) + 1,
            )
            await self.db.execute(
                "INSERT INTO inodes (ino, path) VALUES (?, ?)",
                (next_ino, canonical),
            )
            return next_ino

    async def path_for_inode(self, ino: int) -> Optional[str]:
        """
        Resolve an inode number back to its path.

        Args:
            ino: Inode number.

        Returns:
            The canonical path, or None if the inode is unknown.
        """
        async with self.db.execute(
            "SELECT path FROM inodes WHERE ino = ?", (ino,)
        ) as cursor:
            row = await cursor.fetchone()
        return row["path"] if row is not None else None

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    async def get_file(self, path: str) -> Optional[FileRecord]:
        """
        Fetch a file record.

        Args:
            path: Relative path within the sync root.

        Returns:
            The record, or None if absent.
        """
        async with self.db.execute(
            "SELECT * FROM files WHERE path = ?", (normalize_path(path),)
        ) as cursor:
            row = await cursor.fetchone()
        return FileRecord.from_row(row) if row is not None else None

    async def list_children(self, parent: str) -> List[FileRecord]:
        """
        List the direct children of a directory, for readdir.

        Args:
            parent: Canonical parent path ("" for the sync root).

        Returns:
            Child records ordered by name.
        """
        async with self.db.execute(
            "SELECT * FROM files WHERE parent = ? ORDER BY name",
            (normalize_path(parent),),
        ) as cursor:
            rows = await cursor.fetchall()
        return [FileRecord.from_row(row) for row in rows]

    async def all_paths(self) -> List[str]:
        """Return every path in the catalog, directories included."""
        async with self.db.execute("SELECT path FROM files ORDER BY path") as cursor:
            rows = await cursor.fetchall()
        return [row["path"] for row in rows]

    async def get_chunk_hashes(self, path: str) -> List[str]:
        """
        Fetch the ordered chunk hashes for a file.

        Args:
            path: Relative path within the sync root.

        Returns:
            Chunk hashes ordered by chunk index.
        """
        async with self.db.execute(
            "SELECT hash FROM chunks WHERE path = ? ORDER BY idx",
            (normalize_path(path),),
        ) as cursor:
            rows = await cursor.fetchall()
        return [row["hash"] for row in rows]

    async def get_clock(self, path: str, node_id: str) -> Optional[VectorClock]:
        """
        Rebuild a file's vector clock from the catalog.

        Args:
            path: Relative path within the sync root.
            node_id: This node's identifier, used as the clock's owner.

        Returns:
            The clock, or None if the path has no clock entries.
        """
        async with self.db.execute(
            "SELECT peer_id, counter FROM clocks WHERE path = ?",
            (normalize_path(path),),
        ) as cursor:
            rows = await cursor.fetchall()

        if not rows:
            return None

        return VectorClock(node_id, {row["peer_id"]: row["counter"] for row in rows})

    async def get_tombstone(self, path: str, node_id: str = "") -> Optional[Tombstone]:
        """
        Fetch the tombstone for a path, if one exists.

        Args:
            path: Relative path within the sync root.
            node_id: Owner id for the reconstructed clock.

        Returns:
            The tombstone, or None.
        """
        async with self.db.execute(
            "SELECT * FROM tombstones WHERE path = ?", (normalize_path(path),)
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            return None

        return Tombstone(
            path=row["path"],
            clock=VectorClock(node_id, json.loads(row["clock"])),
            deleted_at=row["deleted_at"],
        )

    async def list_tombstones(self, node_id: str = "") -> List[Tombstone]:
        """Return every tombstone currently retained."""
        async with self.db.execute(
            "SELECT * FROM tombstones ORDER BY deleted_at"
        ) as cursor:
            rows = await cursor.fetchall()

        return [
            Tombstone(
                path=row["path"],
                clock=VectorClock(node_id, json.loads(row["clock"])),
                deleted_at=row["deleted_at"],
            )
            for row in rows
        ]

    async def unapplied_journal(self, node_id: str = "") -> List[JournalEntry]:
        """
        Return journal entries not yet marked applied, in order.

        These are the mutations that were committed locally but whose
        replication was not confirmed before shutdown. Replaying them is what
        makes the write-coalescing window safe across a crash.
        """
        async with self.db.execute(
            "SELECT * FROM sync_journal WHERE applied = 0 ORDER BY id"
        ) as cursor:
            rows = await cursor.fetchall()

        return [
            JournalEntry(
                id=row["id"],
                op=row["op"],
                path=row["path"],
                clock=VectorClock(node_id, json.loads(row["clock"])),
                ts=row["ts"],
                applied=bool(row["applied"]),
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    async def record_file(self, path: str, *, size: int, mtime: float, mode: int,
                          chunk_hashes: Sequence[str], root_hash: Optional[str],
                          clock: VectorClock, op: str = "write",
                          is_dir: bool = False) -> int:
        """
        Record a file mutation atomically.

        The file row, its chunk hashes, its vector clock and the journal entry
        are written in one transaction. Splitting them would allow a crash to
        leave a clock that claims knowledge of data the catalog does not hold.

        Args:
            path: Relative path within the sync root.
            size: File size in bytes.
            mtime: Modification time.
            mode: POSIX mode bits.
            chunk_hashes: Ordered per-chunk hashes.
            root_hash: Merkle root hash.
            clock: The file's vector clock after this mutation.
            op: Journal operation label.
            is_dir: True when recording a directory.

        Returns:
            The inode number for the path.
        """
        canonical = normalize_path(path)
        parent, name = split_path(canonical)
        ino = await self.inode_for_path(canonical)

        async with self._write_lock:
            await self.db.execute("BEGIN IMMEDIATE")
            try:
                await self.db.execute(
                    """
                    INSERT INTO files
                        (path, parent, name, ino, size, mtime, mode,
                         chunk_count, root_hash, is_dir)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(path) DO UPDATE SET
                        parent      = excluded.parent,
                        name        = excluded.name,
                        size        = excluded.size,
                        mtime       = excluded.mtime,
                        mode        = excluded.mode,
                        chunk_count = excluded.chunk_count,
                        root_hash   = excluded.root_hash,
                        is_dir      = excluded.is_dir
                    """,
                    (canonical, parent, name, ino, size, mtime, mode,
                     len(chunk_hashes), root_hash, int(is_dir)),
                )

                await self.db.execute(
                    "DELETE FROM chunks WHERE path = ?", (canonical,)
                )
                if chunk_hashes:
                    await self.db.executemany(
                        "INSERT INTO chunks (path, idx, hash) VALUES (?, ?, ?)",
                        [(canonical, idx, h) for idx, h in enumerate(chunk_hashes)],
                    )

                await self._write_clock(canonical, clock)

                # Recreating a path clears any tombstone for it.
                await self.db.execute(
                    "DELETE FROM tombstones WHERE path = ?", (canonical,)
                )

                await self.db.execute(
                    """
                    INSERT INTO sync_journal (op, path, clock, ts, applied)
                    VALUES (?, ?, ?, ?, 0)
                    """,
                    (op, canonical, json.dumps(clock.clock), time.time()),
                )

                await self.db.execute("COMMIT")
            except Exception:
                await self.db.execute("ROLLBACK")
                raise

        return ino

    async def record_directory(self, path: str, *, mode: int, mtime: float,
                               clock: VectorClock) -> int:
        """
        Record a directory. Convenience wrapper around record_file.

        Args:
            path: Relative path within the sync root.
            mode: POSIX mode bits.
            mtime: Modification time.
            clock: Vector clock for the directory.

        Returns:
            The inode number.
        """
        return await self.record_file(
            path, size=0, mtime=mtime, mode=mode, chunk_hashes=(),
            root_hash=None, clock=clock, op="mkdir", is_dir=True,
        )

    async def _write_clock(self, canonical: str, clock: VectorClock) -> None:
        """
        Replace a path's clock rows. Caller must hold an open transaction.

        Args:
            canonical: Canonical path.
            clock: Clock to persist.
        """
        await self.db.execute("DELETE FROM clocks WHERE path = ?", (canonical,))
        if clock.clock:
            await self.db.executemany(
                "INSERT INTO clocks (path, peer_id, counter) VALUES (?, ?, ?)",
                [(canonical, peer, counter) for peer, counter in clock.clock.items()],
            )

    async def delete_file(self, path: str, clock: VectorClock) -> None:
        """
        Delete a path and record a tombstone, atomically.

        Without a tombstone there is nothing to out-vote a stale
        advertisement from a peer that missed the delete, and the file
        resurrects. The inode row is deliberately retained so that recreating
        the path reuses its inode.

        Args:
            path: Relative path within the sync root.
            clock: The file's clock, already advanced for the delete event.
        """
        canonical = normalize_path(path)

        async with self._write_lock:
            await self.db.execute("BEGIN IMMEDIATE")
            try:
                await self.db.execute("DELETE FROM files WHERE path = ?", (canonical,))
                await self.db.execute("DELETE FROM chunks WHERE path = ?", (canonical,))
                await self.db.execute("DELETE FROM clocks WHERE path = ?", (canonical,))

                await self.db.execute(
                    """
                    INSERT INTO tombstones (path, clock, deleted_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(path) DO UPDATE SET
                        clock      = excluded.clock,
                        deleted_at = excluded.deleted_at
                    """,
                    (canonical, json.dumps(clock.clock), time.time()),
                )

                await self.db.execute(
                    """
                    INSERT INTO sync_journal (op, path, clock, ts, applied)
                    VALUES ('unlink', ?, ?, ?, 0)
                    """,
                    (canonical, json.dumps(clock.clock), time.time()),
                )

                await self.db.execute("COMMIT")
            except Exception:
                await self.db.execute("ROLLBACK")
                raise

    async def rename(self, old_path: str, new_path: str, clock: VectorClock) -> None:
        """
        Move a path within the catalog, atomically.

        POSIX requires rename to be atomic: an observer sees the old name or
        the new name, never both and never neither. Within the catalog that
        holds -- this is a single transaction. It does NOT hold across the
        catalog and the backing store together, nor across peers. See
        docs/FUSE_DESIGN.md section 6.

        Args:
            old_path: Existing relative path.
            new_path: Destination relative path.
            clock: Clock for the rename event.
        """
        old_canonical = normalize_path(old_path)
        new_canonical = normalize_path(new_path)
        new_parent, new_name = split_path(new_canonical)

        # Allocate outside the transaction; inode_for_path takes the same lock.
        await self.inode_for_path(old_canonical)

        async with self._write_lock:
            await self.db.execute("BEGIN IMMEDIATE")
            try:
                # A rename onto an existing target replaces it.
                await self.db.execute(
                    "DELETE FROM files WHERE path = ?", (new_canonical,)
                )
                await self.db.execute(
                    "DELETE FROM chunks WHERE path = ?", (new_canonical,)
                )
                await self.db.execute(
                    "DELETE FROM clocks WHERE path = ?", (new_canonical,)
                )
                await self.db.execute(
                    "DELETE FROM inodes WHERE path = ?", (new_canonical,)
                )

                await self.db.execute(
                    "UPDATE files SET path = ?, parent = ?, name = ? WHERE path = ?",
                    (new_canonical, new_parent, new_name, old_canonical),
                )
                await self.db.execute(
                    "UPDATE chunks SET path = ? WHERE path = ?",
                    (new_canonical, old_canonical),
                )
                await self.db.execute(
                    "UPDATE clocks SET path = ? WHERE path = ?",
                    (new_canonical, old_canonical),
                )
                # The inode follows the file, so the local inode number is
                # preserved across a rename.
                await self.db.execute(
                    "UPDATE inodes SET path = ? WHERE path = ?",
                    (new_canonical, old_canonical),
                )

                await self.db.execute(
                    """
                    INSERT INTO sync_journal (op, path, clock, ts, applied)
                    VALUES ('rename', ?, ?, ?, 0)
                    """,
                    (f"{old_canonical}\x00{new_canonical}",
                     json.dumps(clock.clock), time.time()),
                )

                await self.db.execute("COMMIT")
            except Exception:
                await self.db.execute("ROLLBACK")
                raise

    async def mark_journal_applied(self, entry_ids: Sequence[int]) -> None:
        """
        Mark journal entries as replicated.

        Args:
            entry_ids: Journal row ids to mark.
        """
        if not entry_ids:
            return

        async with self._write_lock:
            await self.db.execute("BEGIN IMMEDIATE")
            try:
                await self.db.executemany(
                    "UPDATE sync_journal SET applied = 1 WHERE id = ?",
                    [(entry_id,) for entry_id in entry_ids],
                )
                await self.db.execute("COMMIT")
            except Exception:
                await self.db.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------
    # Tombstone GC
    # ------------------------------------------------------------------

    async def gc_tombstones(self, acked_by_all_peers: Optional[Sequence[str]] = None,
                            now: Optional[float] = None) -> List[str]:
        """
        Collect expired tombstones.

        A tombstone is collected when it is older than the soft TTL and every
        peer has acknowledged it, or when it is older than the hard TTL
        regardless. The hard limit exists because a permanently dead peer
        would otherwise pin every tombstone forever.

        Args:
            acked_by_all_peers: Paths every configured peer has acknowledged.
            now: Current time, injectable for testing.

        Returns:
            Paths whose tombstones were collected.
        """
        current = time.time() if now is None else now
        soft_cutoff = current - self.tombstone_ttl_days * 86400.0
        hard_cutoff = current - self.tombstone_hard_ttl_days * 86400.0
        acked = set(acked_by_all_peers or ())

        async with self.db.execute(
            "SELECT path, deleted_at FROM tombstones WHERE deleted_at < ?",
            (soft_cutoff,),
        ) as cursor:
            rows = await cursor.fetchall()

        collectable = [
            row["path"]
            for row in rows
            if row["path"] in acked or row["deleted_at"] < hard_cutoff
        ]

        if not collectable:
            return []

        async with self._write_lock:
            await self.db.execute("BEGIN IMMEDIATE")
            try:
                await self.db.executemany(
                    "DELETE FROM tombstones WHERE path = ?",
                    [(path,) for path in collectable],
                )
                await self.db.execute("COMMIT")
            except Exception:
                await self.db.execute("ROLLBACK")
                raise

        return collectable

    # ------------------------------------------------------------------
    # Crash recovery
    # ------------------------------------------------------------------

    async def reconcile(self, root_dir: str, node_id: str,
                        hasher=None, ignore=None) -> ReconcileReport:
        """
        Reconcile the catalog against the real directory tree.

        Re-hashes ONLY files whose (size, mtime) disagree with the catalog.
        That is the whole point of persisting metadata: a remount of a large
        tree should cost a stat per file, not a full re-hash.

        Args:
            root_dir: Absolute path of the sync root.
            node_id: This node's identifier.
            hasher: Optional async callable taking an absolute path and
                returning (root_hash, chunk_hashes). Defaults to building a
                Merkle tree.
            ignore: Optional callable taking a relative path, returning True
                to skip it.

        Returns:
            A ReconcileReport describing what changed.
        """
        if hasher is None:
            hasher = _default_hasher

        report = ReconcileReport()
        root = Path(root_dir)
        seen: set = set()

        for entry in sorted(root.rglob("*")):
            relative = normalize_path(entry.relative_to(root).as_posix())
            if not relative or (ignore is not None and ignore(relative)):
                continue

            if entry.is_dir():
                seen.add(relative)
                report.dirs.append(relative)
                existing = await self.get_file(relative)
                if existing is None or not existing.is_dir:
                    stat = entry.stat()
                    clock = await self.get_clock(relative, node_id) or VectorClock(node_id)
                    await self.record_directory(
                        relative, mode=stat.st_mode, mtime=stat.st_mtime, clock=clock
                    )
                continue

            if not entry.is_file():
                continue

            seen.add(relative)
            stat = entry.stat()
            existing = await self.get_file(relative)

            if (existing is not None
                    and existing.size == stat.st_size
                    and existing.mtime == stat.st_mtime):
                report.unchanged.append(relative)
                continue

            root_hash, chunk_hashes = await hasher(str(entry))
            clock = await self.get_clock(relative, node_id) or VectorClock(node_id)
            clock.increment(node_id)

            await self.record_file(
                relative,
                size=stat.st_size,
                mtime=stat.st_mtime,
                mode=stat.st_mode,
                chunk_hashes=chunk_hashes,
                root_hash=root_hash,
                clock=clock,
                op="recover",
            )

            if existing is None:
                report.ingested.append(relative)
            else:
                report.rehashed.append(relative)

        # Anything the catalog knows about but disk does not was deleted while
        # we were down. Tombstone it so the delete still propagates.
        for path in await self.all_paths():
            if path in seen:
                continue
            report.vanished.append(path)
            clock = await self.get_clock(path, node_id) or VectorClock(node_id)
            clock.increment(node_id)
            await self.delete_file(path, clock)

        return report


async def _default_hasher(absolute_path: str) -> Tuple[str, List[str]]:
    """
    Build a Merkle tree and return (root_hash, ordered chunk hashes).

    Imported lazily so the catalog stays usable without the algorithms
    package, which keeps the recovery tests independent of Merkle changes.

    Args:
        absolute_path: Path to the file to hash.

    Returns:
        Tuple of root hash and the ordered per-chunk hashes.
    """
    from ..algorithms.merkle import MerkleTree

    tree = await MerkleTree.from_file(absolute_path)
    leaves = MerkleTree._collect_leaves(tree.root)

    by_index: Dict[int, str] = {}
    for leaf in leaves:
        if leaf.chunk_index is not None:
            by_index[leaf.chunk_index] = leaf.hash

    ordered = [by_index[idx] for idx in sorted(by_index)]
    return tree.root.hash, ordered
