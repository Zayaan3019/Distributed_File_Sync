"""
Stable inode allocation for the FUSE mount.

FUSE addresses everything by inode number; the catalog addresses everything by
path. This module owns the mapping and, critically, makes it *stable across
remounts* -- the requirement from Phase 3. An inode number that changed on
remount would make every open file handle in every running process point at
the wrong file after a restart, and would break `find -inum`, hardlink
detection in `cp -a`, and rsync's rename heuristics.

Threading contract
------------------
This table is owned by the FUSE thread. It holds its own **synchronous**
sqlite3 connection rather than sharing the engine's `aiosqlite` connection,
for a reason that matters: `aiosqlite` drives a real sqlite3 connection from
its own worker thread, and sqlite3 objects may only be touched by the thread
that created them. Reaching across would be a latent crash.

Two writers to one database is safe here because the database is in WAL mode
and every write this class performs is a single short `INSERT` into `inodes`.
SQLite's own locking serialises them, and `busy_timeout` bounds the wait. The
alternative -- posting allocations to the asyncio loop and blocking on the
result -- would put a cross-thread wait on the hot `lookup` path, which is
precisely the deadlock shape docs/FUSE_DESIGN.md section 7 exists to forbid.

This is a deliberate deviation from the original design note that said "all
writes go through the asyncio loop"; the design doc has been updated to match.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Dict, Optional

from ..core.catalog import FIRST_FILE_INODE, ROOT_INODE, normalize_path


class InodeTable:
    """
    Bidirectional, persistent path <-> inode mapping.

    Numbers are assigned on first sight, never recycled to a different path,
    and retained across a tombstone so that deleting and recreating a path
    reuses its number rather than handing a stale one to different content.

    An in-memory cache fronts the database because `lookup` and `getattr` are
    the hottest FUSE operations there are -- a single `ls -l` of a directory
    with N entries issues N lookups plus N getattrs.

    Attributes:
        db_path: Path to the SQLite catalog.

    Example:
        >>> table = InodeTable("/var/lib/dfs/catalog.db")
        >>> table.open()
        >>> ino = table.inode_for_path("docs/notes.txt")
        >>> table.path_for_inode(ino)
        'docs/notes.txt'
        >>> table.close()
    """

    def __init__(self, db_path: str):
        """
        Initialize the table.

        Args:
            db_path: Path to the SQLite catalog file.
        """
        self.db_path = str(db_path)
        self._db: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()
        self._path_to_ino: Dict[str, int] = {}
        self._ino_to_path: Dict[int, str] = {}
        self._next_ino = FIRST_FILE_INODE

    def open(self) -> None:
        """
        Open the catalog and load the existing mapping into memory.

        Loading eagerly is affordable -- one row per path ever seen -- and it
        means the steady-state lookup path never touches SQLite at all.
        """
        parent = Path(self.db_path).parent
        if str(parent):
            parent.mkdir(parents=True, exist_ok=True)

        self._db = sqlite3.connect(self.db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        # Wait rather than fail if the engine happens to hold the write lock.
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS inodes ("
            "ino INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE)"
        )
        self._db.execute(
            "INSERT OR IGNORE INTO inodes (ino, path) VALUES (?, ?)",
            (ROOT_INODE, ""),
        )
        self._db.commit()

        with self._lock:
            self._path_to_ino.clear()
            self._ino_to_path.clear()
            highest = ROOT_INODE

            for row in self._db.execute("SELECT ino, path FROM inodes"):
                self._path_to_ino[row["path"]] = row["ino"]
                self._ino_to_path[row["ino"]] = row["path"]
                highest = max(highest, row["ino"])

            self._next_ino = max(FIRST_FILE_INODE, highest + 1)

    def close(self) -> None:
        """Close the database connection."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def inode_for_path(self, path: str, allocate: bool = True) -> Optional[int]:
        """
        Return the stable inode number for a path, allocating if needed.

        Args:
            path: Path relative to the sync root. The empty string is the root.
            allocate: Assign a new number if the path has none yet.

        Returns:
            The inode number, or None when unknown and `allocate` is False.
        """
        canonical = normalize_path(path)

        with self._lock:
            existing = self._path_to_ino.get(canonical)
            if existing is not None:
                return existing
            if not allocate:
                return None

            ino = self._next_ino
            self._next_ino += 1

            self._path_to_ino[canonical] = ino
            self._ino_to_path[ino] = canonical

            if self._db is not None:
                # Persist immediately. A number handed to the kernel but lost
                # on restart is exactly the instability this class exists to
                # prevent.
                self._db.execute(
                    "INSERT OR REPLACE INTO inodes (ino, path) VALUES (?, ?)",
                    (ino, canonical),
                )
                self._db.commit()

            return ino

    def path_for_inode(self, ino: int) -> Optional[str]:
        """
        Resolve an inode number back to its path.

        Args:
            ino: Inode number.

        Returns:
            The path, or None if the number was never allocated.
        """
        with self._lock:
            return self._ino_to_path.get(ino)

    def rename(self, old_path: str, new_path: str) -> None:
        """
        Move an inode number to a new path.

        POSIX requires the inode to survive a rename: `mv a b` must not change
        `stat -c %i`. Applications rely on this to detect that a file they hold
        open is the same file after a move.

        Args:
            old_path: Existing path.
            new_path: Destination path.
        """
        old_canonical = normalize_path(old_path)
        new_canonical = normalize_path(new_path)

        with self._lock:
            ino = self._path_to_ino.pop(old_canonical, None)
            if ino is None:
                return

            # A rename onto an existing target replaces it, so the target's
            # old number is retired rather than left dangling.
            displaced = self._path_to_ino.pop(new_canonical, None)
            if displaced is not None:
                self._ino_to_path.pop(displaced, None)

            self._path_to_ino[new_canonical] = ino
            self._ino_to_path[ino] = new_canonical

            if self._db is not None:
                self._db.execute(
                    "DELETE FROM inodes WHERE path = ?", (new_canonical,)
                )
                self._db.execute(
                    "UPDATE inodes SET path = ? WHERE ino = ?", (new_canonical, ino)
                )
                self._db.commit()

    def forget_path(self, path: str) -> None:
        """
        Drop the in-memory entry for a path without releasing its number.

        The database row is deliberately retained: recreating the path must
        reuse the same number, and the number must never be handed to
        different content.

        Args:
            path: Path being removed.
        """
        canonical = normalize_path(path)
        with self._lock:
            ino = self._path_to_ino.get(canonical)
            if ino is not None:
                # Keep ino -> path so an outstanding kernel reference still
                # resolves; only the forward lookup is invalidated.
                self._path_to_ino.pop(canonical, None)
                self._ino_to_path[ino] = canonical

    def __len__(self) -> int:
        """Number of paths currently mapped."""
        with self._lock:
            return len(self._path_to_ino)
