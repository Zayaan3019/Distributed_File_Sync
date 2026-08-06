"""
pyfuse3 operations for the synced tree.

This is Phase 3 (read path) and Phase 4 (write path) of the FUSE work.

The filesystem is a **pass-through over a real backing directory**, not a
synthetic namespace. Reads and writes go to ordinary files with ordinary
`os` calls; what FUSE adds is the hook that turns each mutation into a
Merkle-delta replication event with a vector-clock bump.

That choice is what makes the trio/asyncio boundary tractable. Per
docs/FUSE_DESIGN.md section 7:

* **Rule 1 — the read path never crosses the boundary.** `lookup`, `getattr`,
  `opendir`, `readdir`, `open`, `read`, `release` and `statfs` are served
  entirely from the inode table, the attribute cache, and direct I/O on the
  backing store. They never await the asyncio engine. A read handler that
  blocked on a network round trip would leave an application in
  uninterruptible sleep whenever a peer died.
* **Rule 2 — mutations post, they do not wait.** Write-path handlers apply the
  change to the backing store synchronously (so read-your-writes holds), then
  hand replication to asyncio via `run_coroutine_threadsafe` **without
  blocking on the future**. `fsync` is the sole exception, and even there it
  waits only on *local* durability, never on peer acknowledgement.
* **Rule 3 — the engine never performs I/O through the mount.** It operates on
  the backing directory only. Violating this deadlocks instantly and
  unrecoverably.

Write coalescing (design section 2): a handle accumulates dirty 4KB chunk
indices and flushes replication on `release`, on `fsync`, or once it holds
`DIRTY_CHUNK_WATERMARK` dirty chunks. Only those chunks are re-hashed --
re-hashing a whole 100MB file per write measured 8.5s and would make the
mount unusable.
"""

from __future__ import annotations

import errno
import logging
import os
import stat as stat_module
import threading
import time
from pathlib import Path
from typing import Dict, Optional, Set

import pyfuse3

from ..core.security import PathTraversalError, sanitize_relative_path
from .attrcache import AttrCache
from .inodes import InodeTable

logger = logging.getLogger(__name__)

#: Dirty 4KB chunks a handle may accumulate before replication is triggered
#: without waiting for close. 64 chunks is 256KB. Without a watermark, a
#: long-lived writer that never closes -- a log file, a database, vim's swap
#: file -- would never replicate at all.
DIRTY_CHUNK_WATERMARK = 64

#: Attribute and entry timeouts handed to the kernel, in seconds. This is the
#: single biggest performance lever in the whole filesystem: it lets the
#: kernel answer most stat() calls without entering this process. The cost is
#: that remote-originated changes can be up to this stale in the mount, which
#: docs/FUSE_DESIGN.md section 8 states explicitly as an accepted limit.
ATTR_TIMEOUT = 1.0
ENTRY_TIMEOUT = 1.0


class FileHandle:
    """
    State for one open file.

    Attributes:
        fd: Underlying OS file descriptor on the backing store.
        path: Relative path within the sync root.
        dirty_chunks: 4KB chunk indices written through this handle and not
            yet replicated.
        writable: Whether the handle was opened for writing.
        last_write: Monotonic timestamp of the most recent write.
    """

    __slots__ = ("fd", "path", "dirty_chunks", "writable", "last_write")

    def __init__(self, fd: int, path: str, writable: bool):
        """
        Initialize the handle.

        Args:
            fd: Open OS file descriptor.
            path: Relative path within the sync root.
            writable: True if opened for writing.
        """
        self.fd = fd
        self.path = path
        self.dirty_chunks: Set[int] = set()
        self.writable = writable
        self.last_write = 0.0


class FileSyncFs(pyfuse3.Operations):
    """
    A FUSE filesystem exposing the synced tree.

    Attributes:
        backing_root: Directory holding the real files.
        inodes: Stable path <-> inode mapping.
        attr_cache: Inode attribute cache.
        engine_bridge: Callable posting a coroutine onto the asyncio loop.
        chunk_size: Merkle chunk size, for dirty-chunk accounting.
    """

    #: pyfuse3 asks us to declare these; both are genuinely unimplemented and
    #: saying so is more useful than a broken partial implementation. The
    #: pjdfstest failures they cause are enumerated in the benchmark report.
    supports_dot_lookup = True
    enable_writeback_cache = False
    enable_acl = False

    def __init__(self, backing_root: str, inodes: InodeTable,
                 engine_bridge=None, chunk_size: int = 4096,
                 attr_ttl: float = ATTR_TIMEOUT):
        """
        Initialize the filesystem.

        Args:
            backing_root: Absolute path of the backing directory.
            inodes: Inode table, already opened.
            engine_bridge: Callable taking a coroutine and scheduling it on
                the asyncio loop. None disables replication, which is what the
                standalone filesystem tests use.
            chunk_size: Merkle chunk size in bytes.
            attr_ttl: Attribute cache lifetime in seconds.
        """
        super().__init__()
        self.backing_root = Path(backing_root).resolve()
        self.inodes = inodes
        self.attr_cache = AttrCache(ttl=attr_ttl)
        self.engine_bridge = engine_bridge
        self.chunk_size = chunk_size

        self._handles: Dict[int, FileHandle] = {}
        self._next_fh = 1
        self._handle_lock = threading.RLock()

        # Directory listings are snapshotted at opendir. readdir must not skip
        # or duplicate entries across calls, which a live directory iterator
        # cannot promise if the directory is being modified concurrently.
        self._dir_handles: Dict[int, list] = {}

        self.inodes.inode_for_path("")

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _path_for_inode(self, inode: int) -> str:
        """
        Resolve an inode to its relative path.

        Args:
            inode: Inode number.

        Returns:
            Relative path within the sync root.

        Raises:
            pyfuse3.FUSEError: ENOENT if the inode is unknown.
        """
        if inode == pyfuse3.ROOT_INODE:
            return ""
        path = self.inodes.path_for_inode(inode)
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return path

    def _backing(self, relative_path: str) -> Path:
        """
        Map a relative path to its backing-store location.

        Args:
            relative_path: Path within the sync root.

        Returns:
            Absolute path in the backing store.
        """
        if not relative_path:
            return self.backing_root
        return self.backing_root / relative_path

    def _child_path(self, parent_inode: int, name: bytes) -> str:
        """
        Build the relative path of a named child.

        The name is validated even though it comes from the local kernel
        rather than the network. FUSE names are raw bytes and a caller can
        supply anything that is not NUL or '/', so the same containment rule
        applies here as to peer-supplied paths.

        Args:
            parent_inode: Inode of the containing directory.
            name: Entry name as bytes.

        Returns:
            Relative path of the child.

        Raises:
            pyfuse3.FUSEError: EINVAL for an unusable name.
        """
        parent_path = self._path_for_inode(parent_inode)

        try:
            decoded = os.fsdecode(name)
        except Exception:
            raise pyfuse3.FUSEError(errno.EINVAL)

        if decoded in ("", ".", "..") or "/" in decoded or "\x00" in decoded:
            raise pyfuse3.FUSEError(errno.EINVAL)

        candidate = f"{parent_path}/{decoded}" if parent_path else decoded

        try:
            return sanitize_relative_path(candidate)
        except PathTraversalError:
            raise pyfuse3.FUSEError(errno.EINVAL)

    # ------------------------------------------------------------------
    # Attributes
    # ------------------------------------------------------------------

    def _entry_attributes(self, relative_path: str,
                          use_cache: bool = True) -> pyfuse3.EntryAttributes:
        """
        Build EntryAttributes for a path by stat'ing the backing store.

        Args:
            relative_path: Path within the sync root.
            use_cache: Consult the attribute cache first.

        Returns:
            Populated EntryAttributes.

        Raises:
            pyfuse3.FUSEError: ENOENT if the backing file is gone.
        """
        ino = self.inodes.inode_for_path(relative_path)

        if use_cache:
            cached = self.attr_cache.get(ino)
            if cached is not None:
                return cached

        try:
            info = os.lstat(self._backing(relative_path))
        except FileNotFoundError:
            raise pyfuse3.FUSEError(errno.ENOENT)
        except PermissionError:
            raise pyfuse3.FUSEError(errno.EACCES)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        attrs = pyfuse3.EntryAttributes()
        attrs.st_ino = ino
        attrs.st_mode = info.st_mode
        attrs.st_nlink = info.st_nlink
        attrs.st_uid = info.st_uid
        attrs.st_gid = info.st_gid
        attrs.st_rdev = info.st_rdev
        attrs.st_size = info.st_size
        attrs.st_blksize = self.chunk_size
        attrs.st_blocks = (info.st_size + 511) // 512
        attrs.st_atime_ns = info.st_atime_ns
        attrs.st_mtime_ns = info.st_mtime_ns
        attrs.st_ctime_ns = info.st_ctime_ns
        attrs.generation = 0
        attrs.attr_timeout = ATTR_TIMEOUT
        attrs.entry_timeout = ENTRY_TIMEOUT

        self.attr_cache.put(ino, attrs)
        return attrs

    def _invalidate(self, relative_path: str) -> None:
        """
        Drop cached attributes for a path after a local mutation.

        Args:
            relative_path: Path that changed.
        """
        ino = self.inodes.inode_for_path(relative_path, allocate=False)
        if ino is not None:
            self.attr_cache.invalidate(ino)

    # ------------------------------------------------------------------
    # Replication bridge
    # ------------------------------------------------------------------

    def _post(self, coro) -> None:
        """
        Hand a coroutine to the asyncio engine without waiting for it.

        Not blocking on the result is Rule 2 of the threading design. A FUSE
        handler that waited here would stall every other filesystem operation
        behind whatever the engine happened to be doing, including a network
        round trip to a peer that may be gone.

        Args:
            coro: Coroutine to schedule. Closed and dropped if no bridge is
                configured, so the caller never leaks an un-awaited coroutine.
        """
        if self.engine_bridge is None:
            coro.close()
            return
        try:
            self.engine_bridge(coro)
        except Exception as exc:
            logger.error(f"Failed to post work to sync engine: {exc}")

    def _replicate(self, handle: FileHandle, force: bool = False) -> None:
        """
        Flush a handle's dirty chunks to the replication engine.

        Args:
            handle: The open handle.
            force: Replicate regardless of the watermark (release / fsync).
        """
        if not handle.dirty_chunks:
            return
        if not force and len(handle.dirty_chunks) < DIRTY_CHUNK_WATERMARK:
            return

        dirty = set(handle.dirty_chunks)
        handle.dirty_chunks.clear()
        path = handle.path

        if self.engine_bridge is None:
            return

        engine = getattr(self.engine_bridge, "engine", None)
        if engine is None:
            return

        self._post(engine.apply_local_write(path, dirty))

    # ------------------------------------------------------------------
    # Read path -- never crosses the trio/asyncio boundary
    # ------------------------------------------------------------------

    async def lookup(self, parent_inode: int, name: bytes,
                     ctx=None) -> pyfuse3.EntryAttributes:
        """
        Resolve a name in a directory.

        Args:
            parent_inode: Inode of the containing directory.
            name: Entry name.
            ctx: Request context.

        Returns:
            Attributes of the named entry.
        """
        decoded = os.fsdecode(name)

        # '.' and '..' must resolve without being treated as traversal.
        if decoded == ".":
            return self._entry_attributes(self._path_for_inode(parent_inode))
        if decoded == "..":
            parent_path = self._path_for_inode(parent_inode)
            grandparent = parent_path.rpartition("/")[0] if "/" in parent_path else ""
            return self._entry_attributes(grandparent)

        return self._entry_attributes(self._child_path(parent_inode, name))

    async def getattr(self, inode: int, ctx=None) -> pyfuse3.EntryAttributes:
        """
        Return attributes for an inode.

        This is the hottest handler in the filesystem. It is served from the
        attribute cache when warm and a single `lstat` when cold, and it never
        touches SQLite or the network.

        Args:
            inode: Inode number.
            ctx: Request context.

        Returns:
            Populated EntryAttributes.
        """
        return self._entry_attributes(self._path_for_inode(inode))

    async def opendir(self, inode: int, ctx=None) -> int:
        """
        Open a directory and snapshot its entries.

        Args:
            inode: Directory inode.
            ctx: Request context.

        Returns:
            A directory handle.
        """
        relative_path = self._path_for_inode(inode)
        backing = self._backing(relative_path)

        try:
            names = sorted(os.listdir(backing))
        except FileNotFoundError:
            raise pyfuse3.FUSEError(errno.ENOENT)
        except NotADirectoryError:
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        except PermissionError:
            raise pyfuse3.FUSEError(errno.EACCES)

        # The internal directory holds our own SQLite catalog and scratch
        # files. Exposing it in the mount would let an application corrupt the
        # catalog through the very filesystem the catalog describes.
        if not relative_path:
            names = [n for n in names if n != ".dfs"]

        entries = []
        for name in names:
            child = f"{relative_path}/{name}" if relative_path else name
            entries.append((os.fsencode(name), child))

        with self._handle_lock:
            fh = self._next_fh
            self._next_fh += 1
            self._dir_handles[fh] = entries

        return fh

    async def readdir(self, fh: int, start_id: int, token) -> None:
        """
        Report directory entries from a snapshot.

        Args:
            fh: Directory handle from `opendir`.
            start_id: Index to resume from.
            token: Opaque reply token.
        """
        entries = self._dir_handles.get(fh)
        if entries is None:
            raise pyfuse3.FUSEError(errno.EBADF)

        for index in range(start_id, len(entries)):
            name, child_path = entries[index]
            try:
                attrs = self._entry_attributes(child_path)
            except pyfuse3.FUSEError:
                # Entry vanished between the snapshot and now. Skipping it is
                # explicitly permitted: readdir may omit entries that were
                # removed during the cycle.
                continue

            if not pyfuse3.readdir_reply(token, name, attrs, index + 1):
                return

    async def releasedir(self, fh: int) -> None:
        """
        Release a directory handle.

        Args:
            fh: Directory handle.
        """
        with self._handle_lock:
            self._dir_handles.pop(fh, None)

    async def open(self, inode: int, flags: int, ctx=None) -> pyfuse3.FileInfo:
        """
        Open an existing file.

        Args:
            inode: File inode.
            flags: POSIX open flags.
            ctx: Request context.

        Returns:
            FileInfo carrying the handle.
        """
        relative_path = self._path_for_inode(inode)

        # O_CREAT/O_EXCL/O_TRUNC arrive through create(); stripping them here
        # is what pyfuse3 expects of open().
        flags &= ~(os.O_CREAT | os.O_EXCL)

        try:
            fd = os.open(self._backing(relative_path), flags)
        except FileNotFoundError:
            raise pyfuse3.FUSEError(errno.ENOENT)
        except IsADirectoryError:
            raise pyfuse3.FUSEError(errno.EISDIR)
        except PermissionError:
            raise pyfuse3.FUSEError(errno.EACCES)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        writable = bool(flags & (os.O_WRONLY | os.O_RDWR))
        handle = FileHandle(fd, relative_path, writable)

        with self._handle_lock:
            fh = self._next_fh
            self._next_fh += 1
            self._handles[fh] = handle

        if flags & os.O_TRUNC:
            self._invalidate(relative_path)
            handle.dirty_chunks.add(0)

        info = pyfuse3.FileInfo(fh=fh)
        # Page cache is safe here because every local mutation invalidates
        # explicitly and remote updates invalidate the inode. Direct I/O would
        # cost roughly an order of magnitude on sequential reads.
        info.keep_cache = True
        return info

    async def read(self, fh: int, off: int, size: int) -> bytes:
        """
        Read from an open file.

        Args:
            fh: File handle.
            off: Byte offset.
            size: Maximum bytes to read.

        Returns:
            The bytes read; shorter than `size` at end of file.
        """
        handle = self._handles.get(fh)
        if handle is None:
            raise pyfuse3.FUSEError(errno.EBADF)

        try:
            return os.pread(handle.fd, size, off)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

    async def statfs(self, ctx=None) -> pyfuse3.StatvfsData:
        """
        Report filesystem statistics.

        Numbers are taken from the backing filesystem: the mount's real
        capacity *is* the backing store's capacity, and inventing figures
        would make `df` lie and `cp` misjudge whether a file fits.

        Args:
            ctx: Request context.

        Returns:
            Populated StatvfsData.
        """
        data = pyfuse3.StatvfsData()

        try:
            info = os.statvfs(self.backing_root)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        data.f_bsize = info.f_bsize
        data.f_frsize = info.f_frsize
        data.f_blocks = info.f_blocks
        data.f_bfree = info.f_bfree
        data.f_bavail = info.f_bavail
        data.f_files = info.f_files
        data.f_ffree = info.f_ffree
        data.f_favail = info.f_favail
        data.f_namemax = info.f_namemax
        return data

    async def access(self, inode: int, mode: int, ctx=None) -> bool:
        """
        Check access permission against the backing store.

        Args:
            inode: Inode number.
            mode: Access mode bits.
            ctx: Request context.

        Returns:
            True if access is permitted.
        """
        relative_path = self._path_for_inode(inode)
        return os.access(self._backing(relative_path), mode)

    async def forget(self, inode_list) -> None:
        """
        Drop cached attributes for inodes the kernel has released.

        The inode *number* is deliberately retained in the table -- it must
        stay stable across remounts -- but its cached attributes are not worth
        holding once the kernel has forgotten the inode.

        Args:
            inode_list: Sequence of (inode, lookup_count) pairs.
        """
        for inode, _count in inode_list:
            self.attr_cache.invalidate(inode)

    # ------------------------------------------------------------------
    # Write path -- applies locally, posts replication
    # ------------------------------------------------------------------

    async def create(self, parent_inode: int, name: bytes, mode: int,
                     flags: int, ctx=None):
        """
        Create and open a new file.

        Args:
            parent_inode: Inode of the containing directory.
            name: New entry name.
            mode: Permission bits.
            flags: POSIX open flags.
            ctx: Request context.

        Returns:
            Tuple of (FileInfo, EntryAttributes).
        """
        relative_path = self._child_path(parent_inode, name)
        backing = self._backing(relative_path)

        try:
            fd = os.open(backing, flags | os.O_CREAT | os.O_TRUNC, mode)
        except FileExistsError:
            raise pyfuse3.FUSEError(errno.EEXIST)
        except PermissionError:
            raise pyfuse3.FUSEError(errno.EACCES)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        # A newly created file has no owner recorded yet; match the caller so
        # the file behaves like one made by an ordinary open(O_CREAT).
        if ctx is not None:
            try:
                os.fchown(fd, ctx.uid, ctx.gid)
            except (PermissionError, OSError):
                # Non-root cannot chown; the file already belongs to us.
                pass

        handle = FileHandle(fd, relative_path, writable=True)
        # Mark chunk 0 dirty so an empty create still replicates. Without
        # this, `touch f` inside the mount would never reach a peer.
        handle.dirty_chunks.add(0)

        with self._handle_lock:
            fh = self._next_fh
            self._next_fh += 1
            self._handles[fh] = handle

        self._invalidate(relative_path)
        attrs = self._entry_attributes(relative_path, use_cache=False)

        info = pyfuse3.FileInfo(fh=fh)
        info.keep_cache = True
        return (info, attrs)

    async def write(self, fh: int, off: int, buf: bytes) -> int:
        """
        Write to an open file.

        Handles the awkward cases explicitly:

        * **Partial writes.** `os.pwrite` may write fewer bytes than offered;
          the loop retries until the buffer is drained or the device refuses.
        * **Writes past EOF.** `pwrite` extends the file and the intervening
          region reads back as zeroes, which is exactly POSIX sparse-file
          behaviour. The dirty-chunk set covers the written range only, but
          the sparse hole between the old EOF and `off` is also marked, since
          its content changed from "absent" to "zeroes".
        * **O_APPEND.** The kernel resolves append offsets before the request
          reaches us, so `off` is already the true end of file.

        Args:
            fh: File handle.
            off: Byte offset.
            buf: Data to write.

        Returns:
            Number of bytes written.
        """
        handle = self._handles.get(fh)
        if handle is None:
            raise pyfuse3.FUSEError(errno.EBADF)

        try:
            old_size = os.fstat(handle.fd).st_size
        except OSError:
            old_size = 0

        written = 0
        try:
            while written < len(buf):
                count = os.pwrite(handle.fd, buf[written:], off + written)
                if count <= 0:
                    break
                written += count
        except OSError as exc:
            if written == 0:
                raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        if written:
            first = off // self.chunk_size
            last = (off + written - 1) // self.chunk_size

            # A write past EOF turns the gap into a zero-filled hole, which is
            # new content the peer does not have.
            if off > old_size:
                first = min(first, old_size // self.chunk_size)

            handle.dirty_chunks.update(range(first, last + 1))
            handle.last_write = time.monotonic()
            self._invalidate(handle.path)
            self._replicate(handle)

        return written

    async def flush(self, fh: int) -> None:
        """
        Handle a `close()` on one descriptor.

        `flush` fires per close, `release` only when the last descriptor goes
        away. Replicating here is what makes `cp` and `cat >` propagate
        promptly even when the file stays open elsewhere.

        Args:
            fh: File handle.
        """
        handle = self._handles.get(fh)
        if handle is not None:
            self._replicate(handle, force=True)

    async def fsync(self, fh: int, datasync: bool) -> None:
        """
        Flush a file to local durable storage.

        POSIX requires a result here, so this is the one handler that does
        real work synchronously. It waits on **local** durability only: it says
        nothing about any peer, and it must not, or an unreachable peer would
        hang `fsync` forever.

        Args:
            fh: File handle.
            datasync: Sync data only, skipping metadata where supported.
        """
        handle = self._handles.get(fh)
        if handle is None:
            raise pyfuse3.FUSEError(errno.EBADF)

        try:
            if datasync and hasattr(os, "fdatasync"):
                os.fdatasync(handle.fd)
            else:
                os.fsync(handle.fd)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        self._replicate(handle, force=True)

    async def release(self, fh: int) -> None:
        """
        Close the last descriptor for a file.

        Args:
            fh: File handle.
        """
        with self._handle_lock:
            handle = self._handles.pop(fh, None)

        if handle is None:
            return

        self._replicate(handle, force=True)

        try:
            os.close(handle.fd)
        except OSError:
            pass

        self._invalidate(handle.path)

    async def setattr(self, inode: int, attr, fields, fh: int,
                      ctx=None) -> pyfuse3.EntryAttributes:
        """
        Change file attributes: size, mode, ownership, times.

        Args:
            inode: Inode number.
            attr: EntryAttributes carrying the new values.
            fields: SetattrFields marking which values are meaningful.
            fh: File handle, when the syscall was descriptor-based.
            ctx: Request context.

        Returns:
            The full, updated attributes.
        """
        relative_path = self._path_for_inode(inode)
        backing = self._backing(relative_path)
        target = self._handles[fh].fd if (fh is not None and fh in self._handles) else backing

        try:
            if fields.update_size:
                # truncate() is a content change: it either removes tail
                # chunks or extends the file with a zero-filled hole. Either
                # way peers must learn about it.
                if isinstance(target, int):
                    os.ftruncate(target, attr.st_size)
                else:
                    os.truncate(target, attr.st_size)

            if fields.update_mode:
                os.chmod(backing, stat_module.S_IMODE(attr.st_mode))

            if fields.update_uid or fields.update_gid:
                uid = attr.st_uid if fields.update_uid else -1
                gid = attr.st_gid if fields.update_gid else -1
                os.chown(backing, uid, gid)

            if fields.update_atime or fields.update_mtime:
                current = os.lstat(backing)
                atime = attr.st_atime_ns if fields.update_atime else current.st_atime_ns
                mtime = attr.st_mtime_ns if fields.update_mtime else current.st_mtime_ns
                os.utime(backing, ns=(atime, mtime))

        except FileNotFoundError:
            raise pyfuse3.FUSEError(errno.ENOENT)
        except PermissionError:
            raise pyfuse3.FUSEError(errno.EACCES)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        self._invalidate(relative_path)

        if fields.update_size:
            self._post_full_resync(relative_path)

        return self._entry_attributes(relative_path, use_cache=False)

    async def mkdir(self, parent_inode: int, name: bytes, mode: int,
                    ctx=None) -> pyfuse3.EntryAttributes:
        """
        Create a directory.

        Args:
            parent_inode: Inode of the containing directory.
            name: New directory name.
            mode: Permission bits.
            ctx: Request context.

        Returns:
            Attributes of the new directory.
        """
        relative_path = self._child_path(parent_inode, name)

        try:
            os.mkdir(self._backing(relative_path), mode)
        except FileExistsError:
            raise pyfuse3.FUSEError(errno.EEXIST)
        except PermissionError:
            raise pyfuse3.FUSEError(errno.EACCES)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        if ctx is not None:
            try:
                os.chown(self._backing(relative_path), ctx.uid, ctx.gid)
            except (PermissionError, OSError):
                pass

        self._invalidate(relative_path)
        return self._entry_attributes(relative_path, use_cache=False)

    async def rmdir(self, parent_inode: int, name: bytes, ctx=None) -> None:
        """
        Remove an empty directory.

        Args:
            parent_inode: Inode of the containing directory.
            name: Directory name.
            ctx: Request context.
        """
        relative_path = self._child_path(parent_inode, name)

        try:
            os.rmdir(self._backing(relative_path))
        except FileNotFoundError:
            raise pyfuse3.FUSEError(errno.ENOENT)
        except OSError as exc:
            # ENOTEMPTY must reach the caller intact; `rm -r` depends on it.
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        self._invalidate(relative_path)
        self.inodes.forget_path(relative_path)

    async def unlink(self, parent_inode: int, name: bytes, ctx=None) -> None:
        """
        Remove a file.

        The delete must propagate as a tombstone with its own clock entry, or
        a peer that missed it re-advertises the file and it resurrects. That
        work happens in the engine; here we remove the backing file and post
        the event.

        Args:
            parent_inode: Inode of the containing directory.
            name: File name.
            ctx: Request context.
        """
        relative_path = self._child_path(parent_inode, name)

        try:
            os.unlink(self._backing(relative_path))
        except FileNotFoundError:
            raise pyfuse3.FUSEError(errno.ENOENT)
        except IsADirectoryError:
            raise pyfuse3.FUSEError(errno.EISDIR)
        except PermissionError:
            raise pyfuse3.FUSEError(errno.EACCES)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        self._invalidate(relative_path)
        self.inodes.forget_path(relative_path)

        engine = getattr(self.engine_bridge, "engine", None)
        if engine is not None:
            self._post(engine.on_file_deleted(relative_path))

    async def rename(self, parent_inode_old: int, name_old: bytes,
                     parent_inode_new: int, name_new: bytes, flags: int,
                     ctx=None) -> None:
        """
        Rename an entry.

        What is actually guaranteed, stated rather than implied:

        * **Locally, as applications see it: atomic.** `os.rename` on the
          backing store is atomic within a filesystem, and the inode number is
          preserved, so `stat -c %i` is unchanged as POSIX requires.
        * **Locally, catalog versus backing store: not atomic together.** A
          crash between the two is possible; startup reconciliation is exactly
          the mechanism that repairs it.
        * **Across peers: not atomic.** A peer may transiently see both names
          or neither. This is inherent to asynchronous replication and is
          documented in docs/FUSE_DESIGN.md section 6 rather than papered over.

        Args:
            parent_inode_old: Source directory inode.
            name_old: Source name.
            parent_inode_new: Destination directory inode.
            name_new: Destination name.
            flags: RENAME_NOREPLACE / RENAME_EXCHANGE, if requested.
            ctx: Request context.
        """
        if flags != 0:
            # RENAME_EXCHANGE and RENAME_NOREPLACE have no faithful mapping
            # onto a replicated rename, and guessing would silently break
            # atomicity that callers requested explicitly.
            raise pyfuse3.FUSEError(errno.EINVAL)

        old_path = self._child_path(parent_inode_old, name_old)
        new_path = self._child_path(parent_inode_new, name_new)

        try:
            os.rename(self._backing(old_path), self._backing(new_path))
        except FileNotFoundError:
            raise pyfuse3.FUSEError(errno.ENOENT)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO)

        self.inodes.rename(old_path, new_path)
        self._invalidate(old_path)
        self._invalidate(new_path)

        engine = getattr(self.engine_bridge, "engine", None)
        if engine is not None:
            self._post(engine.on_file_renamed(old_path, new_path))

    def _post_full_resync(self, relative_path: str) -> None:
        """
        Ask the engine to re-examine a path whose content changed outside a
        write handle (truncate, utime).

        Args:
            relative_path: Path to re-examine.
        """
        engine = getattr(self.engine_bridge, "engine", None)
        if engine is not None:
            self._post(engine.on_file_changed(relative_path))
