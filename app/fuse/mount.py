"""
The trio/asyncio boundary.

pyfuse3 runs on trio; the sync engine runs on asyncio. Two event loops cannot
share a thread, so the FUSE session gets a dedicated thread with its own trio
loop and the two communicate in exactly one direction:

    FUSE thread (trio)  --run_coroutine_threadsafe-->  main thread (asyncio)

Nothing goes the other way. That asymmetry is the entire deadlock argument:

* The FUSE thread posts work to asyncio and **never waits for the result**
  (`EngineBridge.__call__` drops the future deliberately). A handler that
  waited would block every other filesystem operation behind a network round
  trip.
* The asyncio engine never performs I/O through the mount point -- it operates
  on the backing directory only. If it ever touched the mount, a FUSE handler
  waiting on the engine while the engine waited on FUSE would be an immediate,
  unrecoverable deadlock. This is why the backing store is a *separate
  directory* from the mount point, and it is the single most important
  structural constraint in the design.

Because no call ever blocks in both directions, there is no cycle, and
therefore no deadlock to reason about at runtime.

This module is imported only on Linux. `import pyfuse3` fails on Windows,
which is why `app.fuse.__init__` defers it.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from pathlib import Path
from typing import Callable, Optional

import pyfuse3
import trio

from .inodes import InodeTable
from .operations import FileSyncFs

logger = logging.getLogger(__name__)


class EngineBridge:
    """
    Callable that schedules engine coroutines from the FUSE thread.

    Deliberately fire-and-forget. The returned `concurrent.futures.Future` is
    dropped rather than awaited, because waiting is what would deadlock. The
    cost is that a replication failure surfaces in the log rather than as an
    error to the calling application -- which is correct for an eventually
    consistent system: the write *did* succeed locally, and local durability is
    the only thing the caller was promised.

    Attributes:
        engine: The SyncEngine, reached by handlers via ``bridge.engine``.
        loop: The asyncio loop owning the engine.
    """

    def __init__(self, engine, loop: asyncio.AbstractEventLoop):
        """
        Initialize the bridge.

        Args:
            engine: SyncEngine instance living on `loop`.
            loop: The asyncio event loop.
        """
        self.engine = engine
        self.loop = loop
        self._pending = 0
        self._lock = threading.Lock()

    def __call__(self, coro) -> None:
        """
        Schedule a coroutine on the asyncio loop.

        Args:
            coro: Coroutine to run. Closed without running if the loop has
                already stopped, so shutdown does not emit "coroutine was
                never awaited" warnings.
        """
        if self.loop.is_closed():
            coro.close()
            return

        with self._lock:
            self._pending += 1

        def _done(_future) -> None:
            with self._lock:
                self._pending -= 1
            try:
                exc = _future.exception()
            except Exception:
                return
            if exc is not None:
                logger.error(f"Replication task failed: {exc!r}")

        try:
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            future.add_done_callback(_done)
        except RuntimeError:
            # Loop shut down between the check and the call.
            coro.close()
            with self._lock:
                self._pending -= 1

    def pending(self) -> int:
        """
        Number of posted-but-unfinished replication tasks.

        Returns:
            The pending count. Used by tests to wait for quiescence rather
            than sleeping and hoping.
        """
        with self._lock:
            return self._pending


class FuseMount:
    """
    Owns the FUSE session thread and its trio loop.

    Attributes:
        mount_point: Directory the filesystem is mounted on.
        backing_root: Directory holding the real files.
        fs: The `FileSyncFs` instance.
    """

    def __init__(self, mount_point: str, backing_root: str, catalog_path: str,
                 engine=None, loop: Optional[asyncio.AbstractEventLoop] = None,
                 chunk_size: int = 4096, allow_other: bool = False,
                 debug: bool = False):
        """
        Initialize the mount.

        Args:
            mount_point: Where to mount. Created if absent.
            backing_root: Directory holding the real files.
            catalog_path: SQLite catalog, for the persistent inode table.
            engine: SyncEngine to replicate through, or None for a local-only
                mount.
            loop: The asyncio loop the engine runs on.
            chunk_size: Merkle chunk size.
            allow_other: Pass ``allow_other`` to libfuse.
            debug: Enable libfuse debug logging.

        Raises:
            ValueError: If the mount point is inside the backing store.
        """
        self.mount_point = str(Path(mount_point).resolve())
        self.backing_root = str(Path(backing_root).resolve())
        self.catalog_path = str(catalog_path)
        self.allow_other = allow_other
        self.debug = debug

        # The one structural rule that must never be violated. If the mount
        # sat inside the backing store, the engine's ordinary file I/O would
        # re-enter FUSE and deadlock -- and the watchdog observer would spin
        # forever on its own events.
        if self.mount_point == self.backing_root:
            raise ValueError("mount point and backing store must differ")
        if Path(self.backing_root) in Path(self.mount_point).parents:
            raise ValueError(
                f"mount point {self.mount_point} is inside the backing store "
                f"{self.backing_root}; this deadlocks the engine"
            )
        if Path(self.mount_point) in Path(self.backing_root).parents:
            raise ValueError(
                f"backing store {self.backing_root} is inside the mount point "
                f"{self.mount_point}; this deadlocks the engine"
            )

        self.inodes = InodeTable(self.catalog_path)
        self.bridge = EngineBridge(engine, loop) if engine is not None else None
        self.fs = None
        self.chunk_size = chunk_size

        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._stopped = threading.Event()
        self._error: Optional[BaseException] = None

        #: Handle onto the session's trio loop, captured once it is running.
        #: `pyfuse3.terminate()` refuses to run outside a trio context, so
        #: `stop()` cannot simply call it from the main thread -- it has to
        #: hand the call back into the loop it is trying to stop.
        self._trio_token = None

    def start(self) -> None:
        """
        Mount the filesystem on a background thread and return once ready.

        Raises:
            RuntimeError: If the mount failed to come up.
        """
        Path(self.mount_point).mkdir(parents=True, exist_ok=True)
        Path(self.backing_root).mkdir(parents=True, exist_ok=True)

        self.inodes.open()
        self.fs = FileSyncFs(
            self.backing_root, self.inodes,
            engine_bridge=self.bridge, chunk_size=self.chunk_size,
        )

        self._thread = threading.Thread(
            target=self._run, name="fuse-session", daemon=True
        )
        self._thread.start()

        if not self._ready.wait(timeout=30.0):
            raise RuntimeError("FUSE mount did not become ready within 30s")

        if self._error is not None:
            raise RuntimeError(f"FUSE mount failed: {self._error!r}")

        logger.info(f"FUSE mounted at {self.mount_point} over {self.backing_root}")

    def _run(self) -> None:
        """Thread body: initialise the session and run the trio loop."""
        try:
            options = set(pyfuse3.default_options)
            options.add("fsname=dfs")
            # default_permissions makes the kernel enforce the mode bits we
            # report, instead of us re-implementing POSIX permission checks
            # in every handler and getting one of them wrong.
            options.add("default_permissions")
            if self.allow_other:
                options.add("allow_other")
            if self.debug:
                options.add("debug")

            pyfuse3.init(self.fs, self.mount_point, options)
            self._ready.set()

            trio.run(self._serve)

        except BaseException as exc:
            self._error = exc
            logger.error(f"FUSE session error: {exc!r}", exc_info=True)
            self._ready.set()
        finally:
            try:
                pyfuse3.close(unmount=True)
            except Exception:
                pass
            self._stopped.set()

    async def _serve(self) -> None:
        """Run the pyfuse3 main loop under trio."""
        self._trio_token = trio.lowlevel.current_trio_token()
        await pyfuse3.main()

    def stop(self, timeout: float = 15.0) -> None:
        """
        Unmount and stop the session thread.

        Args:
            timeout: Seconds to wait for the thread to exit.
        """
        if self._thread is None:
            return

        # terminate() must run inside the trio context, so it is posted into
        # the session loop rather than called here. Calling it directly from
        # this thread raises "must be called from async context" -- which it
        # did, silently, leaving the session to be torn down by the forced
        # lazy-unmount fallback below instead of shutting down cleanly.
        if self._trio_token is not None:
            try:
                self._trio_token.run_sync_soon(pyfuse3.terminate)
            except Exception as exc:
                logger.warning(f"could not post pyfuse3.terminate(): {exc}")
        else:
            logger.warning("FUSE session never started; nothing to terminate")

        # terminate() only sets a flag; the trio session loop notices it when
        # it next returns from waiting on the FUSE device. On an idle mount
        # nothing ever arrives, so the loop sits there and join() burns its
        # full timeout -- measured at ~14s per unmount across the test suite.
        # One harmless request wakes it immediately.
        try:
            os.listdir(self.mount_point)
        except OSError:
            # Already gone, or the session is mid-teardown. Either is fine.
            pass

        self._thread.join(timeout=timeout)

        if self._thread.is_alive():
            # The session is wedged, usually because a process still holds a
            # file open under the mount. A lazy unmount detaches the tree so
            # the thread can finish rather than hanging shutdown forever.
            logger.warning("FUSE thread still alive; forcing lazy unmount")
            os.system(f"fusermount3 -uz {self.mount_point!r} 2>/dev/null")
            self._thread.join(timeout=5.0)

        self.inodes.close()
        logger.info(f"FUSE unmounted from {self.mount_point}")

    def is_running(self) -> bool:
        """
        Report whether the session thread is alive.

        Returns:
            True while the filesystem is mounted.
        """
        return self._thread is not None and self._thread.is_alive()


def mount_blocking(mount_point: str, backing_root: str, catalog_path: str,
                   chunk_size: int = 4096, allow_other: bool = False) -> None:
    """
    Mount a local-only filesystem and serve until interrupted.

    Used by the standalone `--fuse-only` mode and by the benchmark harness,
    where there is no sync engine and therefore no asyncio loop to bridge to.

    Args:
        mount_point: Where to mount.
        backing_root: Directory holding the real files.
        catalog_path: SQLite catalog for the inode table.
        chunk_size: Merkle chunk size.
        allow_other: Pass ``allow_other`` to libfuse.
    """
    inodes = InodeTable(catalog_path)
    inodes.open()

    fs = FileSyncFs(backing_root, inodes, engine_bridge=None,
                    chunk_size=chunk_size)

    options = set(pyfuse3.default_options)
    options.add("fsname=dfs")
    options.add("default_permissions")
    if allow_other:
        options.add("allow_other")

    pyfuse3.init(fs, mount_point, options)
    try:
        trio.run(pyfuse3.main)
    finally:
        pyfuse3.close(unmount=True)
        inodes.close()
