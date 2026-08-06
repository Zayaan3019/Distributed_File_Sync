"""
File System Monitor using Watchdog.

This module monitors the sync directory for file system changes (create, modify, delete)
and triggers synchronization events. It uses the watchdog library for efficient
event-driven file monitoring.
"""

import asyncio
import logging
from pathlib import Path
import threading
from typing import Callable, Awaitable, Dict, Set
from watchdog.observers import Observer
from watchdog.events import (
    FileSystemEventHandler,
    FileSystemEvent,
    FileModifiedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileMovedEvent
)

logger = logging.getLogger(__name__)


class AsyncFileMonitor(FileSystemEventHandler):
    """
    Async file system event handler.
    
    This class bridges synchronous watchdog events to async callbacks,
    allowing integration with the asyncio event loop.
    
    Attributes:
        sync_directory: Directory being monitored.
        on_file_changed: Async callback for file changes.
        on_file_deleted: Async callback for file deletions.
        event_loop: AsyncIO event loop to schedule callbacks.
        ignored_patterns: Set of file patterns to ignore.
    """
    
    def __init__(
        self,
        sync_directory: str,
        on_file_changed: Callable[[str], Awaitable[None]],
        on_file_deleted: Callable[[str], Awaitable[None]],
        event_loop: asyncio.AbstractEventLoop
    ):
        """
        Initialize the file monitor.
        
        Args:
            sync_directory: Directory to monitor.
            on_file_changed: Async callback for file modifications/creations.
            on_file_deleted: Async callback for file deletions.
            event_loop: AsyncIO event loop.
        """
        super().__init__()
        self.sync_directory = Path(sync_directory).resolve()
        self.on_file_changed = on_file_changed
        self.on_file_deleted = on_file_deleted
        self.event_loop = event_loop
        
        # Patterns to ignore (temporary files, system files, etc.)
        self.ignored_patterns: Set[str] = {
            '.tmp', '.temp', '.swp', '.swo', '.swn',
            '~', '.DS_Store', 'Thumbs.db', '.git',
            '__pycache__', '.pyc', '_conflict'
        }
        
        # Debounce rapid events. Maps event key -> "another event for this key
        # arrived while the window was open", which is what makes the debounce
        # coalescing rather than lossy. See `_schedule_callback`.
        self._pending: Dict[str, bool] = {}
        self._pending_lock = threading.Lock()
        self._debounce_delay = 0.5  # seconds
    
    def _should_ignore(self, file_path: str) -> bool:
        """
        Check if a file should be ignored.
        
        Args:
            file_path: Path to check.
        
        Returns:
            True if file should be ignored, False otherwise.
        """
        path = Path(file_path)
        
        # Ignore directories
        if path.is_dir():
            return True
        
        # Check against ignored patterns
        file_name = path.name
        for pattern in self.ignored_patterns:
            if pattern in file_name:
                return True
        
        # Ignore files outside sync directory
        try:
            path.resolve().relative_to(self.sync_directory)
        except ValueError:
            return True
        
        return False
    
    def _get_relative_path(self, file_path: str) -> str:
        """
        Get path relative to sync directory.
        
        Args:
            file_path: Absolute file path.
        
        Returns:
            Relative path as string.
        """
        try:
            abs_path = Path(file_path).resolve()
            return str(abs_path.relative_to(self.sync_directory))
        except ValueError:
            return file_path
    
    def _schedule_callback(self, make_coro: Callable[[], Awaitable[None]],
                           event_key: str) -> None:
        """
        Schedule an async callback, coalescing bursts for the same path.

        Editors and build tools produce several events per logical save, so
        some debouncing is required or every save costs multiple full hashes.
        The previous implementation debounced by *dropping* the later event,
        which loses writes: a file modified twice inside the window was hashed
        once, at whatever content it held the first time, and the second edit
        never propagated at all. It also built the coroutine before deciding
        to drop it, so every debounced event leaked an un-awaited coroutine --
        visible as `RuntimeWarning: coroutine 'SyncEngine.on_file_changed' was
        never awaited` in the container logs, which is how this was found.

        The window is now trailing-edge: a duplicate arriving mid-window sets a
        dirty flag instead of being discarded, and one more run is scheduled
        when the window closes. The last state of the file is therefore always
        processed, at the cost of at most one extra pass.

        Args:
            make_coro: Zero-argument factory producing a *fresh* coroutine.
                A factory rather than a coroutine because a dropped or
                re-scheduled event must not reuse an already-created one.
            event_key: Key identifying this event for coalescing.
        """
        with self._pending_lock:
            if event_key in self._pending:
                # A run is already in flight for this path. Remember that the
                # file changed again so we re-run once the window closes.
                self._pending[event_key] = True
                logger.debug(f"Coalescing event: {event_key}")
                return
            self._pending[event_key] = False

        self._submit(make_coro())

        # call_later is not thread-safe and watchdog calls us from the
        # observer thread, so the timer has to be armed from inside the loop.
        def arm() -> None:
            self.event_loop.call_later(
                self._debounce_delay, self._window_closed, event_key, make_coro
            )

        try:
            self.event_loop.call_soon_threadsafe(arm)
        except RuntimeError:
            # Loop is shutting down; drop the timer and the bookkeeping.
            with self._pending_lock:
                self._pending.pop(event_key, None)

    def _window_closed(self, event_key: str,
                       make_coro: Callable[[], Awaitable[None]]) -> None:
        """
        Close a debounce window, re-running once if the path changed again.

        Args:
            event_key: The key whose window expired.
            make_coro: Factory for another run, if one is owed.
        """
        with self._pending_lock:
            dirty = self._pending.pop(event_key, False)

        if dirty:
            logger.debug(f"Re-running coalesced event: {event_key}")
            self._schedule_callback(make_coro, event_key)

    def _submit(self, coro: Awaitable[None]) -> None:
        """
        Hand a coroutine to the engine's event loop from the observer thread.

        Args:
            coro: Coroutine to run. Closed rather than leaked if the loop is
                already gone, so shutdown stays quiet.
        """
        if self.event_loop.is_closed():
            coro.close()
            return
        try:
            asyncio.run_coroutine_threadsafe(coro, self.event_loop)
        except RuntimeError:
            coro.close()


    def on_created(self, event: FileCreatedEvent) -> None:
        """
        Handle file creation event.
        
        Args:
            event: File system event.
        """
        if self._should_ignore(event.src_path):
            return
        
        file_path = self._get_relative_path(event.src_path)
        logger.info(f"File created: {file_path}")
        
        self._schedule_callback(
            lambda: self.on_file_changed(file_path), f"changed:{file_path}"
        )
    
    def on_modified(self, event: FileModifiedEvent) -> None:
        """
        Handle file modification event.
        
        Args:
            event: File system event.
        """
        if self._should_ignore(event.src_path):
            return
        
        file_path = self._get_relative_path(event.src_path)
        logger.info(f"File modified: {file_path}")
        
        # Same key as `on_created`: both end in `on_file_changed`, and a save
        # that produces create-then-modify should cost one hash, not two.
        self._schedule_callback(
            lambda: self.on_file_changed(file_path), f"changed:{file_path}"
        )
    
    def on_deleted(self, event: FileDeletedEvent) -> None:
        """
        Handle file deletion event.
        
        Args:
            event: File system event.
        """
        if self._should_ignore(event.src_path):
            return
        
        file_path = self._get_relative_path(event.src_path)
        logger.info(f"File deleted: {file_path}")
        
        self._schedule_callback(
            lambda: self.on_file_deleted(file_path), f"deleted:{file_path}"
        )
    
    def on_moved(self, event: FileMovedEvent) -> None:
        """
        Handle file move/rename event.
        
        Args:
            event: File system event.
        """
        if self._should_ignore(event.src_path) or self._should_ignore(event.dest_path):
            return
        
        src_path = self._get_relative_path(event.src_path)
        dest_path = self._get_relative_path(event.dest_path)
        logger.info(f"File moved: {src_path} -> {dest_path}")
        
        # Treat move as delete + create
        self._schedule_callback(
            lambda: self.on_file_deleted(src_path), f"deleted:{src_path}"
        )
        self._schedule_callback(
            lambda: self.on_file_changed(dest_path), f"changed:{dest_path}"
        )


class FileMonitorManager:
    """
    Manages the file system observer and event handling.
    
    Attributes:
        sync_directory: Directory to monitor.
        event_handler: Async event handler.
        observer: Watchdog observer instance.
    """
    
    def __init__(
        self,
        sync_directory: str,
        on_file_changed: Callable[[str], Awaitable[None]],
        on_file_deleted: Callable[[str], Awaitable[None]],
        event_loop: asyncio.AbstractEventLoop
    ):
        """
        Initialize the file monitor manager.
        
        Args:
            sync_directory: Directory to monitor.
            on_file_changed: Async callback for file changes.
            on_file_deleted: Async callback for file deletions.
            event_loop: AsyncIO event loop.
        """
        self.sync_directory = sync_directory
        self.event_handler = AsyncFileMonitor(
            sync_directory,
            on_file_changed,
            on_file_deleted,
            event_loop
        )
        self.observer = Observer()
        self._running = False
    
    def start(self) -> None:
        """
        Start monitoring the file system.
        """
        if self._running:
            logger.warning("File monitor already running")
            return
        
        logger.info(f"Starting file monitor on {self.sync_directory}")
        
        # Schedule the observer
        self.observer.schedule(
            self.event_handler,
            self.sync_directory,
            recursive=True
        )
        
        self.observer.start()
        self._running = True
        logger.info("File monitor started")
    
    def stop(self) -> None:
        """
        Stop monitoring the file system.
        """
        if not self._running:
            return
        
        logger.info("Stopping file monitor")
        self.observer.stop()
        self.observer.join(timeout=5.0)
        self._running = False
        logger.info("File monitor stopped")
    
    def is_running(self) -> bool:
        """
        Check if monitor is running.
        
        Returns:
            True if running, False otherwise.
        """
        return self._running
