"""
File System Monitor using Watchdog.

This module monitors the sync directory for file system changes (create, modify, delete)
and triggers synchronization events. It uses the watchdog library for efficient
event-driven file monitoring.
"""

import asyncio
import logging
from pathlib import Path
from typing import Callable, Awaitable, Set
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
        
        # Debounce rapid events (track recent events)
        self._recent_events: Set[str] = set()
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
    
    def _schedule_callback(self, coro: Awaitable[None], event_key: str) -> None:
        """
        Schedule an async callback with debouncing.
        
        Args:
            coro: Coroutine to schedule.
            event_key: Unique key for this event (for debouncing).
        """
        # Skip if we recently processed this event
        if event_key in self._recent_events:
            logger.debug(f"Debouncing event: {event_key}")
            return
        
        # Mark event as recent
        self._recent_events.add(event_key)
        
        # Schedule the coroutine
        asyncio.run_coroutine_threadsafe(coro, self.event_loop)
        
        # Clear event from recent after delay
        def clear_event():
            self._recent_events.discard(event_key)
        
        self.event_loop.call_later(self._debounce_delay, clear_event)
    
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
        
        coro = self.on_file_changed(file_path)
        self._schedule_callback(coro, f"created:{file_path}")
    
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
        
        coro = self.on_file_changed(file_path)
        self._schedule_callback(coro, f"modified:{file_path}")
    
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
        
        coro = self.on_file_deleted(file_path)
        self._schedule_callback(coro, f"deleted:{file_path}")
    
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
        delete_coro = self.on_file_deleted(src_path)
        create_coro = self.on_file_changed(dest_path)
        
        self._schedule_callback(delete_coro, f"deleted:{src_path}")
        self._schedule_callback(create_coro, f"created:{dest_path}")


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
