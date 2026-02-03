"""
Services module for distributed file synchronization.

This module provides service components:
- Network: Async TCP communication layer
- File Monitor: Watchdog-based file system monitoring
- Sync Engine: Core synchronization orchestration
"""

from .network import NetworkManager, PeerConnection
from .file_monitor import FileMonitorManager, AsyncFileMonitor
from .sync_engine import SyncEngine, FileMetadata

__all__ = [
    'NetworkManager',
    'PeerConnection',
    'FileMonitorManager',
    'AsyncFileMonitor',
    'SyncEngine',
    'FileMetadata'
]
