"""
Core module for distributed file synchronization.

This module provides core functionality including:
- Configuration management
- Binary protocol implementation
- Logging infrastructure
"""

from .config import SyncConfig, PeerConfig
from .protocol import Protocol, Message, MessageType
from .logger import setup_logger, get_logger

__all__ = [
    'SyncConfig',
    'PeerConfig',
    'Protocol',
    'Message',
    'MessageType',
    'setup_logger',
    'get_logger'
]
