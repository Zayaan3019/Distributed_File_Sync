"""
Distributed File Synchronization Engine.

A production-grade P2P file sync system using:
- Merkle Trees for bandwidth-optimized synchronization
- Vector Clocks for conflict detection and resolution
- AsyncIO for non-blocking I/O operations
- Custom binary protocol over TCP
"""

__version__ = '1.0.0'
__author__ = 'Distributed Systems Team'

from .core import SyncConfig, PeerConfig, Protocol, Message, MessageType
from .algorithms import VectorClock, ClockRelation, MerkleTree
from .services import NetworkManager, SyncEngine, FileMonitorManager

__all__ = [
    'SyncConfig',
    'PeerConfig',
    'Protocol',
    'Message',
    'MessageType',
    'VectorClock',
    'ClockRelation',
    'MerkleTree',
    'NetworkManager',
    'SyncEngine',
    'FileMonitorManager'
]
