"""
Algorithms module for distributed file synchronization.

This module contains core algorithms for conflict resolution and bandwidth optimization:
- Vector Clocks: Causal ordering and conflict detection
- Merkle Trees: Efficient file diffing and bandwidth-optimized synchronization
"""

from .vector_clock import VectorClock, ClockRelation
from .merkle import MerkleTree, MerkleNode, TreeDiff

__all__ = ['VectorClock', 'ClockRelation', 'MerkleTree', 'MerkleNode', 'TreeDiff']
