"""
Vector Clock Implementation for Distributed Concurrency Control.

This module implements vector clocks to track causality and detect concurrent 
operations in a distributed file synchronization system. Vector clocks enable 
proper ordering of events and conflict detection across multiple nodes.
"""

from typing import Dict, Optional
from enum import Enum
import json


class ClockRelation(Enum):
    """Represents the causal relationship between two vector clocks."""
    BEFORE = "before"       # This clock happened before the other
    AFTER = "after"         # This clock happened after the other
    CONCURRENT = "concurrent"  # These clocks are concurrent (conflict)
    EQUAL = "equal"         # These clocks are identical


class VectorClock:
    """
    A vector clock implementation for tracking causality in distributed systems.
    
    Each node maintains a vector of logical timestamps, one per node in the system.
    When an event occurs at a node, that node increments its own counter.
    When communicating, nodes merge their clocks to maintain causal ordering.
    
    Attributes:
        clock: Dictionary mapping node_id to its logical timestamp.
        node_id: The ID of the node that owns this clock.
    
    Example:
        >>> clock1 = VectorClock("node1")
        >>> clock1.increment()
        >>> clock2 = VectorClock("node2")
        >>> clock2.merge(clock1)
        >>> relation = clock1.compare(clock2)
    """
    
    def __init__(self, node_id: str, initial_clock: Optional[Dict[str, int]] = None):
        """
        Initialize a vector clock for a specific node.
        
        Args:
            node_id: Unique identifier for this node.
            initial_clock: Optional initial clock state for deserialization.
        """
        self.node_id: str = node_id
        self.clock: Dict[str, int] = initial_clock if initial_clock else {}
        
        # Ensure this node exists in the clock
        if node_id not in self.clock:
            self.clock[node_id] = 0
    
    def increment(self, node_id: Optional[str] = None) -> None:
        """
        Increment the clock for the specified node (defaults to self).
        
        This should be called when a local event occurs at this node.
        
        Args:
            node_id: The node whose counter to increment (defaults to self.node_id).
        """
        target_node = node_id if node_id else self.node_id
        
        if target_node not in self.clock:
            self.clock[target_node] = 0
        
        self.clock[target_node] += 1
    
    def merge(self, other: 'VectorClock') -> None:
        """
        Merge another vector clock into this one (take element-wise maximum).

        This implements the merge rule for vector clocks: for each node,
        take the maximum timestamp between this clock and the other.
        This should be called when receiving a message from another node.

        Merging does NOT increment this node's counter. Receiving a message is
        not a local event, and ticking here would make the receiver strictly
        AFTER the sender, so the receiver would immediately advertise a "newer"
        version back and the two peers would never converge to EQUAL. Callers
        that are also performing a local event should call increment()
        explicitly, or use merge_and_tick().

        Args:
            other: The vector clock to merge with this one.
        """
        # Merge all nodes from the other clock
        for node, timestamp in other.clock.items():
            if node not in self.clock:
                self.clock[node] = timestamp
            else:
                self.clock[node] = max(self.clock[node], timestamp)

    def merge_and_tick(self, other: 'VectorClock') -> None:
        """
        Merge another clock and then record a local event.

        Use this only when receiving the remote clock and performing a local
        mutation are genuinely the same event (for example, resolving a
        conflict by writing a merged version). For a plain receive, use
        merge().

        Args:
            other: The vector clock to merge with this one.
        """
        self.merge(other)
        self.increment()
    
    def compare(self, other: 'VectorClock') -> ClockRelation:
        """
        Compare this vector clock with another to determine causal relationship.
        
        The comparison follows these rules:
        - BEFORE: All timestamps in this clock <= other, and at least one is strictly less
        - AFTER: All timestamps in this clock >= other, and at least one is strictly greater
        - CONCURRENT: Some timestamps are greater, some are less (conflict)
        - EQUAL: All timestamps are identical
        
        Args:
            other: The vector clock to compare with.
        
        Returns:
            ClockRelation indicating the relationship between the clocks.
        """
        # Get all node IDs from both clocks
        all_nodes = set(self.clock.keys()) | set(other.clock.keys())
        
        less_than = False
        greater_than = False
        
        for node in all_nodes:
            self_time = self.clock.get(node, 0)
            other_time = other.clock.get(node, 0)
            
            if self_time < other_time:
                less_than = True
            elif self_time > other_time:
                greater_than = True
        
        # Determine relationship based on comparison results
        if not less_than and not greater_than:
            return ClockRelation.EQUAL
        elif less_than and not greater_than:
            return ClockRelation.BEFORE
        elif greater_than and not less_than:
            return ClockRelation.AFTER
        else:
            return ClockRelation.CONCURRENT
    
    def is_concurrent(self, other: 'VectorClock') -> bool:
        """
        Check if this clock is concurrent with another (indicates a conflict).
        
        Args:
            other: The vector clock to compare with.
        
        Returns:
            True if the clocks are concurrent, False otherwise.
        """
        return self.compare(other) == ClockRelation.CONCURRENT
    
    def happens_before(self, other: 'VectorClock') -> bool:
        """
        Check if this clock happened before another.
        
        Args:
            other: The vector clock to compare with.
        
        Returns:
            True if this clock happened before the other, False otherwise.
        """
        return self.compare(other) == ClockRelation.BEFORE
    
    def copy(self) -> 'VectorClock':
        """
        Create a deep copy of this vector clock.
        
        Returns:
            A new VectorClock instance with the same state.
        """
        return VectorClock(self.node_id, self.clock.copy())
    
    def to_dict(self) -> Dict[str, any]:
        """
        Serialize the vector clock to a dictionary for transmission.
        
        Returns:
            Dictionary representation of the clock.
        """
        return {
            'node_id': self.node_id,
            'clock': self.clock
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> 'VectorClock':
        """
        Deserialize a vector clock from a dictionary.
        
        Args:
            data: Dictionary containing clock data.
        
        Returns:
            A new VectorClock instance.
        """
        return cls(data['node_id'], data['clock'])
    
    def to_json(self) -> str:
        """
        Serialize the vector clock to JSON string.
        
        Returns:
            JSON string representation.
        """
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'VectorClock':
        """
        Deserialize a vector clock from JSON string.
        
        Args:
            json_str: JSON string containing clock data.
        
        Returns:
            A new VectorClock instance.
        """
        return cls.from_dict(json.loads(json_str))
    
    def __str__(self) -> str:
        """String representation of the vector clock."""
        clock_str = ", ".join(f"{k}:{v}" for k, v in sorted(self.clock.items()))
        return f"VectorClock({self.node_id})[{clock_str}]"
    
    def __repr__(self) -> str:
        """Detailed representation of the vector clock."""
        return f"VectorClock(node_id='{self.node_id}', clock={self.clock})"
    
    def __eq__(self, other: object) -> bool:
        """Check equality of two vector clocks."""
        if not isinstance(other, VectorClock):
            return False
        return self.compare(other) == ClockRelation.EQUAL
