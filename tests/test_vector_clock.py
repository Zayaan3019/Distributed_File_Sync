"""
Tests for Vector Clock implementation.
"""

import pytest
from app.algorithms.vector_clock import VectorClock, ClockRelation


class TestVectorClock:
    """Test suite for VectorClock."""
    
    def test_initialization(self):
        """Test vector clock initialization."""
        clock = VectorClock("node1")
        assert clock.node_id == "node1"
        assert clock.clock["node1"] == 0
    
    def test_increment(self):
        """Test clock increment."""
        clock = VectorClock("node1")
        clock.increment()
        assert clock.clock["node1"] == 1
        
        clock.increment()
        assert clock.clock["node1"] == 2
    
    def test_increment_other_node(self):
        """Test incrementing another node's counter."""
        clock = VectorClock("node1")
        clock.increment("node2")
        assert clock.clock["node2"] == 1
        assert clock.clock["node1"] == 0
    
    def test_merge(self):
        """Test merging two clocks takes the element-wise maximum."""
        clock1 = VectorClock("node1")
        clock1.increment()  # node1: 1

        clock2 = VectorClock("node2")
        clock2.increment()  # node2: 1

        clock1.merge(clock2)
        # merge() is a pure element-wise max: receiving a message is not a
        # local event, so node1's own counter must not tick.
        assert clock1.clock["node1"] == 1
        assert clock1.clock["node2"] == 1

    def test_merge_and_tick(self):
        """merge_and_tick() merges and then records a local event."""
        clock1 = VectorClock("node1")
        clock1.increment()  # node1: 1

        clock2 = VectorClock("node2")
        clock2.increment()  # node2: 1

        clock1.merge_and_tick(clock2)
        assert clock1.clock["node1"] == 2
        assert clock1.clock["node2"] == 1

    def test_merge_converges_to_equal(self):
        """Two peers exchanging clocks must converge, not ping-pong.

        Regression test: merge() used to increment the merger's own counter,
        which made the receiver strictly AFTER the sender. The receiver would
        then advertise a "newer" version back and the pair would never settle.
        """
        a = VectorClock("A")
        b = VectorClock("B")

        a.increment()          # A edits a file
        assert a.compare(b) == ClockRelation.AFTER

        b.merge(a)             # B receives A's clock
        assert b.compare(a) == ClockRelation.EQUAL

        a.merge(b)             # and back again: still settled
        assert a.compare(b) == ClockRelation.EQUAL

    def test_merge_preserves_concurrent_detection(self):
        """Plain-max merge must not mask genuine concurrent edits."""
        a = VectorClock("A")
        b = VectorClock("B")
        a.increment()
        b.increment()

        assert a.compare(b) == ClockRelation.CONCURRENT

        # Both peers resolve the same conflict independently and converge.
        a_resolved, b_resolved = a.copy(), b.copy()
        a_resolved.merge(b)
        b_resolved.merge(a)
        assert a_resolved.compare(b_resolved) == ClockRelation.EQUAL

    def test_merge_is_idempotent(self):
        """Merging the same clock twice changes nothing the second time."""
        a = VectorClock("A")
        a.increment()
        b = VectorClock("B")
        b.increment()

        a.merge(b)
        snapshot = dict(a.clock)
        a.merge(b)
        assert a.clock == snapshot
    
    def test_compare_equal(self):
        """Test comparing equal clocks."""
        clock1 = VectorClock("node1")
        clock1.increment()
        
        clock2 = VectorClock("node2", {"node1": 1})
        
        assert clock1.compare(clock2) == ClockRelation.EQUAL
    
    def test_compare_before(self):
        """Test comparing when first clock is before second."""
        clock1 = VectorClock("node1")
        clock1.increment()  # {node1: 1}
        
        clock2 = VectorClock("node2")
        clock2.clock = {"node1": 2, "node2": 1}
        
        assert clock1.compare(clock2) == ClockRelation.BEFORE
    
    def test_compare_after(self):
        """Test comparing when first clock is after second."""
        clock1 = VectorClock("node1")
        clock1.clock = {"node1": 2, "node2": 1}
        
        clock2 = VectorClock("node2")
        clock2.increment()  # {node2: 1}
        
        assert clock1.compare(clock2) == ClockRelation.AFTER
    
    def test_compare_concurrent(self):
        """Test comparing concurrent clocks (conflict)."""
        clock1 = VectorClock("node1")
        clock1.clock = {"node1": 2, "node2": 1}
        
        clock2 = VectorClock("node2")
        clock2.clock = {"node1": 1, "node2": 2}
        
        assert clock1.compare(clock2) == ClockRelation.CONCURRENT
    
    def test_is_concurrent(self):
        """Test concurrent check."""
        clock1 = VectorClock("node1")
        clock1.clock = {"node1": 2, "node2": 1}
        
        clock2 = VectorClock("node2")
        clock2.clock = {"node1": 1, "node2": 2}
        
        assert clock1.is_concurrent(clock2)
    
    def test_happens_before(self):
        """Test happens-before check."""
        clock1 = VectorClock("node1")
        clock1.increment()
        
        clock2 = VectorClock("node2")
        clock2.clock = {"node1": 2, "node2": 1}
        
        assert clock1.happens_before(clock2)
    
    def test_copy(self):
        """Test clock copy."""
        clock1 = VectorClock("node1")
        clock1.increment()
        
        clock2 = clock1.copy()
        assert clock2.node_id == clock1.node_id
        assert clock2.clock == clock1.clock
        assert clock2 is not clock1
    
    def test_serialization(self):
        """Test serialization and deserialization."""
        clock1 = VectorClock("node1")
        clock1.increment()
        clock1.increment("node2")
        
        # To dict
        data = clock1.to_dict()
        assert data["node_id"] == "node1"
        assert data["clock"]["node1"] == 1
        assert data["clock"]["node2"] == 1
        
        # From dict
        clock2 = VectorClock.from_dict(data)
        assert clock2.node_id == clock1.node_id
        assert clock2.clock == clock1.clock
    
    def test_json_serialization(self):
        """Test JSON serialization."""
        clock1 = VectorClock("node1")
        clock1.increment()
        
        json_str = clock1.to_json()
        clock2 = VectorClock.from_json(json_str)
        
        assert clock2.node_id == clock1.node_id
        assert clock2.clock == clock1.clock
    
    def test_equality(self):
        """Test equality operator."""
        clock1 = VectorClock("node1")
        clock1.increment()
        
        clock2 = VectorClock("node2", {"node1": 1})
        
        assert clock1 == clock2
    
    def test_string_representation(self):
        """Test string representation."""
        clock = VectorClock("node1")
        clock.increment()
        
        str_repr = str(clock)
        assert "node1" in str_repr
        assert "1" in str_repr
