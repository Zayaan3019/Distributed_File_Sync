"""
Tests for Merkle Tree implementation.
"""

import pytest
import asyncio
import tempfile
import os
from pathlib import Path

from app.algorithms.merkle import MerkleTree, MerkleNode, TreeDiff, CHUNK_SIZE


class TestMerkleTree:
    """Test suite for MerkleTree."""
    
    @pytest.fixture
    def temp_file(self):
        """Create a temporary file for testing."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            # Write 10KB of data
            data = b'A' * (10 * 1024)
            f.write(data)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    @pytest.fixture
    def temp_file_small(self):
        """Create a small temporary file."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'Hello World')
            temp_path = f.name
        
        yield temp_path
        
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    @pytest.mark.asyncio
    async def test_from_file(self, temp_file):
        """Test building Merkle tree from file."""
        tree = await MerkleTree.from_file(temp_file)
        
        assert tree.root is not None
        assert tree.file_path == temp_file
        assert tree.file_size == 10 * 1024
        assert len(tree.root.hash) == 64  # SHA-256 hex length
    
    @pytest.mark.asyncio
    async def test_empty_file(self):
        """Test building tree from empty file."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            temp_path = f.name
        
        try:
            tree = await MerkleTree.from_file(temp_path)
            assert tree.root.is_leaf
            assert tree.file_size == 0
        finally:
            os.unlink(temp_path)
    
    @pytest.mark.asyncio
    async def test_file_not_found(self):
        """Test error handling for missing file."""
        with pytest.raises(FileNotFoundError):
            await MerkleTree.from_file("/nonexistent/file.txt")
    
    @pytest.mark.asyncio
    async def test_identical_files(self, temp_file):
        """Test that identical files produce identical trees."""
        tree1 = await MerkleTree.from_file(temp_file)
        tree2 = await MerkleTree.from_file(temp_file)
        
        assert tree1.root.hash == tree2.root.hash
    
    @pytest.mark.asyncio
    async def test_different_files(self, temp_file):
        """Test that different files produce different trees."""
        tree1 = await MerkleTree.from_file(temp_file)
        
        # Create a different file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'B' * (10 * 1024))
            temp_path2 = f.name
        
        try:
            tree2 = await MerkleTree.from_file(temp_path2)
            assert tree1.root.hash != tree2.root.hash
        finally:
            os.unlink(temp_path2)
    
    @pytest.mark.asyncio
    async def test_get_diff_identical(self, temp_file):
        """Test diff between identical trees."""
        tree1 = await MerkleTree.from_file(temp_file)
        tree2 = await MerkleTree.from_file(temp_file)
        
        diff = MerkleTree.get_diff(tree1, tree2)
        
        assert diff.is_empty()
        assert len(diff.changed_chunks) == 0
    
    @pytest.mark.asyncio
    async def test_get_diff_different(self):
        """Test diff between different trees."""
        # Create two different files
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'A' * (10 * 1024))
            path1 = f.name
        
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            # Same size, different content in first chunk
            f.write(b'B' * CHUNK_SIZE)
            f.write(b'A' * (10 * 1024 - CHUNK_SIZE))
            path2 = f.name
        
        try:
            tree1 = await MerkleTree.from_file(path1)
            tree2 = await MerkleTree.from_file(path2)
            
            diff = MerkleTree.get_diff(tree1, tree2)
            
            assert not diff.is_empty()
            assert 0 in diff.changed_chunks  # First chunk should differ
        finally:
            os.unlink(path1)
            os.unlink(path2)
    
    @pytest.mark.asyncio
    async def test_get_chunks(self, temp_file):
        """Test reading specific chunks."""
        tree = await MerkleTree.from_file(temp_file)
        
        # Request first two chunks
        chunks = await tree.get_chunks({0, 1})
        
        assert len(chunks) == 2
        assert 0 in chunks
        assert 1 in chunks
        assert len(chunks[0]) == CHUNK_SIZE
        assert chunks[0] == b'A' * CHUNK_SIZE
    
    @pytest.mark.asyncio
    async def test_serialization(self, temp_file_small):
        """Test tree serialization."""
        tree1 = await MerkleTree.from_file(temp_file_small)
        
        # Serialize
        data = tree1.to_dict()
        
        assert data['root_hash'] == tree1.root.hash
        assert data['file_path'] == temp_file_small
        assert 'root_node' in data
        
        # Deserialize
        tree2 = MerkleTree.from_dict(data)
        
        assert tree2.root.hash == tree1.root.hash
        assert tree2.file_size == tree1.file_size
    
    def test_node_equality(self):
        """Test MerkleNode equality."""
        node1 = MerkleNode(hash="abc123", is_leaf=True)
        node2 = MerkleNode(hash="abc123", is_leaf=False)
        node3 = MerkleNode(hash="def456", is_leaf=True)
        
        assert node1 == node2  # Same hash
        assert node1 != node3  # Different hash
    
    def test_tree_diff(self):
        """Test TreeDiff functionality."""
        diff = TreeDiff()
        
        assert diff.is_empty()
        
        diff.add_chunk(0, 0, 4096)
        diff.add_chunk(1, 4096, 8192)
        
        assert not diff.is_empty()
        assert len(diff.changed_chunks) == 2
        assert diff.total_bytes() == 8192
    
    @pytest.mark.asyncio
    async def test_small_file_single_chunk(self, temp_file_small):
        """Test that small files create single-chunk trees."""
        tree = await MerkleTree.from_file(temp_file_small)
        
        assert tree.root.is_leaf
        assert tree.root.chunk_index == 0
