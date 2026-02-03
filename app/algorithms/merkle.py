"""
Merkle Tree Implementation for Bandwidth-Optimized File Synchronization.

This module implements Merkle trees to efficiently detect and transfer only the 
changed chunks of files. By comparing tree roots and recursively traversing 
differences, we minimize bandwidth usage during synchronization.
"""

import hashlib
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass, field
from pathlib import Path
import asyncio
import aiofiles


# Chunk size for file splitting (4KB for optimal I/O performance)
CHUNK_SIZE = 4096


@dataclass
class MerkleNode:
    """
    Represents a node in the Merkle tree.
    
    Attributes:
        hash: SHA-256 hash of this node's data.
        is_leaf: Whether this is a leaf node (file chunk).
        children: List of child nodes (empty for leaves).
        chunk_index: Index of the file chunk (only for leaf nodes).
        start_byte: Starting byte position in file (only for leaf nodes).
        end_byte: Ending byte position in file (only for leaf nodes).
    """
    hash: str
    is_leaf: bool
    children: List['MerkleNode'] = field(default_factory=list)
    chunk_index: Optional[int] = None
    start_byte: Optional[int] = None
    end_byte: Optional[int] = None
    
    def __eq__(self, other: object) -> bool:
        """Compare nodes by hash."""
        if not isinstance(other, MerkleNode):
            return False
        return self.hash == other.hash
    
    def __hash__(self) -> int:
        """Make node hashable by its hash value."""
        return hash(self.hash)


@dataclass
class TreeDiff:
    """
    Represents the difference between two Merkle trees.
    
    Attributes:
        changed_chunks: Set of chunk indices that differ between trees.
        chunk_details: Dictionary mapping chunk index to (start_byte, end_byte).
    """
    changed_chunks: Set[int] = field(default_factory=set)
    chunk_details: Dict[int, Tuple[int, int]] = field(default_factory=dict)
    
    def add_chunk(self, chunk_index: int, start_byte: int, end_byte: int) -> None:
        """Add a changed chunk to the diff."""
        self.changed_chunks.add(chunk_index)
        self.chunk_details[chunk_index] = (start_byte, end_byte)
    
    def is_empty(self) -> bool:
        """Check if there are no differences."""
        return len(self.changed_chunks) == 0
    
    def total_bytes(self) -> int:
        """Calculate total bytes that need to be transferred."""
        return sum(end - start for start, end in self.chunk_details.values())


class MerkleTree:
    """
    A Merkle tree for efficient file synchronization.
    
    Files are divided into fixed-size chunks (4KB). Each chunk is hashed to form
    a leaf node. Parent nodes are built by hashing the concatenation of children
    hashes. The root hash represents the entire file.
    
    This enables efficient diffing: if root hashes match, files are identical.
    Otherwise, we recursively traverse to find exactly which chunks differ.
    
    Attributes:
        root: The root node of the tree.
        file_path: Path to the file this tree represents.
        file_size: Total size of the file in bytes.
        chunk_size: Size of each chunk in bytes.
    
    Example:
        >>> tree1 = await MerkleTree.from_file("file1.txt")
        >>> tree2 = await MerkleTree.from_file("file2.txt")
        >>> diff = MerkleTree.get_diff(tree1, tree2)
        >>> print(f"Changed chunks: {diff.changed_chunks}")
    """
    
    def __init__(self, root: MerkleNode, file_path: Optional[str] = None, 
                 file_size: int = 0, chunk_size: int = CHUNK_SIZE):
        """
        Initialize a Merkle tree.
        
        Args:
            root: The root node of the tree.
            file_path: Optional path to the source file.
            file_size: Total size of the file.
            chunk_size: Size of each chunk.
        """
        self.root: MerkleNode = root
        self.file_path: Optional[str] = file_path
        self.file_size: int = file_size
        self.chunk_size: int = chunk_size
    
    @staticmethod
    def _compute_hash(data: bytes) -> str:
        """
        Compute SHA-256 hash of data.
        
        Args:
            data: Bytes to hash.
        
        Returns:
            Hexadecimal hash string.
        """
        return hashlib.sha256(data).hexdigest()
    
    @staticmethod
    async def _read_file_chunks(file_path: str, chunk_size: int = CHUNK_SIZE) -> List[Tuple[int, bytes, int, int]]:
        """
        Read a file in chunks asynchronously.
        
        Args:
            file_path: Path to the file to read.
            chunk_size: Size of each chunk in bytes.
        
        Returns:
            List of tuples: (chunk_index, chunk_data, start_byte, end_byte).
        """
        chunks = []
        chunk_index = 0
        byte_offset = 0
        
        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk_data = await f.read(chunk_size)
                if not chunk_data:
                    break
                
                start_byte = byte_offset
                end_byte = byte_offset + len(chunk_data)
                chunks.append((chunk_index, chunk_data, start_byte, end_byte))
                
                chunk_index += 1
                byte_offset = end_byte
        
        return chunks
    
    @classmethod
    async def from_file(cls, file_path: str, chunk_size: int = CHUNK_SIZE) -> 'MerkleTree':
        """
        Build a Merkle tree from a file asynchronously.
        
        This method:
        1. Reads the file in chunks
        2. Creates leaf nodes by hashing each chunk
        3. Builds parent nodes by hashing concatenated child hashes
        4. Continues until a single root node is created
        
        Args:
            file_path: Path to the file to build tree from.
            chunk_size: Size of each chunk in bytes.
        
        Returns:
            A new MerkleTree instance.
        
        Raises:
            FileNotFoundError: If the file doesn't exist.
            IOError: If there's an error reading the file.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_size = path.stat().st_size
        
        # Handle empty files
        if file_size == 0:
            empty_hash = cls._compute_hash(b"")
            root = MerkleNode(hash=empty_hash, is_leaf=True, chunk_index=0,
                            start_byte=0, end_byte=0)
            return cls(root, str(file_path), file_size, chunk_size)
        
        # Read file in chunks
        chunks = await cls._read_file_chunks(str(file_path), chunk_size)
        
        # Build leaf nodes
        leaf_nodes = []
        for chunk_index, chunk_data, start_byte, end_byte in chunks:
            chunk_hash = cls._compute_hash(chunk_data)
            leaf = MerkleNode(
                hash=chunk_hash,
                is_leaf=True,
                chunk_index=chunk_index,
                start_byte=start_byte,
                end_byte=end_byte
            )
            leaf_nodes.append(leaf)
        
        # Build tree bottom-up
        root = cls._build_tree(leaf_nodes)
        
        return cls(root, str(file_path), file_size, chunk_size)
    
    @staticmethod
    def _build_tree(nodes: List[MerkleNode]) -> MerkleNode:
        """
        Build a Merkle tree from a list of nodes (bottom-up construction).
        
        Args:
            nodes: List of nodes to build tree from.
        
        Returns:
            The root node of the constructed tree.
        """
        if len(nodes) == 0:
            raise ValueError("Cannot build tree from empty node list")
        
        if len(nodes) == 1:
            return nodes[0]
        
        # Build parent level
        parent_nodes = []
        for i in range(0, len(nodes), 2):
            left = nodes[i]
            right = nodes[i + 1] if i + 1 < len(nodes) else left
            
            # Hash of parent is hash of concatenated children hashes
            combined_hash = left.hash + right.hash
            parent_hash = MerkleTree._compute_hash(combined_hash.encode())
            
            parent = MerkleNode(
                hash=parent_hash,
                is_leaf=False,
                children=[left, right] if left != right else [left]
            )
            parent_nodes.append(parent)
        
        # Recursively build next level
        return MerkleTree._build_tree(parent_nodes)
    
    @staticmethod
    def get_diff(local_tree: 'MerkleTree', remote_tree: 'MerkleTree') -> TreeDiff:
        """
        Compute the difference between two Merkle trees.
        
        This method compares trees recursively:
        1. If root hashes match, trees are identical (return empty diff)
        2. If roots differ, recursively compare children
        3. When leaf nodes differ, record the chunk index
        
        This minimizes bandwidth by identifying exactly which chunks need transfer.
        
        Args:
            local_tree: The local version of the file.
            remote_tree: The remote version of the file.
        
        Returns:
            TreeDiff containing indices of chunks that differ.
        """
        diff = TreeDiff()
        
        # If roots are identical, no diff needed
        if local_tree.root.hash == remote_tree.root.hash:
            return diff
        
        # Recursively find differences
        MerkleTree._find_diff_recursive(local_tree.root, remote_tree.root, diff)
        
        return diff
    
    @staticmethod
    def _find_diff_recursive(local_node: MerkleNode, remote_node: MerkleNode, 
                            diff: TreeDiff) -> None:
        """
        Recursively traverse trees to find differing chunks.
        
        Args:
            local_node: Node from local tree.
            remote_node: Node from remote tree.
            diff: TreeDiff object to accumulate differences.
        """
        # If hashes match, no need to traverse further
        if local_node.hash == remote_node.hash:
            return
        
        # If both are leaf nodes and hashes differ, record the chunk
        if local_node.is_leaf and remote_node.is_leaf:
            if local_node.chunk_index is not None:
                diff.add_chunk(
                    local_node.chunk_index,
                    local_node.start_byte or 0,
                    local_node.end_byte or 0
                )
            return
        
        # If one is leaf and other is not, handle structural difference
        if local_node.is_leaf or remote_node.is_leaf:
            # This shouldn't happen in normal cases (same file, different content)
            # But we handle it by marking the chunk as different
            if local_node.is_leaf and local_node.chunk_index is not None:
                diff.add_chunk(
                    local_node.chunk_index,
                    local_node.start_byte or 0,
                    local_node.end_byte or 0
                )
            return
        
        # Recursively compare children
        # We compare corresponding children at same positions
        max_children = max(len(local_node.children), len(remote_node.children))
        
        for i in range(max_children):
            local_child = local_node.children[i] if i < len(local_node.children) else None
            remote_child = remote_node.children[i] if i < len(remote_node.children) else None
            
            if local_child and remote_child:
                MerkleTree._find_diff_recursive(local_child, remote_child, diff)
            elif local_child and local_child.is_leaf and local_child.chunk_index is not None:
                # Local has chunk that remote doesn't
                diff.add_chunk(
                    local_child.chunk_index,
                    local_child.start_byte or 0,
                    local_child.end_byte or 0
                )
    
    async def get_chunks(self, chunk_indices: Set[int]) -> Dict[int, bytes]:
        """
        Read specific chunks from the file asynchronously.
        
        Args:
            chunk_indices: Set of chunk indices to read.
        
        Returns:
            Dictionary mapping chunk index to chunk data.
        
        Raises:
            FileNotFoundError: If the file doesn't exist.
        """
        if not self.file_path:
            raise ValueError("Cannot read chunks: file_path not set")
        
        chunks = {}
        
        async with aiofiles.open(self.file_path, 'rb') as f:
            for chunk_index in sorted(chunk_indices):
                # Calculate byte offset for this chunk
                offset = chunk_index * self.chunk_size
                await f.seek(offset)
                chunk_data = await f.read(self.chunk_size)
                chunks[chunk_index] = chunk_data
        
        return chunks
    
    def to_dict(self) -> Dict:
        """
        Serialize the tree to a dictionary for transmission.
        
        Returns:
            Dictionary representation including root hash and metadata.
        """
        return {
            'root_hash': self.root.hash,
            'file_path': self.file_path,
            'file_size': self.file_size,
            'chunk_size': self.chunk_size,
            'root_node': self._node_to_dict(self.root)
        }
    
    @staticmethod
    def _node_to_dict(node: MerkleNode) -> Dict:
        """Serialize a node to dictionary."""
        return {
            'hash': node.hash,
            'is_leaf': node.is_leaf,
            'chunk_index': node.chunk_index,
            'start_byte': node.start_byte,
            'end_byte': node.end_byte,
            'children': [MerkleTree._node_to_dict(child) for child in node.children]
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'MerkleTree':
        """
        Deserialize a tree from a dictionary.
        
        Args:
            data: Dictionary containing tree data.
        
        Returns:
            A new MerkleTree instance.
        """
        root_node = cls._node_from_dict(data['root_node'])
        return cls(
            root=root_node,
            file_path=data.get('file_path'),
            file_size=data.get('file_size', 0),
            chunk_size=data.get('chunk_size', CHUNK_SIZE)
        )
    
    @staticmethod
    def _node_from_dict(data: Dict) -> MerkleNode:
        """Deserialize a node from dictionary."""
        children = [MerkleTree._node_from_dict(child) for child in data.get('children', [])]
        return MerkleNode(
            hash=data['hash'],
            is_leaf=data['is_leaf'],
            children=children,
            chunk_index=data.get('chunk_index'),
            start_byte=data.get('start_byte'),
            end_byte=data.get('end_byte')
        )
    
    def __str__(self) -> str:
        """String representation showing root hash."""
        return f"MerkleTree(root={self.root.hash[:16]}..., size={self.file_size})"
    
    def __repr__(self) -> str:
        """Detailed representation."""
        return f"MerkleTree(root_hash='{self.root.hash}', file='{self.file_path}', size={self.file_size})"
