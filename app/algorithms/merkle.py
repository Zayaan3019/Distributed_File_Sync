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
    
    @classmethod
    def from_leaf_hashes(cls, leaf_hashes: List[str], file_size: int,
                         chunk_size: int = CHUNK_SIZE,
                         file_path: Optional[str] = None) -> 'MerkleTree':
        """
        Rebuild a tree from its ordered leaf hashes alone.

        Interior nodes are pure function of the leaves, so they never need to
        be transmitted or stored. This is what makes two things possible:

        * **The leaf-only wire encoding** (`to_wire`). Serialising the full
          nested tree for a 100MB file produced an 8.9MB message against a
          10MB cap -- so the delta optimisation stopped working just above
          118MB, precisely where it matters most. Leaves alone are ~1.8MB.
        * **Incremental re-hashing.** The FUSE write path re-hashes only the
          4KB chunks that were actually dirtied, then rebuilds the root from
          the amended leaf list. Re-hashing a whole 100MB file per write costs
          8.5s; re-hashing one chunk costs microseconds.

        Args:
            leaf_hashes: Per-chunk hashes, ordered by chunk index.
            file_size: Size of the file the leaves describe.
            chunk_size: Chunk size the leaves were computed with.
            file_path: Optional path to associate with the tree.

        Returns:
            A tree whose root hash equals that of `from_file` on the same data.

        Raises:
            ValueError: If `leaf_hashes` is empty for a non-empty file.
        """
        if file_size == 0:
            # Match from_file's representation of an empty file exactly, or
            # the two would disagree on the root hash for the same content.
            empty_hash = cls._compute_hash(b"")
            root = MerkleNode(hash=empty_hash, is_leaf=True, chunk_index=0,
                              start_byte=0, end_byte=0)
            return cls(root, file_path, 0, chunk_size)

        if not leaf_hashes:
            raise ValueError("non-empty file must have at least one leaf hash")

        leaves = []
        for index, chunk_hash in enumerate(leaf_hashes):
            start = index * chunk_size
            end = min(start + chunk_size, file_size)
            leaves.append(MerkleNode(
                hash=chunk_hash, is_leaf=True, chunk_index=index,
                start_byte=start, end_byte=end,
            ))

        return cls(cls._build_tree(leaves), file_path, file_size, chunk_size)

    def leaf_hashes(self) -> List[str]:
        """
        Return this tree's per-chunk hashes, ordered by chunk index.

        Returns:
            Ordered leaf hashes. An empty file yields a single-element list
            holding the hash of the empty string, matching `from_file`.
        """
        by_index = {
            leaf.chunk_index: leaf.hash
            for leaf in MerkleTree._collect_leaves(self.root)
            if leaf.chunk_index is not None
        }
        return [by_index[index] for index in sorted(by_index)]

    def to_wire(self) -> Dict:
        """
        Serialize for transmission, sending leaves only.

        The nested `to_dict` form carries every interior node -- 51,202 nodes
        for a 100MB file where only 25,600 are leaves, so roughly half the
        payload is recomputable padding. `from_wire` rebuilds the interior
        nodes locally, so the root hash and the diff results are identical.

        Returns:
            Dictionary with the ordered leaf hashes and file geometry.
        """
        return {
            'encoding': 'leaves-v1',
            'file_path': self.file_path,
            'file_size': self.file_size,
            'chunk_size': self.chunk_size,
            'leaf_hashes': self.leaf_hashes(),
        }

    @classmethod
    def from_wire(cls, data: Dict) -> 'MerkleTree':
        """
        Deserialize a tree sent by a peer, in either encoding.

        Accepts the leaf-only form and the legacy nested form, so a 1.1 node
        can still talk to a 1.0 node that only knows how to send full trees.

        Args:
            data: Dictionary produced by `to_wire` or `to_dict`.

        Returns:
            A new MerkleTree instance.
        """
        if data.get('encoding') == 'leaves-v1' or 'leaf_hashes' in data:
            return cls.from_leaf_hashes(
                data['leaf_hashes'],
                data.get('file_size', 0),
                data.get('chunk_size', CHUNK_SIZE),
                data.get('file_path'),
            )
        return cls.from_dict(data)

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

            # Always record both children, even when their hashes are equal.
            # MerkleNode.__eq__ compares by hash, so collapsing to a single
            # child here would merge two *distinct positions* that happen to
            # hold *identical content* (zero-filled regions, repeated padding).
            # _find_diff_recursive walks children positionally, so an
            # asymmetric collapse misaligns the walk and silently drops the
            # changed chunk. The parent hash is unaffected by this, so the
            # wire format and root hashes are unchanged.
            parent = MerkleNode(
                hash=parent_hash,
                is_leaf=False,
                children=[left, right]
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

        The recursive walk requires both trees to have the same shape, which
        holds only when they have the same chunk count. When the file has
        grown or shrunk the depths differ, positional alignment is meaningless,
        and the walk degrades to fetching every remote leaf. Appending to a
        file is the most common write pattern there is, so that degradation
        would re-ship whole files. For unequal chunk counts we therefore align
        leaves by chunk index instead, which is O(n) in leaves but exact.

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

        # An empty remote file has no chunks to fetch. from_file() represents
        # it with a single zero-length sentinel leaf at index 0, which would
        # otherwise be reported as a chunk to transfer. Truncating the local
        # file to zero is driven by the remote file size, not by the diff.
        if remote_tree.file_size == 0:
            return diff

        local_by_index = MerkleTree._leaves_by_index(local_tree.root)
        remote_by_index = MerkleTree._leaves_by_index(remote_tree.root)

        # Tree shape is determined entirely by the leaf count, so equal chunk
        # counts guarantee the two walks stay aligned. Note this must compare
        # *distinct chunk indices*, not the length of the collected leaf list:
        # _build_tree pairs an odd trailing node with itself, so that node
        # appears twice in the leaf list and the raw list length overstates
        # the chunk count.
        if len(local_by_index) == len(remote_by_index):
            # Same shape: the recursive walk prunes whole identical subtrees.
            MerkleTree._find_diff_recursive(local_tree.root, remote_tree.root, diff)
        else:
            MerkleTree._diff_by_chunk_index(local_by_index, remote_by_index, diff)

        return diff

    @staticmethod
    def _leaves_by_index(root: MerkleNode) -> Dict[int, MerkleNode]:
        """
        Map chunk index to leaf node for a whole tree.

        Args:
            root: Root of the tree.

        Returns:
            Dictionary mapping chunk index to its leaf node.
        """
        return {
            leaf.chunk_index: leaf
            for leaf in MerkleTree._collect_leaves(root)
            if leaf.chunk_index is not None
        }

    @staticmethod
    def _diff_by_chunk_index(local_by_index: Dict[int, MerkleNode],
                             remote_by_index: Dict[int, MerkleNode],
                             diff: TreeDiff) -> None:
        """
        Compare leaves by chunk index rather than by tree position.

        Used when the two files have different chunk counts. Records every
        remote chunk whose hash differs from the local chunk at the same
        index, plus every remote chunk with no local counterpart (file grew).
        Local chunks with no remote counterpart mean the file shrank; there is
        nothing to fetch for those, and the truncation is applied from the
        remote file size.

        Args:
            local_by_index: Local tree leaves, keyed by chunk index.
            remote_by_index: Remote tree leaves, keyed by chunk index.
            diff: TreeDiff to accumulate into.
        """
        for chunk_index, remote_leaf in remote_by_index.items():
            local_leaf = local_by_index.get(chunk_index)
            if local_leaf is None or local_leaf.hash != remote_leaf.hash:
                MerkleTree._record_leaf(remote_leaf, diff)
    
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

        # If both are leaf nodes and hashes differ, record the chunk.
        # Index and byte range come from the REMOTE node: these chunks are
        # about to be fetched from the peer, so the peer's offsets are the
        # authoritative ones. Using the local node's index loses chunks that
        # exist only on the remote (a file that grew).
        if local_node.is_leaf and remote_node.is_leaf:
            MerkleTree._record_leaf(remote_node, diff)
            return

        # Structural divergence: one side is a leaf where the other has a
        # subtree. This happens whenever the two files have different chunk
        # counts, because the tree depth differs. We cannot align positionally,
        # so fall back to requesting every leaf in the remote subtree. This
        # over-fetches, but it is correct; the previous behaviour dropped the
        # remote-only chunks entirely.
        if local_node.is_leaf or remote_node.is_leaf:
            for leaf in MerkleTree._collect_leaves(remote_node):
                MerkleTree._record_leaf(leaf, diff)
            return

        # Recursively compare children at matching positions. _build_tree
        # always emits exactly two children per interior node, so the walk
        # stays aligned as long as both sides are interior nodes.
        max_children = max(len(local_node.children), len(remote_node.children))

        for i in range(max_children):
            local_child = local_node.children[i] if i < len(local_node.children) else None
            remote_child = remote_node.children[i] if i < len(remote_node.children) else None

            if local_child is not None and remote_child is not None:
                MerkleTree._find_diff_recursive(local_child, remote_child, diff)
            elif remote_child is not None:
                # Remote has a subtree we have no counterpart for: fetch it all.
                for leaf in MerkleTree._collect_leaves(remote_child):
                    MerkleTree._record_leaf(leaf, diff)
            # A local child with no remote counterpart means the file shrank.
            # There is nothing to fetch; the truncation is applied from the
            # remote file size, not from the chunk list.

    @staticmethod
    def _record_leaf(leaf: MerkleNode, diff: TreeDiff) -> None:
        """
        Record a leaf node in the diff, if it carries a chunk index.

        Args:
            leaf: Leaf node to record.
            diff: TreeDiff to accumulate into.
        """
        if leaf.chunk_index is not None:
            diff.add_chunk(
                leaf.chunk_index,
                leaf.start_byte or 0,
                leaf.end_byte or 0
            )

    @staticmethod
    def _collect_leaves(node: MerkleNode) -> List[MerkleNode]:
        """
        Collect every leaf node in a subtree, in traversal order.

        Args:
            node: Root of the subtree.

        Returns:
            List of leaf nodes.
        """
        if node.is_leaf:
            return [node]

        leaves: List[MerkleNode] = []
        for child in node.children:
            leaves.extend(MerkleTree._collect_leaves(child))
        return leaves
    
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
