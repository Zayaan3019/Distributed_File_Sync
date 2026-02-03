"""
Core Synchronization Engine.

This module implements the main synchronization logic:
- Computes Merkle trees for changed files
- Manages vector clocks for conflict detection
- Coordinates file chunk transfers
- Handles conflict resolution
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, Set, Optional
from dataclasses import dataclass, field
import aiofiles
import shutil

from ..algorithms.merkle import MerkleTree, TreeDiff, CHUNK_SIZE
from ..algorithms.vector_clock import VectorClock, ClockRelation
from ..core.config import SyncConfig
from ..core.protocol import Message, MessageType, Protocol
from ..services.network import NetworkManager, PeerConnection

logger = logging.getLogger(__name__)


@dataclass
class FileMetadata:
    """
    Metadata for a synchronized file.
    
    Attributes:
        file_path: Relative path to the file.
        merkle_tree: Merkle tree of the file.
        vector_clock: Vector clock for version tracking.
        last_modified: Timestamp of last modification.
    """
    file_path: str
    merkle_tree: MerkleTree
    vector_clock: VectorClock
    last_modified: float = field(default_factory=lambda: asyncio.get_event_loop().time())


class SyncEngine:
    """
    Main synchronization engine.
    
    This class orchestrates the entire synchronization process:
    1. Monitors local file changes via file monitor callbacks
    2. Computes Merkle trees and updates vector clocks
    3. Broadcasts sync requests to peers
    4. Handles incoming sync requests and resolves conflicts
    5. Transfers file chunks efficiently
    
    Attributes:
        config: Node configuration.
        network: Network manager for peer communication.
        file_metadata: Dictionary of file metadata indexed by relative path.
        pending_chunks: Tracks chunks being received for files.
    """
    
    def __init__(self, config: SyncConfig, network: NetworkManager):
        """
        Initialize the sync engine.
        
        Args:
            config: Node configuration.
            network: Network manager instance.
        """
        self.config = config
        self.network = network
        self.file_metadata: Dict[str, FileMetadata] = {}
        self.pending_chunks: Dict[str, Dict[int, bytes]] = {}
        self._sync_lock = asyncio.Lock()
        self._initializing = False
    
    def _get_absolute_path(self, relative_path: str) -> str:
        """
        Convert relative path to absolute path within sync directory.
        
        Args:
            relative_path: Relative file path.
        
        Returns:
            Absolute file path.
        """
        return str(Path(self.config.sync_directory) / relative_path)
    
    async def initialize(self) -> None:
        """
        Initialize the sync engine by scanning existing files.
        """
        logger.info("Initializing sync engine...")
        self._initializing = True
        
        try:
            sync_dir = Path(self.config.sync_directory)
            
            # Scan all files in sync directory
            for file_path in sync_dir.rglob('*'):
                if file_path.is_file() and not self._should_ignore(str(file_path)):
                    relative_path = str(file_path.relative_to(sync_dir))
                    try:
                        await self._process_local_file(relative_path, notify=False)
                    except Exception as e:
                        logger.error(f"Error initializing file {relative_path}: {e}")
            
            logger.info(f"Initialized {len(self.file_metadata)} files")
        finally:
            self._initializing = False
    
    def _should_ignore(self, file_path: str) -> bool:
        """Check if file should be ignored."""
        ignored_patterns = {'.tmp', '.temp', '.swp', '__pycache__', '.git', '_conflict'}
        return any(pattern in file_path for pattern in ignored_patterns)
    
    async def on_file_changed(self, relative_path: str) -> None:
        """
        Handle local file change event.
        
        Args:
            relative_path: Relative path to changed file.
        """
        logger.info(f"Processing file change: {relative_path}")
        
        try:
            await self._process_local_file(relative_path, notify=True)
        except Exception as e:
            logger.error(f"Error processing file change {relative_path}: {e}", exc_info=True)
    
    async def on_file_deleted(self, relative_path: str) -> None:
        """
        Handle local file deletion event.
        
        Args:
            relative_path: Relative path to deleted file.
        """
        logger.info(f"Processing file deletion: {relative_path}")
        
        try:
            async with self._sync_lock:
                if relative_path in self.file_metadata:
                    metadata = self.file_metadata[relative_path]
                    metadata.vector_clock.increment()
                    
                    # Broadcast deletion
                    if not self._initializing:
                        delete_msg = Message(
                            msg_type=MessageType.FILE_DELETE,
                            payload={
                                'file_path': relative_path,
                                'vector_clock': metadata.vector_clock.to_dict()
                            },
                            node_id=self.config.node_id
                        )
                        await self.network.broadcast(delete_msg)
                    
                    del self.file_metadata[relative_path]
                    logger.info(f"File {relative_path} deleted and synced")
        
        except Exception as e:
            logger.error(f"Error processing file deletion {relative_path}: {e}", exc_info=True)
    
    async def _process_local_file(self, relative_path: str, notify: bool = True) -> None:
        """
        Process a local file change (compute Merkle tree, update vector clock).
        
        Args:
            relative_path: Relative path to file.
            notify: Whether to notify peers of the change.
        """
        absolute_path = self._get_absolute_path(relative_path)
        
        if not Path(absolute_path).exists():
            logger.warning(f"File no longer exists: {relative_path}")
            return
        
        async with self._sync_lock:
            # Build Merkle tree
            merkle_tree = await MerkleTree.from_file(absolute_path, self.config.chunk_size)
            
            # Get or create vector clock
            if relative_path in self.file_metadata:
                metadata = self.file_metadata[relative_path]
                metadata.vector_clock.increment()
                metadata.merkle_tree = merkle_tree
                metadata.last_modified = asyncio.get_event_loop().time()
            else:
                vclock = VectorClock(self.config.node_id)
                vclock.increment()
                metadata = FileMetadata(
                    file_path=relative_path,
                    merkle_tree=merkle_tree,
                    vector_clock=vclock
                )
                self.file_metadata[relative_path] = metadata
            
            logger.info(f"File {relative_path} processed: hash={merkle_tree.root.hash[:16]}...")
            
            # Notify peers if not initializing
            if notify and not self._initializing:
                sync_req = Protocol.create_sync_request(
                    relative_path,
                    merkle_tree.root.hash,
                    metadata.vector_clock.to_dict(),
                    self.config.node_id
                )
                await self.network.broadcast(sync_req)
    
    async def handle_message(self, message: Message, peer_conn: PeerConnection) -> None:
        """
        Handle incoming message from a peer.
        
        Args:
            message: Received message.
            peer_conn: Connection to the peer.
        """
        try:
            if message.msg_type == MessageType.SYNC_REQ:
                await self._handle_sync_request(message, peer_conn)
            elif message.msg_type == MessageType.MERKLE_TREE:
                await self._handle_merkle_tree(message, peer_conn)
            elif message.msg_type == MessageType.CHUNK_REQ:
                await self._handle_chunk_request(message, peer_conn)
            elif message.msg_type == MessageType.CHUNK_DATA:
                await self._handle_chunk_data(message, peer_conn)
            elif message.msg_type == MessageType.FILE_DELETE:
                await self._handle_file_delete(message, peer_conn)
            else:
                logger.warning(f"Unhandled message type: {message.msg_type}")
        
        except Exception as e:
            logger.error(f"Error handling message {message.msg_type}: {e}", exc_info=True)
    
    async def _handle_sync_request(self, message: Message, peer_conn: PeerConnection) -> None:
        """
        Handle SYNC_REQ from a peer.
        
        Compare vector clocks and Merkle roots to decide action.
        """
        file_path = message.payload['file_path']
        remote_hash = message.payload['root_hash']
        remote_vclock = VectorClock.from_dict(message.payload['vector_clock'])
        
        logger.info(f"Received sync request for {file_path} from {peer_conn.peer_id}")
        
        async with self._sync_lock:
            local_metadata = self.file_metadata.get(file_path)
            
            if not local_metadata:
                # We don't have this file, request it
                logger.info(f"File {file_path} not found locally, requesting from peer")
                await self._request_full_file(file_path, peer_conn)
                return
            
            # Compare vector clocks
            relation = local_metadata.vector_clock.compare(remote_vclock)
            
            if relation == ClockRelation.EQUAL:
                # Same version
                if local_metadata.merkle_tree.root.hash != remote_hash:
                    logger.warning(f"Clock mismatch for {file_path}: same clock but different hash")
                else:
                    logger.debug(f"File {file_path} already in sync")
                return
            
            elif relation == ClockRelation.BEFORE:
                # Remote is newer, request update
                logger.info(f"Remote version of {file_path} is newer, requesting update")
                await self._request_merkle_tree(file_path, peer_conn)
                
                # Merge vector clocks
                local_metadata.vector_clock.merge(remote_vclock)
            
            elif relation == ClockRelation.AFTER:
                # Local is newer, send our version
                logger.info(f"Local version of {file_path} is newer, sending to peer")
                await self._send_merkle_tree(file_path, local_metadata, peer_conn)
            
            elif relation == ClockRelation.CONCURRENT:
                # Conflict! Handle it
                logger.warning(f"Conflict detected for {file_path}")
                await self._handle_conflict(file_path, local_metadata, remote_vclock, peer_conn)
    
    async def _request_full_file(self, file_path: str, peer_conn: PeerConnection) -> None:
        """Request complete file from peer."""
        request = Message(
            msg_type=MessageType.MERKLE_TREE,
            payload={'file_path': file_path, 'request': True},
            node_id=self.config.node_id
        )
        await Protocol.send_message(peer_conn.writer, request)
    
    async def _request_merkle_tree(self, file_path: str, peer_conn: PeerConnection) -> None:
        """Request Merkle tree from peer."""
        request = Message(
            msg_type=MessageType.MERKLE_TREE,
            payload={'file_path': file_path, 'request': True},
            node_id=self.config.node_id
        )
        await Protocol.send_message(peer_conn.writer, request)
    
    async def _send_merkle_tree(self, file_path: str, metadata: FileMetadata, 
                                peer_conn: PeerConnection) -> None:
        """Send Merkle tree to peer."""
        tree_msg = Message(
            msg_type=MessageType.MERKLE_TREE,
            payload={
                'file_path': file_path,
                'tree': metadata.merkle_tree.to_dict(),
                'vector_clock': metadata.vector_clock.to_dict()
            },
            node_id=self.config.node_id
        )
        await Protocol.send_message(peer_conn.writer, tree_msg)
    
    async def _handle_merkle_tree(self, message: Message, peer_conn: PeerConnection) -> None:
        """Handle received Merkle tree."""
        file_path = message.payload['file_path']
        
        # Check if this is a request for our tree
        if message.payload.get('request'):
            local_metadata = self.file_metadata.get(file_path)
            if local_metadata:
                await self._send_merkle_tree(file_path, local_metadata, peer_conn)
            return
        
        # Received tree from peer
        remote_tree = MerkleTree.from_dict(message.payload['tree'])
        remote_vclock = VectorClock.from_dict(message.payload['vector_clock'])
        
        logger.info(f"Received Merkle tree for {file_path}")
        
        local_metadata = self.file_metadata.get(file_path)
        
        if not local_metadata:
            # Request all chunks
            logger.info(f"Requesting all chunks for new file {file_path}")
            await self._request_all_chunks(file_path, remote_tree, peer_conn)
        else:
            # Compute diff
            diff = MerkleTree.get_diff(local_metadata.merkle_tree, remote_tree)
            
            if diff.is_empty():
                logger.info(f"No chunks to sync for {file_path}")
                # Update vector clock
                local_metadata.vector_clock.merge(remote_vclock)
            else:
                logger.info(f"Requesting {len(diff.changed_chunks)} chunks for {file_path}")
                await self._request_chunks(file_path, diff.changed_chunks, peer_conn)
    
    async def _request_all_chunks(self, file_path: str, tree: MerkleTree, 
                                  peer_conn: PeerConnection) -> None:
        """Request all chunks of a file."""
        # Calculate total chunks
        total_chunks = (tree.file_size + tree.chunk_size - 1) // tree.chunk_size
        chunk_indices = set(range(total_chunks))
        
        await self._request_chunks(file_path, chunk_indices, peer_conn)
    
    async def _request_chunks(self, file_path: str, chunk_indices: Set[int], 
                             peer_conn: PeerConnection) -> None:
        """Request specific chunks from peer."""
        # Initialize pending chunks buffer
        if file_path not in self.pending_chunks:
            self.pending_chunks[file_path] = {}
        
        chunk_req = Message(
            msg_type=MessageType.CHUNK_REQ,
            payload={
                'file_path': file_path,
                'chunk_indices': list(chunk_indices)
            },
            node_id=self.config.node_id
        )
        await Protocol.send_message(peer_conn.writer, chunk_req)
    
    async def _handle_chunk_request(self, message: Message, peer_conn: PeerConnection) -> None:
        """Handle request for file chunks."""
        file_path = message.payload['file_path']
        chunk_indices = set(message.payload['chunk_indices'])
        
        logger.info(f"Received chunk request for {file_path}: {len(chunk_indices)} chunks")
        
        local_metadata = self.file_metadata.get(file_path)
        if not local_metadata:
            logger.warning(f"Cannot send chunks for unknown file {file_path}")
            return
        
        # Read and send requested chunks
        try:
            chunks = await local_metadata.merkle_tree.get_chunks(chunk_indices)
            
            for chunk_index, chunk_data in chunks.items():
                await Protocol.send_chunk_data(
                    peer_conn.writer,
                    chunk_index,
                    chunk_data,
                    file_path,
                    self.config.node_id
                )
                logger.debug(f"Sent chunk {chunk_index} of {file_path}")
        
        except Exception as e:
            logger.error(f"Error sending chunks for {file_path}: {e}")
    
    async def _handle_chunk_data(self, message: Message, peer_conn: PeerConnection) -> None:
        """Handle received file chunk."""
        file_path, chunk_index, chunk_data = Protocol.decode_chunk_data(message)
        
        logger.debug(f"Received chunk {chunk_index} for {file_path}")
        
        # Store chunk
        if file_path not in self.pending_chunks:
            self.pending_chunks[file_path] = {}
        
        self.pending_chunks[file_path][chunk_index] = chunk_data
        
        # Check if we have all chunks (simple heuristic: no more expected)
        # In production, track expected chunks count
        # For now, write chunks as we receive them
        await self._write_chunks(file_path)
    
    async def _write_chunks(self, file_path: str) -> None:
        """Write received chunks to file."""
        if file_path not in self.pending_chunks:
            return
        
        chunks = self.pending_chunks[file_path]
        if not chunks:
            return
        
        absolute_path = self._get_absolute_path(file_path)
        
        try:
            # Ensure directory exists
            Path(absolute_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write chunks in order
            async with aiofiles.open(absolute_path, 'wb') as f:
                for chunk_index in sorted(chunks.keys()):
                    await f.seek(chunk_index * self.config.chunk_size)
                    await f.write(chunks[chunk_index])
            
            logger.info(f"Wrote {len(chunks)} chunks to {file_path}")
            
            # Clear pending chunks
            del self.pending_chunks[file_path]
            
            # Rebuild metadata
            await self._process_local_file(file_path, notify=False)
        
        except Exception as e:
            logger.error(f"Error writing chunks to {file_path}: {e}")
    
    async def _handle_file_delete(self, message: Message, peer_conn: PeerConnection) -> None:
        """Handle file deletion notification."""
        file_path = message.payload['file_path']
        remote_vclock = VectorClock.from_dict(message.payload['vector_clock'])
        
        logger.info(f"Received delete notification for {file_path}")
        
        local_metadata = self.file_metadata.get(file_path)
        
        if not local_metadata:
            logger.debug(f"File {file_path} already deleted locally")
            return
        
        # Check vector clock
        relation = local_metadata.vector_clock.compare(remote_vclock)
        
        if relation in [ClockRelation.BEFORE, ClockRelation.EQUAL]:
            # Remote delete is newer or same, delete local file
            absolute_path = self._get_absolute_path(file_path)
            try:
                Path(absolute_path).unlink(missing_ok=True)
                del self.file_metadata[file_path]
                logger.info(f"Deleted local file {file_path}")
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")
    
    async def _handle_conflict(self, file_path: str, local_metadata: FileMetadata,
                               remote_vclock: VectorClock, peer_conn: PeerConnection) -> None:
        """
        Handle concurrent edit conflict.
        
        Strategy: Last-Write-Wins with conflict backup
        """
        logger.warning(f"Resolving conflict for {file_path}")
        
        # Create conflict backup of local version
        absolute_path = self._get_absolute_path(file_path)
        conflict_path = f"{absolute_path}_conflict_{self.config.node_id}_{int(asyncio.get_event_loop().time())}"
        
        try:
            shutil.copy2(absolute_path, conflict_path)
            logger.info(f"Created conflict backup: {conflict_path}")
        except Exception as e:
            logger.error(f"Error creating conflict backup: {e}")
        
        # Accept remote version (could implement other strategies)
        await self._request_merkle_tree(file_path, peer_conn)
        
        # Merge vector clocks
        local_metadata.vector_clock.merge(remote_vclock)
