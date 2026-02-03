"""
Async Network Layer for Distributed File Synchronization.

This module implements the TCP server and client for peer-to-peer communication
using asyncio. It handles connection management, message routing, and implements
exponential backoff for retries.
"""

import asyncio
import logging
from typing import Dict, Optional, Set, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime

from ..core.config import SyncConfig, PeerConfig
from ..core.protocol import Protocol, Message, MessageType

logger = logging.getLogger(__name__)


@dataclass
class PeerConnection:
    """
    Represents an active connection to a peer.
    
    Attributes:
        peer_id: Unique identifier of the peer.
        host: Peer's host address.
        port: Peer's port number.
        reader: AsyncIO stream reader.
        writer: AsyncIO stream writer.
        connected_at: Timestamp of connection establishment.
        last_heartbeat: Timestamp of last heartbeat received.
    """
    peer_id: str
    host: str
    port: int
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    connected_at: float = field(default_factory=lambda: asyncio.get_event_loop().time())
    last_heartbeat: float = field(default_factory=lambda: asyncio.get_event_loop().time())
    
    def address(self) -> str:
        """Get peer address as string."""
        return f"{self.host}:{self.port}"
    
    async def close(self) -> None:
        """Close the connection gracefully."""
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception as e:
            logger.warning(f"Error closing connection to {self.address()}: {e}")


class NetworkManager:
    """
    Manages all network connections and communication for a node.
    
    This class:
    - Runs a TCP server to accept incoming peer connections
    - Maintains outgoing connections to known peers
    - Routes messages to appropriate handlers
    - Implements exponential backoff for failed connections
    - Sends periodic heartbeats to maintain connections
    
    Attributes:
        config: Node configuration.
        message_handler: Async callback for handling received messages.
        connections: Dictionary of active peer connections.
        server: The running TCP server.
    """
    
    def __init__(self, config: SyncConfig, 
                 message_handler: Callable[[Message, PeerConnection], Awaitable[None]]):
        """
        Initialize the network manager.
        
        Args:
            config: Node configuration.
            message_handler: Async function to handle received messages.
        """
        self.config = config
        self.message_handler = message_handler
        self.connections: Dict[str, PeerConnection] = {}
        self.server: Optional[asyncio.Server] = None
        self._running = False
        self._tasks: Set[asyncio.Task] = set()
        self._retry_delays: Dict[str, float] = {}  # Track retry delays per peer
    
    async def start(self) -> None:
        """
        Start the network manager (server and client connections).
        """
        logger.info(f"Starting network manager on {self.config.listen_host}:{self.config.listen_port}")
        self._running = True
        
        # Start TCP server
        self.server = await asyncio.start_server(
            self._handle_client,
            self.config.listen_host,
            self.config.listen_port
        )
        
        logger.info(f"Server listening on {self.config.listen_host}:{self.config.listen_port}")
        
        # Connect to known peers
        for peer in self.config.peers:
            task = asyncio.create_task(self._connect_to_peer(peer))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        
        # Start heartbeat sender
        task = asyncio.create_task(self._heartbeat_loop())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
    
    async def stop(self) -> None:
        """
        Stop the network manager and close all connections.
        """
        logger.info("Stopping network manager")
        self._running = False
        
        # Cancel all running tasks
        for task in self._tasks:
            task.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close all peer connections
        for conn in list(self.connections.values()):
            await conn.close()
        
        self.connections.clear()
        
        # Stop server
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        logger.info("Network manager stopped")
    
    async def _handle_client(self, reader: asyncio.StreamReader, 
                            writer: asyncio.StreamWriter) -> None:
        """
        Handle an incoming client connection.
        
        Args:
            reader: AsyncIO stream reader.
            writer: AsyncIO stream writer.
        """
        peer_addr = writer.get_extra_info('peername')
        logger.info(f"New connection from {peer_addr}")
        
        peer_conn: Optional[PeerConnection] = None
        
        try:
            # First message should be HELLO
            message = await Protocol.receive_message(reader)
            
            if not message or message.msg_type != MessageType.HELLO:
                logger.warning(f"Expected HELLO from {peer_addr}, got {message.msg_type if message else 'None'}")
                return
            
            peer_id = message.node_id
            if not peer_id:
                logger.warning(f"HELLO message missing node_id from {peer_addr}")
                return
            
            # Create peer connection
            peer_conn = PeerConnection(
                peer_id=peer_id,
                host=peer_addr[0],
                port=peer_addr[1],
                reader=reader,
                writer=writer
            )
            
            # Store connection
            self.connections[peer_id] = peer_conn
            logger.info(f"Peer {peer_id} connected from {peer_addr}")
            
            # Send HELLO_ACK
            hello_ack = Message(
                msg_type=MessageType.HELLO_ACK,
                payload={'version': Protocol.VERSION},
                node_id=self.config.node_id
            )
            await Protocol.send_message(writer, hello_ack)
            
            # Handle messages from this peer
            await self._message_loop(peer_conn)
            
        except asyncio.CancelledError:
            logger.info(f"Connection handler cancelled for {peer_addr}")
        except Exception as e:
            logger.error(f"Error handling client {peer_addr}: {e}", exc_info=True)
        finally:
            if peer_conn and peer_conn.peer_id in self.connections:
                del self.connections[peer_conn.peer_id]
                await peer_conn.close()
                logger.info(f"Peer {peer_conn.peer_id} disconnected")
    
    async def _connect_to_peer(self, peer: PeerConfig) -> None:
        """
        Connect to a peer with exponential backoff retry.
        
        Args:
            peer: Peer configuration.
        """
        peer_key = f"{peer.host}:{peer.port}"
        retry_delay = self._retry_delays.get(peer_key, self.config.retry_delay)
        retries = 0
        
        while self._running and retries < self.config.max_retries:
            try:
                logger.info(f"Connecting to peer at {peer_key}")
                
                # Open connection
                reader, writer = await asyncio.open_connection(peer.host, peer.port)
                
                # Send HELLO
                hello_msg = Protocol.create_hello_message(self.config.node_id)
                await Protocol.send_message(writer, hello_msg)
                
                # Wait for HELLO_ACK
                response = await asyncio.wait_for(
                    Protocol.receive_message(reader),
                    timeout=10.0
                )
                
                if not response or response.msg_type != MessageType.HELLO_ACK:
                    logger.warning(f"Invalid handshake response from {peer_key}")
                    writer.close()
                    await writer.wait_closed()
                    raise ConnectionError("Invalid handshake")
                
                peer_id = response.node_id
                if not peer_id:
                    raise ConnectionError("Peer did not provide node_id")
                
                # Create connection
                peer_conn = PeerConnection(
                    peer_id=peer_id,
                    host=peer.host,
                    port=peer.port,
                    reader=reader,
                    writer=writer
                )
                
                self.connections[peer_id] = peer_conn
                logger.info(f"Connected to peer {peer_id} at {peer_key}")
                
                # Reset retry delay on success
                self._retry_delays[peer_key] = self.config.retry_delay
                
                # Handle messages from this peer
                await self._message_loop(peer_conn)
                
                break  # Exit retry loop on success
                
            except (ConnectionRefusedError, asyncio.TimeoutError, OSError) as e:
                retries += 1
                logger.warning(f"Failed to connect to {peer_key} (attempt {retries}/{self.config.max_retries}): {e}")
                
                if retries < self.config.max_retries:
                    # Exponential backoff
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60.0)  # Cap at 60 seconds
                    self._retry_delays[peer_key] = retry_delay
                else:
                    logger.error(f"Failed to connect to {peer_key} after {self.config.max_retries} attempts")
            
            except asyncio.CancelledError:
                logger.info(f"Connection attempt to {peer_key} cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error connecting to {peer_key}: {e}", exc_info=True)
                await asyncio.sleep(retry_delay)
    
    async def _message_loop(self, peer_conn: PeerConnection) -> None:
        """
        Continuously receive and handle messages from a peer.
        
        Args:
            peer_conn: The peer connection.
        """
        try:
            while self._running:
                message = await Protocol.receive_message(peer_conn.reader)
                
                if not message:
                    # Connection closed
                    logger.info(f"Connection closed by peer {peer_conn.peer_id}")
                    break
                
                # Update heartbeat timestamp
                if message.msg_type == MessageType.HEARTBEAT:
                    peer_conn.last_heartbeat = asyncio.get_event_loop().time()
                    logger.debug(f"Heartbeat from {peer_conn.peer_id}")
                    continue
                
                # Route message to handler
                try:
                    await self.message_handler(message, peer_conn)
                except Exception as e:
                    logger.error(f"Error handling message from {peer_conn.peer_id}: {e}", exc_info=True)
                    
                    # Send error response
                    error_msg = Protocol.create_error_message(str(e), self.config.node_id)
                    await Protocol.send_message(peer_conn.writer, error_msg)
        
        except asyncio.CancelledError:
            logger.info(f"Message loop cancelled for {peer_conn.peer_id}")
        except Exception as e:
            logger.error(f"Error in message loop for {peer_conn.peer_id}: {e}", exc_info=True)
        finally:
            if peer_conn.peer_id in self.connections:
                del self.connections[peer_conn.peer_id]
            await peer_conn.close()
    
    async def _heartbeat_loop(self) -> None:
        """
        Periodically send heartbeats to all connected peers.
        """
        try:
            while self._running:
                await asyncio.sleep(self.config.heartbeat_interval)
                
                # Send heartbeat to all peers
                for peer_id, conn in list(self.connections.items()):
                    try:
                        heartbeat = Protocol.create_heartbeat(self.config.node_id)
                        await Protocol.send_message(conn.writer, heartbeat)
                        logger.debug(f"Sent heartbeat to {peer_id}")
                    except Exception as e:
                        logger.error(f"Failed to send heartbeat to {peer_id}: {e}")
        
        except asyncio.CancelledError:
            logger.info("Heartbeat loop cancelled")
    
    async def broadcast(self, message: Message, exclude_peer: Optional[str] = None) -> None:
        """
        Broadcast a message to all connected peers.
        
        Args:
            message: Message to broadcast.
            exclude_peer: Optional peer ID to exclude from broadcast.
        """
        for peer_id, conn in list(self.connections.items()):
            if peer_id == exclude_peer:
                continue
            
            try:
                await Protocol.send_message(conn.writer, message)
                logger.debug(f"Broadcasted {message.msg_type.value} to {peer_id}")
            except Exception as e:
                logger.error(f"Failed to broadcast to {peer_id}: {e}")
    
    async def send_to_peer(self, peer_id: str, message: Message) -> bool:
        """
        Send a message to a specific peer.
        
        Args:
            peer_id: ID of the target peer.
            message: Message to send.
        
        Returns:
            True if message was sent successfully, False otherwise.
        """
        conn = self.connections.get(peer_id)
        if not conn:
            logger.warning(f"Peer {peer_id} not connected")
            return False
        
        try:
            await Protocol.send_message(conn.writer, message)
            logger.debug(f"Sent {message.msg_type.value} to {peer_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send to {peer_id}: {e}")
            return False
    
    def get_connected_peers(self) -> Set[str]:
        """
        Get set of currently connected peer IDs.
        
        Returns:
            Set of peer IDs.
        """
        return set(self.connections.keys())
    
    def is_connected(self, peer_id: str) -> bool:
        """
        Check if a peer is currently connected.
        
        Args:
            peer_id: ID of the peer to check.
        
        Returns:
            True if peer is connected, False otherwise.
        """
        return peer_id in self.connections
