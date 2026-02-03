"""
Custom Binary Protocol for File Synchronization.

This module implements a length-prefixed binary protocol over TCP to handle
message framing and prevent TCP fragmentation issues. All messages are 
JSON-encoded with a 4-byte length header.
"""

import struct
import json
import asyncio
from typing import Dict, Any, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """
    Enumeration of all message types in the protocol.
    
    Message flow:
    1. HELLO: Initial handshake (node_id, version)
    2. HELLO_ACK: Acknowledge handshake
    3. SYNC_REQ: Request to sync a file (includes merkle root)
    4. SYNC_ACK: Acknowledge sync request
    5. MERKLE_TREE: Send full Merkle tree structure
    6. CHUNK_REQ: Request specific file chunks
    7. CHUNK_DATA: Send file chunk data
    8. VCLOCK_UPDATE: Send vector clock update
    9. FILE_DELETE: Notify file deletion
    10. HEARTBEAT: Keep-alive message
    11. ERROR: Error notification
    """
    HELLO = "HELLO"
    HELLO_ACK = "HELLO_ACK"
    SYNC_REQ = "SYNC_REQ"
    SYNC_ACK = "SYNC_ACK"
    MERKLE_TREE = "MERKLE_TREE"
    CHUNK_REQ = "CHUNK_REQ"
    CHUNK_DATA = "CHUNK_DATA"
    VCLOCK_UPDATE = "VCLOCK_UPDATE"
    FILE_DELETE = "FILE_DELETE"
    HEARTBEAT = "HEARTBEAT"
    ERROR = "ERROR"


@dataclass
class Message:
    """
    Represents a protocol message.
    
    Attributes:
        msg_type: Type of the message.
        payload: Message payload (dictionary).
        node_id: ID of the sending node.
    """
    msg_type: MessageType
    payload: Dict[str, Any]
    node_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary for serialization."""
        return {
            'type': self.msg_type.value,
            'payload': self.payload,
            'node_id': self.node_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """
        Create message from dictionary.
        
        Args:
            data: Dictionary containing message data.
        
        Returns:
            New Message instance.
        """
        return cls(
            msg_type=MessageType(data['type']),
            payload=data['payload'],
            node_id=data.get('node_id')
        )
    
    def to_json(self) -> str:
        """Serialize message to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """Deserialize message from JSON string."""
        return cls.from_dict(json.loads(json_str))


class Protocol:
    """
    Binary protocol handler for file synchronization.
    
    Uses length-prefixed framing to handle TCP stream fragmentation:
    - First 4 bytes: Message length (unsigned int, network byte order)
    - Remaining bytes: JSON-encoded message
    
    This ensures complete messages are received even over unreliable networks.
    """
    
    # Protocol version for compatibility checking
    VERSION = "1.0"
    
    # Maximum message size (10 MB)
    MAX_MESSAGE_SIZE = 10 * 1024 * 1024
    
    # Length prefix format (4 bytes, unsigned int, network byte order)
    LENGTH_PREFIX_FORMAT = '!I'
    LENGTH_PREFIX_SIZE = struct.calcsize(LENGTH_PREFIX_FORMAT)
    
    @staticmethod
    async def send_message(writer: asyncio.StreamWriter, message: Message) -> None:
        """
        Send a message over an async stream with length-prefixing.
        
        Message format:
        [4 bytes: length][N bytes: JSON message]
        
        Args:
            writer: AsyncIO stream writer.
            message: Message to send.
        
        Raises:
            ValueError: If message is too large.
            ConnectionError: If connection is closed.
        """
        try:
            # Serialize message to JSON
            json_data = message.to_json()
            message_bytes = json_data.encode('utf-8')
            
            # Check message size
            if len(message_bytes) > Protocol.MAX_MESSAGE_SIZE:
                raise ValueError(f"Message too large: {len(message_bytes)} bytes")
            
            # Pack length prefix (4 bytes, network byte order)
            length_prefix = struct.pack(Protocol.LENGTH_PREFIX_FORMAT, len(message_bytes))
            
            # Send length prefix followed by message
            writer.write(length_prefix + message_bytes)
            await writer.drain()
            
            logger.debug(f"Sent message: type={message.msg_type.value}, size={len(message_bytes)}")
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise
    
    @staticmethod
    async def receive_message(reader: asyncio.StreamReader) -> Optional[Message]:
        """
        Receive a length-prefixed message from an async stream.
        
        This method:
        1. Reads 4 bytes to get message length
        2. Reads exactly that many bytes for the message
        3. Deserializes JSON to Message object
        
        Args:
            reader: AsyncIO stream reader.
        
        Returns:
            Received Message, or None if connection closed.
        
        Raises:
            ValueError: If message format is invalid or too large.
            ConnectionError: If connection is closed unexpectedly.
        """
        try:
            # Read length prefix (4 bytes)
            length_data = await reader.readexactly(Protocol.LENGTH_PREFIX_SIZE)
            
            # Unpack length (network byte order)
            message_length = struct.unpack(Protocol.LENGTH_PREFIX_FORMAT, length_data)[0]
            
            # Validate message length
            if message_length > Protocol.MAX_MESSAGE_SIZE:
                raise ValueError(f"Message too large: {message_length} bytes")
            
            if message_length == 0:
                raise ValueError("Invalid message length: 0")
            
            # Read exact message bytes
            message_bytes = await reader.readexactly(message_length)
            
            # Deserialize JSON to Message
            json_str = message_bytes.decode('utf-8')
            message = Message.from_json(json_str)
            
            logger.debug(f"Received message: type={message.msg_type.value}, size={message_length}")
            
            return message
            
        except asyncio.IncompleteReadError:
            # Connection closed
            logger.info("Connection closed by peer")
            return None
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            raise
    
    @staticmethod
    async def send_chunk_data(writer: asyncio.StreamWriter, chunk_index: int, 
                             chunk_data: bytes, file_path: str, node_id: str) -> None:
        """
        Send a file chunk using the protocol.
        
        For efficiency, chunk data is base64-encoded within JSON.
        For production, consider a hybrid protocol with binary chunk transfer.
        
        Args:
            writer: AsyncIO stream writer.
            chunk_index: Index of the chunk.
            chunk_data: Raw chunk bytes.
            file_path: Path to the file.
            node_id: ID of sending node.
        """
        import base64
        
        # Encode chunk data as base64 for JSON transmission
        chunk_b64 = base64.b64encode(chunk_data).decode('ascii')
        
        message = Message(
            msg_type=MessageType.CHUNK_DATA,
            payload={
                'file_path': file_path,
                'chunk_index': chunk_index,
                'chunk_data': chunk_b64,
                'chunk_size': len(chunk_data)
            },
            node_id=node_id
        )
        
        await Protocol.send_message(writer, message)
    
    @staticmethod
    def decode_chunk_data(message: Message) -> Tuple[str, int, bytes]:
        """
        Decode chunk data from a CHUNK_DATA message.
        
        Args:
            message: Message containing chunk data.
        
        Returns:
            Tuple of (file_path, chunk_index, chunk_data).
        """
        import base64
        
        if message.msg_type != MessageType.CHUNK_DATA:
            raise ValueError(f"Expected CHUNK_DATA, got {message.msg_type}")
        
        file_path = message.payload['file_path']
        chunk_index = message.payload['chunk_index']
        chunk_b64 = message.payload['chunk_data']
        chunk_data = base64.b64decode(chunk_b64)
        
        return file_path, chunk_index, chunk_data
    
    @staticmethod
    def create_hello_message(node_id: str) -> Message:
        """
        Create a HELLO handshake message.
        
        Args:
            node_id: ID of this node.
        
        Returns:
            HELLO message.
        """
        return Message(
            msg_type=MessageType.HELLO,
            payload={
                'version': Protocol.VERSION,
                'timestamp': asyncio.get_event_loop().time()
            },
            node_id=node_id
        )
    
    @staticmethod
    def create_sync_request(file_path: str, root_hash: str, vclock: Dict, node_id: str) -> Message:
        """
        Create a SYNC_REQ message.
        
        Args:
            file_path: Path to the file to sync.
            root_hash: Merkle tree root hash.
            vclock: Vector clock as dictionary.
            node_id: ID of this node.
        
        Returns:
            SYNC_REQ message.
        """
        return Message(
            msg_type=MessageType.SYNC_REQ,
            payload={
                'file_path': file_path,
                'root_hash': root_hash,
                'vector_clock': vclock,
                'timestamp': asyncio.get_event_loop().time()
            },
            node_id=node_id
        )
    
    @staticmethod
    def create_error_message(error_msg: str, node_id: str) -> Message:
        """
        Create an ERROR message.
        
        Args:
            error_msg: Error description.
            node_id: ID of this node.
        
        Returns:
            ERROR message.
        """
        return Message(
            msg_type=MessageType.ERROR,
            payload={
                'error': error_msg,
                'timestamp': asyncio.get_event_loop().time()
            },
            node_id=node_id
        )
    
    @staticmethod
    def create_heartbeat(node_id: str) -> Message:
        """
        Create a HEARTBEAT keep-alive message.
        
        Args:
            node_id: ID of this node.
        
        Returns:
            HEARTBEAT message.
        """
        return Message(
            msg_type=MessageType.HEARTBEAT,
            payload={
                'timestamp': asyncio.get_event_loop().time()
            },
            node_id=node_id
        )
