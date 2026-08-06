"""
Custom Binary Protocol for File Synchronization.

This module implements a length-prefixed binary protocol over TCP to handle
message framing and prevent TCP fragmentation issues. All messages are 
JSON-encoded with a 4-byte length header.
"""

import struct
import json
import time
import asyncio

# Wire timestamps use time.time(), not asyncio.get_event_loop().time().
# The loop clock is a per-process monotonic counter measured from an arbitrary
# origin, so it is meaningless to the peer receiving it, and calling it outside
# a running loop raises RuntimeError on Linux -- which made every message
# constructor untestable there.
from typing import Dict, Any, Optional, Tuple, Protocol as TypingProtocol
from enum import Enum
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


class UnknownMessageType(Exception):
    """
    Raised when a message names a type this build does not implement.

    This exists so that a newer peer sending a message we do not understand is
    a *skippable frame*, not a dropped connection. Before this, `MessageType(...)`
    raised ValueError straight out of `receive_message`, the network layer's
    message loop caught it and tore the connection down, and there was no way
    to add a message type without a flag-day upgrade of every peer.
    """

    def __init__(self, type_name: str):
        super().__init__(f"unknown message type: {type_name!r}")
        self.type_name = type_name


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
    # Added in protocol 1.1. Without it, a rename maps to delete(old) +
    # create(new), which re-transmits the entire file -- renaming a 100MB file
    # would ship 100MB and destroy the exact Merkle-delta benefit this project
    # exists to demonstrate. It also loses the path's causal history.
    # Only sent to peers that advertised >= 1.1 in HELLO; older peers still get
    # delete + create. See docs/FUSE_DESIGN.md section 6.
    FILE_RENAME = "FILE_RENAME"


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
        raw_type = data.get('type')
        try:
            msg_type = MessageType(raw_type)
        except ValueError:
            raise UnknownMessageType(str(raw_type))

        payload = data.get('payload')
        if not isinstance(payload, dict):
            raise ValueError("message payload must be an object")

        return cls(
            msg_type=msg_type,
            payload=payload,
            node_id=data.get('node_id')
        )
    
    def to_json(self) -> str:
        """Serialize message to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """Deserialize message from JSON string."""
        return cls.from_dict(json.loads(json_str))


class Channel(TypingProtocol):
    """
    The byte-frame transport `Protocol` sits on.

    Two implementations exist: `PlainChannel` (length-prefixed cleartext, the
    original behaviour, retained for explicitly-insecure local runs) and
    `app.core.security.SecureChannel` (AES-256-GCM, the default). Making this
    an interface is what let authentication and encryption be added without
    touching a single message handler.
    """

    async def send(self, plaintext: bytes) -> None:
        """Write one frame."""
        ...

    async def receive(self) -> Optional[bytes]:
        """Read one frame, or None if the peer closed the connection."""
        ...


class PlainChannel:
    """
    Unencrypted length-prefixed framing.

    Wire format: ``[4-byte big-endian length][body]``, which is exactly the
    original protocol. Used only when the operator passes ``--allow-insecure``;
    the default path is `SecureChannel`.

    Attributes:
        reader: Underlying stream reader.
        writer: Underlying stream writer.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Initialize the channel.

        Args:
            reader: Stream to read frames from.
            writer: Stream to write frames to.
        """
        self.reader = reader
        self.writer = writer
        self._send_lock = asyncio.Lock()

    async def send(self, plaintext: bytes) -> None:
        """
        Write one length-prefixed frame.

        Args:
            plaintext: Frame body.

        Raises:
            ValueError: If the frame exceeds the protocol maximum.
        """
        if len(plaintext) > Protocol.MAX_MESSAGE_SIZE:
            raise ValueError(f"Message too large: {len(plaintext)} bytes")

        async with self._send_lock:
            self.writer.write(
                struct.pack(Protocol.LENGTH_PREFIX_FORMAT, len(plaintext)) + plaintext
            )
            await self.writer.drain()

    async def receive(self) -> Optional[bytes]:
        """
        Read one length-prefixed frame.

        Returns:
            The frame body, or None if the peer closed the connection.

        Raises:
            ValueError: If the declared length is zero or exceeds the maximum.
        """
        try:
            header = await self.reader.readexactly(Protocol.LENGTH_PREFIX_SIZE)
        except asyncio.IncompleteReadError:
            return None

        length = struct.unpack(Protocol.LENGTH_PREFIX_FORMAT, header)[0]

        # Bound the allocation *before* reading the body. An attacker-controlled
        # prefix is otherwise a one-packet memory-exhaustion DoS.
        if length > Protocol.MAX_MESSAGE_SIZE:
            raise ValueError(f"Message too large: {length} bytes")
        if length == 0:
            raise ValueError("Invalid message length: 0")

        try:
            return await self.reader.readexactly(length)
        except asyncio.IncompleteReadError:
            return None


class Protocol:
    """
    Binary protocol handler for file synchronization.

    Uses length-prefixed framing to handle TCP stream fragmentation:
    - First 4 bytes: Message length (unsigned int, network byte order)
    - Remaining bytes: JSON-encoded message

    This ensures complete messages are received even over unreliable networks.
    """

    # Protocol version for compatibility checking.
    # 1.0 -> 1.1 adds FILE_RENAME and tolerant decoding of unknown message
    # types. A 1.1 node only sends FILE_RENAME to peers that advertised >= 1.1
    # in the handshake, so mixed-version clusters keep working.
    VERSION = "1.1"

    #: Lowest peer version that understands FILE_RENAME.
    RENAME_MIN_VERSION = (1, 1)

    # Maximum message size (10 MB)
    MAX_MESSAGE_SIZE = 10 * 1024 * 1024
    
    # Length prefix format (4 bytes, unsigned int, network byte order)
    LENGTH_PREFIX_FORMAT = '!I'
    LENGTH_PREFIX_SIZE = struct.calcsize(LENGTH_PREFIX_FORMAT)
    
    @staticmethod
    def _as_send_channel(target) -> "Channel":
        """
        Coerce a raw stream writer into a channel.

        Callers inside the application always hold a real `Channel`. Tests and
        the handshake path hold a bare `StreamWriter`; wrapping it here keeps
        one code path for both instead of duplicating the framing logic.

        Args:
            target: A `Channel`, or an `asyncio.StreamWriter`.

        Returns:
            Something with an awaitable `send`.
        """
        if hasattr(target, "send"):
            return target
        return PlainChannel(None, target)

    @staticmethod
    def _as_recv_channel(source) -> "Channel":
        """
        Coerce a raw stream reader into a channel.

        Args:
            source: A `Channel`, or an `asyncio.StreamReader`.

        Returns:
            Something with an awaitable `receive`.
        """
        if hasattr(source, "receive"):
            return source
        return PlainChannel(source, None)

    @staticmethod
    async def send_message(target, message: Message) -> None:
        """
        Send a message over a channel (or a raw stream writer).

        Message format on a `PlainChannel`:
        [4 bytes: length][N bytes: JSON message]. On a `SecureChannel` the same
        JSON body is AES-256-GCM sealed; this method does not care which.

        Args:
            target: `Channel` or `asyncio.StreamWriter`.
            message: Message to send.

        Raises:
            ValueError: If message is too large.
            ConnectionError: If connection is closed.
        """
        try:
            message_bytes = message.to_json().encode('utf-8')

            if len(message_bytes) > Protocol.MAX_MESSAGE_SIZE:
                raise ValueError(f"Message too large: {len(message_bytes)} bytes")

            await Protocol._as_send_channel(target).send(message_bytes)

            logger.debug(
                f"Sent message: type={message.msg_type.value}, size={len(message_bytes)}"
            )

        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise

    @staticmethod
    async def receive_message(source) -> Optional[Message]:
        """
        Receive one message from a channel (or a raw stream reader).

        Unknown message types are logged and skipped rather than raised, so a
        peer running a newer build cannot kill the connection simply by using a
        message type we have not heard of. Every other decode failure is still
        fatal to the connection, because it means the stream is desynchronised
        or tampered with and there is no safe way to continue.

        Args:
            source: `Channel` or `asyncio.StreamReader`.

        Returns:
            Received Message, or None if connection closed.

        Raises:
            ValueError: If message format is invalid or too large.
        """
        channel = Protocol._as_recv_channel(source)

        while True:
            try:
                body = await channel.receive()
            except asyncio.IncompleteReadError:
                logger.info("Connection closed by peer")
                return None

            if body is None:
                logger.info("Connection closed by peer")
                return None

            try:
                message = Message.from_json(body.decode('utf-8'))
            except UnknownMessageType as unknown:
                # Forward compatibility: skip the frame, keep the connection.
                logger.warning(
                    f"Ignoring frame with unsupported type {unknown.type_name!r} "
                    f"(this node speaks protocol {Protocol.VERSION})"
                )
                continue
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
                raise

            logger.debug(
                f"Received message: type={message.msg_type.value}, size={len(body)}"
            )
            return message

    @staticmethod
    def parse_version(text: Optional[str]) -> Tuple[int, int]:
        """
        Parse a dotted protocol version into a comparable tuple.

        Args:
            text: Version string such as ``"1.1"``. Missing or malformed
                values are treated as the oldest version, which is the safe
                direction: we fall back to behaviour every peer understands.

        Returns:
            Tuple of (major, minor).
        """
        if not text:
            return (1, 0)
        try:
            major, _, minor = str(text).partition(".")
            return (int(major), int(minor or 0))
        except ValueError:
            return (1, 0)

    @staticmethod
    async def send_chunk_data(writer, chunk_index: int,
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

        # Sanitise here as well as at the apply site. This is the function that
        # turns a wire message into a (path, bytes) pair, so it is the natural
        # chokepoint, and defence in depth is cheap: the Phase 0 probe wrote
        # attacker bytes to '../VICTIM.txt' through exactly this path.
        from .security import sanitize_relative_path

        file_path = sanitize_relative_path(message.payload['file_path'])

        chunk_index = message.payload['chunk_index']
        if not isinstance(chunk_index, int) or chunk_index < 0:
            raise ValueError(f"Invalid chunk index: {chunk_index!r}")

        chunk_data = base64.b64decode(message.payload['chunk_data'], validate=True)

        return file_path, chunk_index, chunk_data

    @staticmethod
    def create_rename_message(old_path: str, new_path: str, vclock: Dict,
                              node_id: str) -> Message:
        """
        Create a FILE_RENAME message (protocol >= 1.1).

        Args:
            old_path: Source path, relative to the sync root.
            new_path: Destination path, relative to the sync root.
            vclock: Vector clock as a dictionary.
            node_id: ID of this node.

        Returns:
            FILE_RENAME message.
        """
        return Message(
            msg_type=MessageType.FILE_RENAME,
            payload={
                'old_path': old_path,
                'new_path': new_path,
                'vector_clock': vclock,
                'timestamp': time.time()
            },
            node_id=node_id
        )
    
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
                'timestamp': time.time()
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
                'timestamp': time.time()
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
                'timestamp': time.time()
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
                'timestamp': time.time()
            },
            node_id=node_id
        )
