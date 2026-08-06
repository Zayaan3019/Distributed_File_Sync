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
from ..core.protocol import Protocol, Message, MessageType, PlainChannel
from ..core.security import Authenticator, AuthenticationError, SecureChannel

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
        channel: Frame transport. `SecureChannel` normally, `PlainChannel`
            only when the operator explicitly opted out of security.
        protocol_version: Version the peer advertised in HELLO, as a tuple.
            Used to gate protocol 1.1 features such as FILE_RENAME.
        authenticated: True once the peer proved knowledge of the shared key.
        outbound: True if we dialled the peer, False if the peer dialled us.
            Needed by `NetworkManager._register` to break duplicate-connection
            ties the same way on both sides.
        connected_at: Timestamp of connection establishment.
        last_heartbeat: Timestamp of last heartbeat received.
    """
    peer_id: str
    host: str
    port: int
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    channel: object = None
    protocol_version: tuple = (1, 0)
    authenticated: bool = False
    outbound: bool = False
    connected_at: float = field(default_factory=lambda: asyncio.get_event_loop().time())
    last_heartbeat: float = field(default_factory=lambda: asyncio.get_event_loop().time())

    def __post_init__(self):
        """Default the channel to plaintext framing over the raw streams."""
        if self.channel is None:
            self.channel = PlainChannel(self.reader, self.writer)

    def address(self) -> str:
        """Get peer address as string."""
        return f"{self.host}:{self.port}"

    def client_id(self, local_node_id: str) -> str:
        """
        Node id of whichever side dialled this connection.

        Both peers compute the same value for the same TCP connection, which
        is what makes the duplicate-connection tie-break in
        `NetworkManager._register` deterministic.

        Args:
            local_node_id: This node's identifier.

        Returns:
            The dialling node's identifier.
        """
        return local_node_id if self.outbound else self.peer_id

    def supports_rename(self) -> bool:
        """
        Report whether this peer understands FILE_RENAME.

        Returns:
            True when the peer advertised protocol >= 1.1.
        """
        return self.protocol_version >= Protocol.RENAME_MIN_VERSION

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
    
    #: Hard cap on simultaneously accepted inbound connections. The Phase 0
    #: audit noted that bounding a single message to 10MB does nothing if an
    #: attacker can open ten thousand sockets and hold 10MB in each. Handshakes
    #: are cheap to refuse and expensive to service, so the limit is applied
    #: before authentication, not after.
    MAX_INBOUND_CONNECTIONS = 64

    def __init__(self, config: SyncConfig,
                 message_handler: Callable[[Message, PeerConnection], Awaitable[None]]):
        """
        Initialize the network manager.

        Args:
            config: Node configuration.
            message_handler: Async function to handle received messages.

        Raises:
            ValueError: If the configuration neither supplies a pre-shared key
                nor explicitly opts out of security.
        """
        self.config = config
        self.message_handler = message_handler

        #: Optional async callback invoked with a peer id whenever a
        #: connection to that peer ends -- including one that merely lost the
        #: duplicate-connection tie-break. The sync engine installs itself
        #: here so work that was in flight over the dead socket restarts at
        #: once instead of waiting for the stall timeout to notice.
        #:
        #: It fires for displaced duplicates too, and that is deliberate: a
        #: transfer being served over the losing connection is exactly as dead
        #: as one on a connection whose peer crashed, and treating the two
        #: differently is what let a 4MB transfer hang for the full timeout.
        self.connection_lost_handler: Optional[Callable[[str], Awaitable[None]]] = None

        self.connections: Dict[str, PeerConnection] = {}
        self.server: Optional[asyncio.Server] = None
        self._running = False
        self._tasks: Set[asyncio.Task] = set()
        self._retry_delays: Dict[str, float] = {}  # Track retry delays per peer

        #: host:port -> the node id last seen there. Peers are configured by
        #: address but connections are keyed by identity, so this is what lets
        #: `_supervise_peer` tell "already connected" from "needs dialling"
        #: without opening a socket to find out.
        self._peer_ids: Dict[str, str] = {}
        self._inbound_count = 0
        #: Handler tasks for accepted connections. asyncio.start_server spawns
        #: these itself, so they are NOT in `_tasks`. Python 3.12's
        #: `Server.wait_closed()` waits for every one of them to finish, and a
        #: handler parked in `_message_loop` never does -- shutdown hung
        #: forever until these were tracked and cancelled explicitly.
        self._client_tasks: Set[asyncio.Task] = set()

        # Security is on by default and must be switched off deliberately.
        # The Phase 0 audit found that any TCP client saying HELLO was trusted
        # to write files, so "secure unless told otherwise" is the only
        # defensible default.
        self._authenticator: Optional[Authenticator] = None
        if config.psk:
            self._authenticator = Authenticator(config.node_id, config.psk)
        elif not config.allow_insecure:
            raise ValueError(
                "No pre-shared key configured. Set SyncConfig.psk (--psk / "
                "SYNC_PSK) to enable mutual authentication and encryption, or "
                "pass allow_insecure=True (--allow-insecure) to run without "
                "them. Running without a key means any peer that can reach "
                "this port can read and write your files."
            )
        else:
            logger.warning(
                "SECURITY: running with --allow-insecure. Peers are NOT "
                "authenticated and traffic is NOT encrypted."
            )
    
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
        
        # Connect to known peers, and keep those connections alive.
        for peer in self.config.peers:
            task = asyncio.create_task(self._supervise_peer(peer))
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
            # Bounded wait. A task that refuses to unwind -- or one belonging
            # to an event loop that is no longer running, which is what
            # happens if shutdown is driven from a different loop than
            # start-up -- must not wedge shutdown forever. Whatever has not
            # finished by now is abandoned; the sockets are closed below
            # regardless.
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=5.0,
                )
            except (asyncio.TimeoutError, RuntimeError) as exc:
                logger.warning(f"Abandoning {len(self._tasks)} task(s) at shutdown: {exc}")

        # Cancel inbound handler tasks. Closing our end of the socket is not
        # enough on Windows: the handler can stay parked in readexactly().
        for task in list(self._client_tasks):
            task.cancel()

        # Close all peer connections
        for conn in list(self.connections.values()):
            await conn.close()

        self.connections.clear()

        if self._client_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._client_tasks, return_exceptions=True),
                    timeout=5.0,
                )
            except (asyncio.TimeoutError, RuntimeError):
                logger.warning("Some connection handlers did not stop in time")
        self._client_tasks.clear()

        # Stop server
        if self.server:
            self.server.close()
            try:
                # Bounded: wait_closed() blocks until every handler has
                # finished, and one stuck handler must not wedge shutdown.
                await asyncio.wait_for(self.server.wait_closed(), timeout=5.0)
            except (asyncio.TimeoutError, RuntimeError):
                logger.warning("Server did not close cleanly within 5s")

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

        if self._inbound_count >= self.MAX_INBOUND_CONNECTIONS:
            logger.warning(
                f"Refusing connection from {peer_addr}: inbound limit "
                f"({self.MAX_INBOUND_CONNECTIONS}) reached"
            )
            writer.close()
            return

        self._inbound_count += 1

        # Register ourselves so stop() can cancel this handler. Without this,
        # Server.wait_closed() waits on a task nothing ever wakes.
        current = asyncio.current_task()
        if current is not None:
            self._client_tasks.add(current)

        try:
            # --- Step 1: prove the peer holds the pre-shared key ------------
            # Nothing else happens until this succeeds. An unauthenticated
            # socket never reaches a message handler, so it can never cause a
            # file to be written.
            channel = PlainChannel(reader, writer)
            authenticated = False

            if self._authenticator is not None:
                result = await self._authenticator.authenticate_as_server(reader, writer)
                channel = result.channel
                authenticated = True
                logger.info(f"Peer {result.peer_node_id!r} authenticated from {peer_addr}")

            # --- Step 2: application handshake over the (now secure) channel
            message = await asyncio.wait_for(
                Protocol.receive_message(channel), timeout=30.0
            )

            if not message or message.msg_type != MessageType.HELLO:
                logger.warning(
                    f"Expected HELLO from {peer_addr}, "
                    f"got {message.msg_type if message else 'None'}"
                )
                return

            peer_id = message.node_id
            if not peer_id:
                logger.warning(f"HELLO message missing node_id from {peer_addr}")
                return

            # The authenticated identity is authoritative. The proof is bound
            # to the node id, so a peer that authenticates as A cannot then
            # claim to be B in HELLO and inherit B's causal history.
            if authenticated and peer_id != result.peer_node_id:
                logger.warning(
                    f"Identity mismatch from {peer_addr}: authenticated as "
                    f"{result.peer_node_id!r} but HELLO claims {peer_id!r}"
                )
                return

            peer_conn = PeerConnection(
                peer_id=peer_id,
                host=peer_addr[0],
                port=peer_addr[1],
                reader=reader,
                writer=writer,
                channel=channel,
                protocol_version=Protocol.parse_version(
                    message.payload.get('version')
                ),
                authenticated=authenticated,
                outbound=False,
            )

            # HELLO_ACK goes out *before* the duplicate check so the dialling
            # peer can finish its handshake and evaluate the same tie-break
            # itself. If we closed the socket here instead, the client would
            # see a truncated handshake, treat it as a transient fault, and
            # redial into the same collision.
            hello_ack = Message(
                msg_type=MessageType.HELLO_ACK,
                payload={'version': Protocol.VERSION},
                node_id=self.config.node_id
            )
            await Protocol.send_message(channel, hello_ack)

            if not await self._register(peer_conn):
                logger.info(
                    f"Duplicate inbound connection from {peer_id} at "
                    f"{peer_addr} dropped; existing connection kept"
                )
                return

            logger.info(
                f"Peer {peer_id} connected from {peer_addr} "
                f"(protocol {peer_conn.protocol_version})"
            )

            await self._message_loop(peer_conn)

        except AuthenticationError as e:
            # Expected against a scanner or a misconfigured peer. Log at
            # warning, not error, and never echo the reason back to the peer.
            logger.warning(f"Rejected unauthenticated peer {peer_addr}: {e}")
        except asyncio.TimeoutError:
            logger.warning(f"Handshake timed out for {peer_addr}")
        except asyncio.CancelledError:
            logger.info(f"Connection handler cancelled for {peer_addr}")
        except Exception as e:
            logger.error(f"Error handling client {peer_addr}: {e}", exc_info=True)
        finally:
            self._inbound_count -= 1
            if current is not None:
                self._client_tasks.discard(current)
            # Only retract the registration if it is still ours. A newer
            # connection from the same peer may have replaced us, and removing
            # its entry here would silently disconnect a live peer.
            if peer_conn and self.connections.get(peer_conn.peer_id) is peer_conn:
                del self.connections[peer_conn.peer_id]
                logger.info(f"Peer {peer_conn.peer_id} disconnected")
            try:
                writer.close()
            except Exception:
                pass
    
    async def _supervise_peer(self, peer: PeerConfig) -> None:
        """
        Keep an outbound connection to a peer alive for the node's lifetime.

        `_connect_to_peer` returns when the connection ends -- cleanly, or
        because the socket died. It used to be called exactly once per peer at
        startup, so *any* disconnect was permanent: the node never dialled
        again, and since the peer's own client has the same shape, a momentary
        network fault partitioned the cluster until both processes were
        restarted. Nothing detected this because every test either used a
        connection that was never interrupted, or restarted the node anyway.

        Redialling after an established connection drops is a different thing
        from retrying an initial connection that never succeeded, so the retry
        budget is fresh each time round: `max_retries` bounds how long we chase
        a peer that will not answer, not how many times a working peer is
        allowed to reconnect over the node's lifetime.

        Args:
            peer: Peer configuration.
        """
        peer_key = f"{peer.host}:{peer.port}"

        while self._running:
            # Skip the dial entirely if this peer is already connected --
            # normally because it dialled us and its connection won the
            # tie-break. Dialling anyway would open a socket, complete a full
            # handshake, lose the tie-break and close, every retry interval.
            # Harmless in principle, but it churns `self.connections` while
            # the engine is mid-transfer over it, which is exactly how a 4MB
            # transfer intermittently died.
            known_id = self._peer_ids.get(peer_key)
            if known_id is not None and known_id in self.connections:
                await asyncio.sleep(self.config.retry_delay)
                continue

            try:
                if not await self._connect_to_peer(peer):
                    # Fatal and permanent -- a key mismatch. Redialling would
                    # turn a loud configuration error into an endless quiet
                    # one.
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    f"Peer supervisor for {peer_key} hit an unexpected error: "
                    f"{exc!r}", exc_info=True
                )

            if not self._running:
                break

            # Back off before redialling. Without this a peer that accepts and
            # immediately closes would spin this loop at full speed.
            await asyncio.sleep(self.config.retry_delay)

    async def _connect_to_peer(self, peer: PeerConfig) -> bool:
        """
        Connect to a peer with exponential backoff and serve it until it ends.

        Returns when the connection is over, when the peer is unreachable
        after `max_retries` attempts, or when authentication fails.

        Args:
            peer: Peer configuration.

        Returns:
            True if the caller should keep supervising this peer; False when
            the failure is permanent (a pre-shared key mismatch), so that a
            misconfiguration surfaces once instead of once per second.
        """
        peer_key = f"{peer.host}:{peer.port}"
        retry_delay = self._retry_delays.get(peer_key, self.config.retry_delay)
        retries = 0

        while self._running and retries < self.config.max_retries:
            try:
                logger.info(f"Connecting to peer at {peer_key}")
                
                # Open connection
                reader, writer = await asyncio.open_connection(peer.host, peer.port)

                # Authenticate before anything else. We verify the *server's*
                # proof first, so we never hand our node id and file list to a
                # host that cannot prove it belongs to this cluster.
                channel = PlainChannel(reader, writer)
                authenticated = False
                if self._authenticator is not None:
                    result = await self._authenticator.authenticate_as_client(reader, writer)
                    channel = result.channel
                    authenticated = True
                    logger.info(f"Authenticated peer {result.peer_node_id!r} at {peer_key}")

                # Send HELLO
                hello_msg = Protocol.create_hello_message(self.config.node_id)
                await Protocol.send_message(channel, hello_msg)

                # Wait for HELLO_ACK
                response = await asyncio.wait_for(
                    Protocol.receive_message(channel),
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

                self._peer_ids[peer_key] = peer_id

                if authenticated and peer_id != result.peer_node_id:
                    raise ConnectionError(
                        f"Identity mismatch: authenticated as "
                        f"{result.peer_node_id!r}, HELLO_ACK claims {peer_id!r}"
                    )

                # Create connection
                peer_conn = PeerConnection(
                    peer_id=peer_id,
                    host=peer.host,
                    port=peer.port,
                    reader=reader,
                    writer=writer,
                    channel=channel,
                    protocol_version=Protocol.parse_version(
                        response.payload.get('version')
                    ),
                    authenticated=authenticated,
                    outbound=True,
                )

                # Reset retry delay on success
                self._retry_delays[peer_key] = self.config.retry_delay

                if not await self._register(peer_conn):
                    # A connection to this peer already exists and won the
                    # tie-break; ours is closed. Wait for the survivor to end
                    # rather than redialling immediately -- an immediate retry
                    # would just lose the same tie-break once per second for
                    # as long as the peer stays up.
                    logger.info(
                        f"Already connected to {peer_id}; dropping redundant "
                        f"outbound connection to {peer_key}"
                    )
                    while self._running and self.connections.get(peer_id) is not None:
                        await asyncio.sleep(self.config.retry_delay)
                    return True

                logger.info(f"Connected to peer {peer_id} at {peer_key}")

                # Serve this connection until it ends, then hand back to the
                # supervisor so it can be re-established.
                await self._message_loop(peer_conn)
                logger.info(f"Connection to {peer_id} ended; will redial")
                return True

            except AuthenticationError as e:
                # A key mismatch is a configuration error, not a transient
                # fault. Retrying with exponential backoff would hide it behind
                # an ever-growing delay; failing loudly and immediately is the
                # only useful behaviour.
                logger.error(
                    f"Authentication with {peer_key} failed: {e}. "
                    f"If the peer is a sync node, check that both share the "
                    f"same --psk. Not retrying."
                )
                try:
                    writer.close()
                except Exception:
                    pass
                return False

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
                raise
            except Exception as e:
                logger.error(f"Unexpected error connecting to {peer_key}: {e}", exc_info=True)
                await asyncio.sleep(retry_delay)

        return True


    async def _message_loop(self, peer_conn: PeerConnection) -> None:
        """
        Continuously receive and handle messages from a peer.
        
        Args:
            peer_conn: The peer connection.
        """
        try:
            while self._running:
                message = await Protocol.receive_message(peer_conn.channel)

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

                    # Report the message type that failed, not the exception
                    # text. Exception text can carry absolute local paths and
                    # internal state, and this goes to a remote peer.
                    error_msg = Protocol.create_error_message(
                        f"failed to handle {message.msg_type.value}",
                        self.config.node_id,
                    )
                    await Protocol.send_message(peer_conn.channel, error_msg)
        
        except asyncio.CancelledError:
            logger.info(f"Message loop cancelled for {peer_conn.peer_id}")
        except Exception as e:
            logger.error(f"Error in message loop for {peer_conn.peer_id}: {e}", exc_info=True)
        finally:
            # Retract the registration only if it is still *ours*. Deleting by
            # peer id alone deregisters whatever connection happens to be
            # installed, which after a duplicate-connection tie-break is the
            # survivor, not the loser -- leaving the pair with a live socket
            # that neither side considers connected. `_handle_client` already
            # guarded this; the guard was useless because this loop runs first
            # and had no guard of its own.
            if self.connections.get(peer_conn.peer_id) is peer_conn:
                del self.connections[peer_conn.peer_id]
            await peer_conn.close()

            if self.connection_lost_handler is not None:
                try:
                    await self.connection_lost_handler(peer_conn.peer_id)
                except Exception as exc:
                    logger.error(
                        f"connection_lost_handler failed: {exc!r}", exc_info=True
                    )
    
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
                        await Protocol.send_message(conn.channel, heartbeat)
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
                await Protocol.send_message(conn.channel, message)
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
            await Protocol.send_message(conn.channel, message)
            logger.debug(f"Sent {message.msg_type.value} to {peer_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send to {peer_id}: {e}")
            return False
    
    async def _register(self, peer_conn: PeerConnection) -> bool:
        """
        Install a peer connection, resolving duplicates deterministically.

        Peers dial each other symmetrically, so an A<->B pair always ends up
        with two TCP connections: A's outbound and A's inbound from B. Both
        are keyed by the same peer id, so one of them has to go -- otherwise
        the second silently overwrites the first and the orphaned handler task
        leaks, which is what used to make `Server.wait_closed()` hang at
        shutdown.

        The previous rule was "keep the newest". That is wrong, and it cost a
        4MB file: A was 1024 chunks into streaming a transfer when its own
        retry loop finally reached B, and B dutifully closed the in-flight
        connection in favour of the brand new idle one. The sender died with
        "Connection lost" and the receiver waited forever. Under 1MB the
        transfer finished before the redial landed, which is exactly why every
        existing test passed.

        The rule now is **keep the connection whose dialler has the
        lexicographically smaller node id**. Both sides compute the same
        answer for the same connection -- see `PeerConnection.client_id` -- so
        they converge on one survivor without negotiating, and the survivor is
        chosen by identity rather than by timing. A late duplicate is dropped
        instead of displacing whatever is currently mid-transfer.

        Args:
            peer_conn: The connection to install.

        Returns:
            True if `peer_conn` is now the live connection for that peer;
            False if it lost the tie-break and has been closed. Callers must
            not run a message loop on a connection that returned False.
        """
        existing = self.connections.get(peer_conn.peer_id)

        if existing is None or existing is peer_conn:
            self.connections[peer_conn.peer_id] = peer_conn
            return True

        local = self.config.node_id
        if peer_conn.client_id(local) < existing.client_id(local):
            winner, loser, keep_new = peer_conn, existing, True
        else:
            # Ties (both connections dialled by the same side, i.e. the older
            # one is genuinely dead and its peer redialled) resolve to the
            # incumbent being dropped, so a real reconnect still works.
            if peer_conn.client_id(local) == existing.client_id(local):
                winner, loser, keep_new = peer_conn, existing, True
            else:
                winner, loser, keep_new = existing, peer_conn, False

        self.connections[peer_conn.peer_id] = winner
        logger.debug(
            f"Duplicate connection to {peer_conn.peer_id}: keeping the one "
            f"dialled by {winner.client_id(local)!r}, closing the other"
        )
        try:
            await loser.close()
        except Exception:
            pass

        return keep_new

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
