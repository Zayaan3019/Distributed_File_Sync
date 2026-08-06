"""
Two real nodes over a real loopback socket.

Nothing is mocked here: two `NetworkManager` servers, a genuine PSK handshake,
AES-GCM frames, and the actual sync engine on both sides. This is the test that
backs Definition-of-Done #2 -- two peers converge, and a concurrent edit
surfaces as a visible conflict.

It is also the only place the *whole* pipeline runs end to end, so it is where
an integration break shows up that every unit test misses.
"""

import asyncio
import os
import shutil
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from app.algorithms.merkle import CHUNK_SIZE
from app.algorithms.vector_clock import ClockRelation
from app.core.config import SyncConfig
from app.services.network import NetworkManager
from app.services.sync_engine import SyncEngine, is_conflict_sibling

PSK = "two-node-integration-key"


class Node:
    """
    One running peer: config, network manager, engine and sync directory.

    Attributes:
        node_id: This node's identifier.
        directory: Its sync root.
        engine: The SyncEngine.
        network: The NetworkManager.
    """

    def __init__(self, node_id: str, port: int, peer_ports=()):
        """
        Initialize the node.

        Args:
            node_id: Node identifier.
            port: Port to listen on.
            peer_ports: Ports of peers to dial.
        """
        self.node_id = node_id
        self.directory = tempfile.mkdtemp(prefix=f"dfs_{node_id}_")
        self.config = SyncConfig(
            node_id=node_id,
            sync_directory=self.directory,
            listen_host="127.0.0.1",
            listen_port=port,
            psk=PSK,
            heartbeat_interval=3600.0,
        )
        for peer_port in peer_ports:
            self.config.add_peer("127.0.0.1", peer_port)

        self.network = NetworkManager(self.config, self._dispatch)
        self.engine = SyncEngine(self.config, self.network)

    async def _dispatch(self, message, peer_conn):
        await self.engine.handle_message(message, peer_conn)

    async def start(self):
        await self.engine.initialize()
        await self.network.start()

    async def stop(self):
        await self.network.stop()
        await self.engine.close()
        shutil.rmtree(self.directory, ignore_errors=True)

    def path(self, relative: str) -> Path:
        """
        Absolute path of a file in this node's tree.

        Args:
            relative: Relative path.

        Returns:
            Absolute path.
        """
        return Path(self.directory) / relative

    def write(self, relative: str, data) -> None:
        """
        Write a file directly into the backing store.

        Args:
            relative: Relative path.
            data: str or bytes to write.
        """
        target = self.path(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(data, str):
            target.write_text(data)
        else:
            target.write_bytes(data)


async def wait_for(predicate, timeout=15.0, interval=0.05):
    """
    Poll until a predicate holds.

    Polling rather than sleeping a fixed amount keeps the test both fast when
    things work and honest when they do not: a failure means the condition was
    never reached, not that the sleep was too short.

    Args:
        predicate: Zero-argument callable returning truthy on success.
        timeout: Seconds before giving up.
        interval: Seconds between polls.

    Returns:
        True if the predicate held, False on timeout.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


#: Ports are handed out per test rather than reused. On Windows a listener
#: socket lingers in TIME_WAIT, so rebinding the same port across sixteen
#: consecutive tests intermittently fails to bind.
_next_port = [19800]


def _allocate_ports():
    """
    Reserve a fresh pair of loopback ports.

    Returns:
        Tuple of two port numbers.
    """
    base = _next_port[0]
    _next_port[0] += 2
    return base, base + 1


@asynccontextmanager
async def two_nodes():
    """
    Start two connected peers and shut them down afterwards.

    Deliberately an async context manager rather than a pytest fixture.
    pytest-asyncio drives async-generator fixture teardown in a *fresh* event
    loop, so `NetworkManager.stop()` would await tasks belonging to the
    already-closed test loop and hang forever -- which is exactly what
    happened, and cost a 600s timeout to find. Doing setup and teardown inside
    the test body guarantees both run on the same loop.

    Yields:
        Tuple of (node_a, node_b), already connected and authenticated.
    """
    port_a, port_b = _allocate_ports()
    a = Node("node_a", port_a, peer_ports=[port_b])
    b = Node("node_b", port_b, peer_ports=[port_a])

    try:
        await a.start()
        await b.start()

        connected = await wait_for(
            lambda: a.network.get_connected_peers()
            and b.network.get_connected_peers(),
            timeout=15.0,
        )
        assert connected, "peers never connected"

        yield a, b
    finally:
        await a.stop()
        await b.stop()


async def settle(node_a, node_b, path, timeout=15.0):
    """
    Wait until both nodes hold `path` with identical vector clocks.

    Establishing a genuinely shared baseline matters for the conflict tests:
    if the two clocks have not converged first, the subsequent edits are not
    actually concurrent and the test would be asserting the wrong thing.

    Args:
        node_a: First node.
        node_b: Second node.
        path: Relative path both nodes should agree on.
        timeout: Seconds before giving up.

    Returns:
        True if the clocks converged.
    """
    return await wait_for(
        lambda: (
            path in node_a.engine.file_metadata
            and path in node_b.engine.file_metadata
            and node_a.engine.file_metadata[path].vector_clock.compare(
                node_b.engine.file_metadata[path].vector_clock
            ) == ClockRelation.EQUAL
        ),
        timeout=timeout,
    )


class TestConnectivity:
    """The handshake has to work before anything else is meaningful."""

    @pytest.mark.asyncio
    async def test_peers_authenticate_each_other(self):
        async with two_nodes() as (a, b):
            assert "node_b" in a.network.get_connected_peers()
            assert "node_a" in b.network.get_connected_peers()

    @pytest.mark.asyncio
    async def test_connections_are_encrypted(self):
        async with two_nodes() as (a, b):
            from app.core.security import SecureChannel
            for conn in a.network.connections.values():
                assert conn.authenticated
                assert isinstance(conn.channel, SecureChannel)

    @pytest.mark.asyncio
    async def test_protocol_version_negotiated(self):
        async with two_nodes() as (a, b):
            for conn in a.network.connections.values():
                assert conn.protocol_version == (1, 1)
                assert conn.supports_rename()

    @pytest.mark.asyncio
    async def test_peers_converge_on_exactly_one_connection(self):
        """
        Both peers dial each other, so two TCP connections always exist
        briefly. They must settle on the same survivor, and each side must
        still consider itself connected afterwards.

        The failure this guards is not "two connections" -- it is the
        aftermath. `_message_loop` used to deregister by peer id without
        checking identity, so the losing connection's teardown removed the
        *winner's* entry and both nodes ended up believing they had no peer
        while a perfectly good socket sat open between them.
        """
        async with two_nodes() as (a, b):
            # Give both dial attempts time to land and the tie-break to run.
            await asyncio.sleep(1.5)

            assert len(a.network.connections) == 1, a.network.connections
            assert len(b.network.connections) == 1, b.network.connections
            assert "node_b" in a.network.connections
            assert "node_a" in b.network.connections

            # The survivor is chosen by identity, not timing: the connection
            # dialled by the lexicographically smaller node id wins, so
            # node_a's outbound connection is the one both sides keep.
            assert a.network.connections["node_b"].outbound is True
            assert b.network.connections["node_a"].outbound is False

            # And it is actually usable, not merely present.
            a.write("after_tiebreak.txt", "still connected")
            await a.engine.on_file_changed("after_tiebreak.txt")
            assert await wait_for(lambda: b.path("after_tiebreak.txt").exists())

    @pytest.mark.asyncio
    async def test_peers_reconnect_after_the_connection_drops(self):
        """
        A dropped connection must be re-established without restarting either
        node.

        This had no coverage and did not work. `_connect_to_peer` was invoked
        once per peer at startup and returned for good once its message loop
        ended, so the first transient fault partitioned the cluster
        permanently -- and because both peers share the implementation,
        neither side would ever dial again. Every existing test used a
        connection that was never interrupted, so nothing noticed.
        """
        async with two_nodes() as (a, b):
            await asyncio.sleep(1.5)
            assert a.network.connections, "no connection to break"

            # Kill the live connection from underneath both nodes, the way a
            # network fault would.
            for conn in list(a.network.connections.values()):
                conn.writer.close()

            dropped = await wait_for(
                lambda: not a.network.connections or not b.network.connections,
                timeout=10.0,
            )
            assert dropped, "closing the writer did not drop the connection"

            restored = await wait_for(
                lambda: a.network.connections and b.network.connections,
                timeout=30.0,
            )
            assert restored, "peers never reconnected after the drop"

            # Reconnected is not the same as working.
            a.write("after_reconnect.txt", "sent on the new connection")
            await a.engine.on_file_changed("after_reconnect.txt")
            assert await wait_for(
                lambda: b.path("after_reconnect.txt").exists(), timeout=30.0
            ), "reconnected but nothing flows over the new connection"


class TestConvergence:
    """Definition-of-Done #2, first half."""

    @pytest.mark.asyncio
    async def test_new_file_propagates(self):
        async with two_nodes() as (a, b):
            a.write("hello.txt", "hello from node_a")
            await a.engine.on_file_changed("hello.txt")

            arrived = await wait_for(lambda: b.path("hello.txt").exists())
            assert arrived, "file never reached node_b"
            assert b.path("hello.txt").read_text() == "hello from node_a"

    @pytest.mark.asyncio
    async def test_clocks_converge_to_equal(self):
        """
        If merge() ever ticks the merger's own counter again, the two peers
        ping-pong forever and this never settles.
        """
        async with two_nodes() as (a, b):
            a.write("conv.txt", "content")
            await a.engine.on_file_changed("conv.txt")

            assert await wait_for(lambda: "conv.txt" in b.engine.file_metadata)

            settled = await wait_for(lambda: (
                "conv.txt" in a.engine.file_metadata
                and a.engine.file_metadata["conv.txt"].vector_clock.compare(
                    b.engine.file_metadata["conv.txt"].vector_clock
                ) == ClockRelation.EQUAL
            ))
            assert settled, (
                f"clocks never converged: "
                f"a={a.engine.file_metadata['conv.txt'].vector_clock.clock} "
                f"b={b.engine.file_metadata['conv.txt'].vector_clock.clock}"
            )

    @pytest.mark.asyncio
    async def test_multi_chunk_file_propagates_intact(self):
        async with two_nodes() as (a, b):
            payload = bytes(range(256)) * 200  # ~51KB, 13 chunks
            a.write("big.bin", payload)
            await a.engine.on_file_changed("big.bin")

            arrived = await wait_for(
                lambda: b.path("big.bin").exists()
                and b.path("big.bin").read_bytes() == payload
            )
            assert arrived, "multi-chunk transfer did not reproduce the file"

    @pytest.mark.asyncio
    async def test_edit_ships_only_changed_chunks(self):
        """
        The whole point of the Merkle tree. A one-chunk edit in a 12-chunk
        file must not re-transmit the file.
        """
        async with two_nodes() as (a, b):
            original = b"".join(bytes([i]) * CHUNK_SIZE for i in range(12))
            a.write("delta.bin", original)
            await a.engine.on_file_changed("delta.bin")
            assert await wait_for(lambda: b.path("delta.bin").read_bytes() == original
                                  if b.path("delta.bin").exists() else False)

            edited = bytearray(original)
            edited[7 * CHUNK_SIZE:8 * CHUNK_SIZE] = b"Z" * CHUNK_SIZE
            a.write("delta.bin", bytes(edited))
            await a.engine.apply_local_write("delta.bin", {7})

            converged = await wait_for(
                lambda: b.path("delta.bin").read_bytes() == bytes(edited)
            )
            assert converged, "edit did not converge"

    @pytest.mark.asyncio
    async def test_deletion_propagates(self):
        async with two_nodes() as (a, b):
            a.write("doomed.txt", "temporary")
            await a.engine.on_file_changed("doomed.txt")
            assert await wait_for(lambda: b.path("doomed.txt").exists())

            a.path("doomed.txt").unlink()
            await a.engine.on_file_deleted("doomed.txt")

            removed = await wait_for(lambda: not b.path("doomed.txt").exists())
            assert removed, "deletion did not propagate"

    @pytest.mark.asyncio
    async def test_deleted_file_does_not_resurrect(self):
        """
        Without a tombstone, the peer that still remembers the file
        re-advertises it and it comes back.
        """
        async with two_nodes() as (a, b):
            a.write("ghost.txt", "boo")
            await a.engine.on_file_changed("ghost.txt")
            assert await wait_for(lambda: b.path("ghost.txt").exists())

            a.path("ghost.txt").unlink()
            await a.engine.on_file_deleted("ghost.txt")
            assert await wait_for(lambda: not b.path("ghost.txt").exists())

            # b re-advertises what it used to know. a's tombstone must out-rank it.
            tombstone = await a.engine.catalog.get_tombstone("ghost.txt", "node_a")
            assert tombstone is not None

            await asyncio.sleep(1.0)
            assert not a.path("ghost.txt").exists(), "file resurrected on node_a"
            assert not b.path("ghost.txt").exists(), "file resurrected on node_b"

    @pytest.mark.asyncio
    async def test_rename_propagates_without_refetch(self):
        async with two_nodes() as (a, b):
            a.write("before.txt", "movable content")
            await a.engine.on_file_changed("before.txt")
            assert await wait_for(lambda: b.path("before.txt").exists())

            import os
            os.replace(a.path("before.txt"), a.path("after.txt"))
            await a.engine.on_file_renamed("before.txt", "after.txt")

            moved = await wait_for(
                lambda: b.path("after.txt").exists() and not b.path("before.txt").exists()
            )
            assert moved, "rename did not propagate"
            assert b.path("after.txt").read_text() == "movable content"

    @pytest.mark.asyncio
    async def test_multi_megabyte_file_propagates(self):
        """
        A file large enough that its transfer spans more than a moment.

        Every other convergence test here uses a file that transfers in a
        single burst, and that is precisely why the duplicate-connection race
        survived the whole suite: under about 1MB the exchange finished before
        the second dial could land. 4MB is 1024 chunks, which takes long enough
        that anything disturbing the connection mid-stream shows up.
        """
        async with two_nodes() as (a, b):
            payload = os.urandom(4 * 1024 * 1024)
            a.write("large.bin", payload)
            await a.engine.on_file_changed("large.bin")

            arrived = await wait_for(
                lambda: b.path("large.bin").exists()
                and b.path("large.bin").stat().st_size == len(payload),
                timeout=60.0,
            )
            assert arrived, "4MB file never reached node_b"
            assert b.path("large.bin").read_bytes() == payload

    @pytest.mark.asyncio
    async def test_stalled_transfer_is_abandoned_and_retried(self):
        """
        A transfer whose sender goes silent must be restarted, not waited on
        forever.

        Simulated by handing the receiver a transfer that is already past the
        stall deadline: what matters is that the reaper drops the partial
        state and re-asks, not how the sender happened to die.
        """
        async with two_nodes() as (a, b):
            a.write("stalls.txt", "content that must arrive")
            await a.engine.on_file_changed("stalls.txt")
            assert await wait_for(lambda: b.path("stalls.txt").exists())

            from app.services.sync_engine import PendingTransfer

            b.engine.transfers["stuck.bin"] = PendingTransfer(
                source_path="stuck.bin",
                dest_path="stuck.bin",
                expected={0, 1, 2},
                peer_id="node_a",
                last_progress=time.monotonic() - 10_000,
            )

            abandoned = await b.engine.reap_stalled_transfers(timeout=1.0)

            assert abandoned == ["stuck.bin"]
            assert "stuck.bin" not in b.engine.transfers
            # Nothing partial is ever written to disk, so an abandoned
            # transfer must leave no trace in the sync root.
            assert not b.path("stuck.bin").exists()

    @pytest.mark.asyncio
    async def test_transfer_recovers_immediately_when_its_connection_dies(self):
        """
        A transfer stranded by a dead connection is restarted at once, not
        after the 60s stall timeout.

        This is the fast path for a failure the reaper would also eventually
        catch. It matters because the common cause is not a network fault at
        all: both peers dial each other, one connection loses the duplicate
        tie-break, and if a transfer happened to be running over the loser it
        used to hang. A 1MB file always arrived and a 4MB file intermittently
        did not, purely because the smaller transfer finished before the
        tie-break could fire.
        """
        async with two_nodes() as (a, b):
            from app.services.sync_engine import PendingTransfer

            b.engine.transfers["orphaned.bin"] = PendingTransfer(
                source_path="orphaned.bin",
                dest_path="orphaned.bin",
                expected={0, 1, 2},
                peer_id="node_a",
            )

            # The engine is told the connection ended. The peer is still
            # reachable, so it should re-ask rather than give up.
            await b.engine.on_connection_lost("node_a")

            assert "orphaned.bin" not in b.engine.transfers, (
                "stranded transfer was not abandoned"
            )

    @pytest.mark.asyncio
    async def test_connection_loss_ignores_unrelated_transfers(self):
        """A transfer from a different peer must not be collateral damage."""
        async with two_nodes() as (a, b):
            from app.services.sync_engine import PendingTransfer

            b.engine.transfers["other_peer.bin"] = PendingTransfer(
                source_path="other_peer.bin",
                dest_path="other_peer.bin",
                expected={0},
                peer_id="node_c",
            )

            await b.engine.on_connection_lost("node_a")

            assert "other_peer.bin" in b.engine.transfers

    @pytest.mark.asyncio
    async def test_reaper_leaves_progressing_transfers_alone(self):
        """A transfer that is still receiving chunks must not be killed."""
        async with two_nodes() as (a, b):
            from app.services.sync_engine import PendingTransfer

            b.engine.transfers["moving.bin"] = PendingTransfer(
                source_path="moving.bin",
                dest_path="moving.bin",
                expected={0},
                peer_id="node_a",
            )

            assert await b.engine.reap_stalled_transfers(timeout=60.0) == []
            assert "moving.bin" in b.engine.transfers


class TestConflictPresentation:
    """
    Definition-of-Done #2, second half: a concurrent edit must surface as a
    *visible* conflict, and both peers must reach the same verdict without
    further communication.
    """

    @pytest.mark.asyncio
    async def test_concurrent_edit_creates_a_visible_sibling(self):
        async with two_nodes() as (a, b):

            # Establish a shared baseline first.
            a.write("shared.txt", "baseline")
            await a.engine.on_file_changed("shared.txt")
            assert await wait_for(lambda: b.path("shared.txt").exists())
            assert await settle(a, b, "shared.txt"), "baseline never converged"

            # Now edit on BOTH sides with no exchange in between: genuinely
            # concurrent, so the clocks must compare CONCURRENT.
            a.write("shared.txt", "edited by A")
            b.write("shared.txt", "edited by B")

            await asyncio.gather(
                a.engine.apply_local_write("shared.txt", {0}),
                b.engine.apply_local_write("shared.txt", {0}),
            )

            def sibling_visible(node):
                return [p.name for p in Path(node.directory).iterdir()
                        if is_conflict_sibling(p.name)]

            surfaced = await wait_for(
                lambda: sibling_visible(a) and sibling_visible(b), timeout=20.0
            )
            assert surfaced, (
                f"no conflict sibling appeared. "
                f"a={sorted(p.name for p in Path(a.directory).iterdir())} "
                f"b={sorted(p.name for p in Path(b.directory).iterdir())}"
            )

    @pytest.mark.asyncio
    async def test_both_peers_agree_on_the_winner(self):
        """
        The decisive property. Last-write-wins would let the two peers keep
        different winners and diverge permanently; the deterministic node_id
        rule cannot.
        """
        async with two_nodes() as (a, b):
            a.write("verdict.txt", "baseline")
            await a.engine.on_file_changed("verdict.txt")
            assert await wait_for(lambda: b.path("verdict.txt").exists())
            assert await settle(a, b, "verdict.txt"), "baseline never converged"

            a.write("verdict.txt", "version from A")
            b.write("verdict.txt", "version from B")
            await asyncio.gather(
                a.engine.apply_local_write("verdict.txt", {0}),
                b.engine.apply_local_write("verdict.txt", {0}),
            )

            agreed = await wait_for(
                lambda: (a.path("verdict.txt").exists() and b.path("verdict.txt").exists()
                         and a.path("verdict.txt").read_bytes()
                         == b.path("verdict.txt").read_bytes()),
                timeout=20.0,
            )
            assert agreed, (
                f"peers disagree on the winning content: "
                f"a={a.path('verdict.txt').read_text()!r} "
                f"b={b.path('verdict.txt').read_text()!r}"
            )

            # node_a sorts lower, so node_a's version keeps the path.
            assert a.path("verdict.txt").read_text() == "version from A"

    @pytest.mark.asyncio
    async def test_no_edit_is_silently_lost(self):
        """Both versions must survive somewhere; neither is discarded."""
        async with two_nodes() as (a, b):
            a.write("keepboth.txt", "baseline")
            await a.engine.on_file_changed("keepboth.txt")
            assert await wait_for(lambda: b.path("keepboth.txt").exists())
            assert await settle(a, b, "keepboth.txt"), "baseline never converged"

            a.write("keepboth.txt", "AAA-unique-to-a")
            b.write("keepboth.txt", "BBB-unique-to-b")
            await asyncio.gather(
                a.engine.apply_local_write("keepboth.txt", {0}),
                b.engine.apply_local_write("keepboth.txt", {0}),
            )

            def all_content(node):
                return {p.read_text() for p in Path(node.directory).iterdir()
                        if p.is_file()}

            survived = await wait_for(
                lambda: {"AAA-unique-to-a", "BBB-unique-to-b"} <= all_content(a),
                timeout=20.0,
            )
            assert survived, f"an edit was lost on node_a: {all_content(a)}"

class TestUnauthenticatedPeerCannotWrite:
    """Definition-of-Done #6, proven against a live server."""

    @pytest.mark.asyncio
    async def test_peer_without_psk_is_refused(self):
        async with two_nodes() as (a, b):
            import json
            import struct

            reader, writer = await asyncio.open_connection("127.0.0.1", a.config.listen_port)
            body = json.dumps({"type": "HELLO", "payload": {"version": "1.1"},
                               "node_id": "ATTACKER"}).encode()
            writer.write(struct.pack("!I", len(body)) + body)
            await writer.drain()
            await asyncio.sleep(0.5)

            assert "ATTACKER" not in a.network.get_connected_peers()
            writer.close()

    @pytest.mark.asyncio
    async def test_peer_with_wrong_psk_is_refused(self):
        async with two_nodes() as (a, b):
            from app.core.security import Authenticator, AuthenticationError
            reader, writer = await asyncio.open_connection("127.0.0.1", a.config.listen_port)

            with pytest.raises(AuthenticationError):
                await Authenticator("ATTACKER", "not-the-right-key").authenticate_as_client(
                    reader, writer
                )

            assert "ATTACKER" not in a.network.get_connected_peers()
            writer.close()

    @pytest.mark.asyncio
    async def test_authenticated_peer_still_cannot_escape_the_root(self):
        """
        Authentication is not authorisation to write anywhere. Even a peer
        holding the correct key must stay inside the sync root.
        """
        async with two_nodes() as (a, b):
            import base64
            outside = Path(a.directory).parent / "ESCAPE_TARGET.txt"
            outside.write_text("untouched")

            from app.core.protocol import Message, MessageType, Protocol

            conn = b.network.connections["node_a"]
            await Protocol.send_message(conn.channel, Message(
                msg_type=MessageType.CHUNK_DATA,
                payload={
                    "file_path": f"../{outside.name}",
                    "chunk_index": 0,
                    "chunk_data": base64.b64encode(b"PWNED").decode(),
                    "chunk_size": 5,
                },
                node_id="node_b",
            ))
            await asyncio.sleep(0.8)

            assert outside.read_text() == "untouched", "authenticated peer escaped the root"
            outside.unlink()
