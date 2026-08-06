"""
Regression tests for the sync engine's chunk-apply path.

Covers defect 1.3 from docs/FUSE_DESIGN.md: _write_chunks opened the
destination with mode 'wb', which truncates to zero, and _handle_chunk_data
called it on *every* chunk received. A transfer of N chunks therefore rewrote
the file N times, each time from empty, so any chunk not included in that
particular transfer -- i.e. every chunk the Merkle diff had correctly
identified as unchanged -- was replaced by a zero-filled hole.

Also covers the clock rule: applying a peer's chunks is not a local event and
must not tick this node's counter, or the two peers ping-pong forever.
"""

import base64
import shutil
import tempfile
from pathlib import Path

import pytest

from app.algorithms.merkle import MerkleTree, CHUNK_SIZE
from app.algorithms.vector_clock import VectorClock, ClockRelation
from app.core.config import SyncConfig
from app.core.protocol import Message, MessageType
from app.services.sync_engine import PendingTransfer, SyncEngine


class StubNetwork:
    """Minimal NetworkManager stand-in that records what would be sent."""

    def __init__(self):
        self.broadcasts = []
        self.connections = {}

    async def broadcast(self, message, exclude_peer=None):
        self.broadcasts.append(message)

    async def send_to_peer(self, peer_id, message):
        self.broadcasts.append(message)
        return True


@pytest.fixture
def engine():
    """A SyncEngine over a real temporary sync directory."""
    temp_dir = tempfile.mkdtemp()
    config = SyncConfig(
        node_id="local",
        sync_directory=temp_dir,
        listen_host="127.0.0.1",
        listen_port=9100,
        psk="test-preshared-key",
    )
    yield SyncEngine(config, StubNetwork())
    shutil.rmtree(temp_dir, ignore_errors=True)


def arm_transfer(engine, rel: str, expected: set, remote_size: int,
                 remote_clock: VectorClock = None) -> PendingTransfer:
    """
    Register an in-flight transfer, as _handle_merkle_tree would.

    `remote_leaves` is left empty on purpose: these tests supply synthetic
    chunk content that does not correspond to any real peer tree, so the
    post-assembly integrity check is not applicable here. It is exercised
    directly in test_transfer_integrity.py.
    """
    transfer = PendingTransfer(
        source_path=rel,
        dest_path=rel,
        expected=set(expected),
        remote_clock=remote_clock or VectorClock("remote", {"remote": 1}),
        remote_size=remote_size,
    )
    engine.transfers[rel] = transfer
    return transfer


def chunk_message(file_path: str, chunk_index: int, data: bytes) -> Message:
    """Build a CHUNK_DATA message the way the engine sends one."""
    return Message(
        msg_type=MessageType.CHUNK_DATA,
        payload={
            "file_path": file_path,
            "chunk_index": chunk_index,
            "chunk_data": base64.b64encode(data).decode("ascii"),
            "chunk_size": len(data),
        },
        node_id="remote",
    )


class TestPartialTransferPreservesData:
    """A transfer must never zero out chunks it did not carry."""

    @pytest.mark.asyncio
    async def test_single_changed_chunk_preserves_the_rest(self, engine):
        """Receive only chunk 2 of a 3-chunk file; chunks 0 and 1 survive."""
        rel = "doc.bin"
        abs_path = Path(engine.config.sync_directory) / rel
        original = b"A" * CHUNK_SIZE + b"B" * CHUNK_SIZE + b"C" * CHUNK_SIZE
        abs_path.write_bytes(original)

        await engine._process_local_file(rel, notify=False)

        # The peer changed only chunk 2.
        arm_transfer(engine, rel, {2}, len(original))

        new_chunk = b"Z" * CHUNK_SIZE
        await engine._handle_chunk_data(chunk_message(rel, 2, new_chunk), None)

        result = abs_path.read_bytes()
        assert len(result) == len(original)
        assert result[:CHUNK_SIZE] == b"A" * CHUNK_SIZE, "chunk 0 was destroyed"
        assert result[CHUNK_SIZE:2 * CHUNK_SIZE] == b"B" * CHUNK_SIZE, "chunk 1 was destroyed"
        assert result[2 * CHUNK_SIZE:] == new_chunk

    @pytest.mark.asyncio
    async def test_transfer_not_applied_until_complete(self, engine):
        """With chunks outstanding, nothing is written to the file yet."""
        rel = "doc.bin"
        abs_path = Path(engine.config.sync_directory) / rel
        original = b"A" * CHUNK_SIZE + b"B" * CHUNK_SIZE
        abs_path.write_bytes(original)

        await engine._process_local_file(rel, notify=False)

        arm_transfer(engine, rel, {0, 1}, len(original))

        # Only the first of two expected chunks arrives.
        await engine._handle_chunk_data(chunk_message(rel, 0, b"X" * CHUNK_SIZE), None)

        assert abs_path.read_bytes() == original, "partial transfer was applied early"
        assert engine.transfers[rel].expected == {1}

        # The second chunk completes it.
        await engine._handle_chunk_data(chunk_message(rel, 1, b"Y" * CHUNK_SIZE), None)

        assert abs_path.read_bytes() == b"X" * CHUNK_SIZE + b"Y" * CHUNK_SIZE
        assert rel not in engine.transfers

    @pytest.mark.asyncio
    async def test_growth_extends_file(self, engine):
        """Receiving a chunk past EOF extends the file."""
        rel = "grow.bin"
        abs_path = Path(engine.config.sync_directory) / rel
        abs_path.write_bytes(b"A" * CHUNK_SIZE)

        await engine._process_local_file(rel, notify=False)

        arm_transfer(engine, rel, {1}, 2 * CHUNK_SIZE)

        await engine._handle_chunk_data(chunk_message(rel, 1, b"B" * CHUNK_SIZE), None)

        assert abs_path.read_bytes() == b"A" * CHUNK_SIZE + b"B" * CHUNK_SIZE

    @pytest.mark.asyncio
    async def test_shrink_truncates_to_remote_size(self, engine):
        """A smaller remote file truncates the local copy."""
        rel = "shrink.bin"
        abs_path = Path(engine.config.sync_directory) / rel
        abs_path.write_bytes(b"A" * CHUNK_SIZE + b"B" * CHUNK_SIZE)

        await engine._process_local_file(rel, notify=False)

        arm_transfer(engine, rel, {0}, 10)

        await engine._handle_chunk_data(chunk_message(rel, 0, b"Z" * CHUNK_SIZE), None)

        assert abs_path.read_bytes() == b"Z" * 10

    @pytest.mark.asyncio
    async def test_new_file_is_created(self, engine):
        """A file we have never seen is created rather than erroring."""
        rel = "brand_new.bin"
        abs_path = Path(engine.config.sync_directory) / rel

        arm_transfer(engine, rel, {0}, CHUNK_SIZE)

        await engine._handle_chunk_data(chunk_message(rel, 0, b"N" * CHUNK_SIZE), None)

        assert abs_path.exists()
        assert abs_path.read_bytes() == b"N" * CHUNK_SIZE


class TestRemoteWriteClockSemantics:
    """Applying a peer's data must not make us look newer than the peer."""

    @pytest.mark.asyncio
    async def test_applying_remote_chunks_does_not_tick_local_clock(self, engine):
        """After applying a peer's update we must be EQUAL to it, not AFTER."""
        rel = "doc.bin"
        abs_path = Path(engine.config.sync_directory) / rel
        abs_path.write_bytes(b"A" * CHUNK_SIZE)

        await engine._process_local_file(rel, notify=False)
        local_before = engine.file_metadata[rel].vector_clock.copy()

        # Peer's clock strictly dominates ours.
        remote_clock = VectorClock("remote", dict(local_before.clock))
        remote_clock.increment("remote")

        arm_transfer(engine, rel, {0}, CHUNK_SIZE, remote_clock)

        await engine._handle_chunk_data(chunk_message(rel, 0, b"B" * CHUNK_SIZE), None)

        settled = engine.file_metadata[rel].vector_clock
        assert settled.compare(remote_clock) == ClockRelation.EQUAL, (
            "applying a remote write ticked the local counter, so this node now "
            "looks newer than the peer that sent the data and will advertise "
            "back to it forever"
        )

    @pytest.mark.asyncio
    async def test_local_edit_does_tick_local_clock(self, engine):
        """The converse: a genuine local edit must advance our counter."""
        rel = "doc.bin"
        abs_path = Path(engine.config.sync_directory) / rel
        abs_path.write_bytes(b"A" * CHUNK_SIZE)

        await engine._process_local_file(rel, notify=False)
        first = engine.file_metadata[rel].vector_clock.copy()

        abs_path.write_bytes(b"B" * CHUNK_SIZE)
        await engine._process_local_file(rel, notify=False)
        second = engine.file_metadata[rel].vector_clock

        assert second.compare(first) == ClockRelation.AFTER


class TestChunkApplyRebuildsMetadata:
    """The Merkle tree must reflect what actually landed on disk."""

    @pytest.mark.asyncio
    async def test_tree_matches_file_after_apply(self, engine):
        rel = "doc.bin"
        abs_path = Path(engine.config.sync_directory) / rel
        abs_path.write_bytes(b"A" * CHUNK_SIZE + b"B" * CHUNK_SIZE)

        await engine._process_local_file(rel, notify=False)

        arm_transfer(engine, rel, {1}, 2 * CHUNK_SIZE)
        await engine._handle_chunk_data(chunk_message(rel, 1, b"C" * CHUNK_SIZE), None)

        fresh = await MerkleTree.from_file(str(abs_path))
        assert engine.file_metadata[rel].merkle_tree.root.hash == fresh.root.hash
