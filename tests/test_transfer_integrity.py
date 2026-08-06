"""
Tests for atomic application of an incoming transfer.

Phase 0 finding (HIGH): chunks were written straight into the destination
inode with no temp file, no fsync and no rename, so a crash mid-apply left a
file that was neither the old version nor the new one. There was also no check
that what we assembled matched what the peer said it was sending.
"""

import os
import shutil
import tempfile
from pathlib import Path

import pytest

from app.algorithms.merkle import CHUNK_SIZE, MerkleTree
from app.algorithms.vector_clock import VectorClock
from app.core.config import SyncConfig
from app.services.sync_engine import PendingTransfer, SyncEngine


class StubNetwork:
    def __init__(self):
        self.broadcasts = []
        self.connections = {}

    async def broadcast(self, message, exclude_peer=None):
        self.broadcasts.append(message)

    async def send_to_peer(self, peer_id, message):
        return True


@pytest.fixture
def engine():
    """An engine with persistence disabled, for fast focused tests."""
    temp_dir = tempfile.mkdtemp(prefix="dfs_integrity_")
    config = SyncConfig(
        node_id="local", sync_directory=temp_dir,
        listen_host="127.0.0.1", listen_port=9500,
        psk="test-preshared-key", enable_catalog=False,
    )
    yield SyncEngine(config, StubNetwork())
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestAtomicApply:
    """A transfer is published with os.replace or not at all."""

    @pytest.mark.asyncio
    async def test_apply_leaves_no_temp_files(self, engine):
        rel = "doc.bin"
        payload = b"N" * CHUNK_SIZE

        engine.transfers[rel] = PendingTransfer(
            source_path=rel, dest_path=rel, expected=set(),
            chunks={0: payload},
            remote_clock=VectorClock("remote", {"remote": 1}),
            remote_size=len(payload),
        )
        await engine._apply_transfer(rel)

        temp_dir = Path(engine.config.sync_directory) / ".dfs" / "tmp"
        leftovers = list(temp_dir.iterdir()) if temp_dir.exists() else []
        assert leftovers == [], f"temp files left behind: {leftovers}"
        assert (Path(engine.config.sync_directory) / rel).read_bytes() == payload

    @pytest.mark.asyncio
    async def test_mismatched_content_is_not_published(self, engine):
        """
        If what we assembled does not hash to what the peer advertised, the
        destination must be left untouched. Publishing it would silently
        corrupt the tree and then replicate the corruption onward.
        """
        rel = "guarded.bin"
        target = Path(engine.config.sync_directory) / rel
        target.write_bytes(b"ORIGINAL" * 100)
        original = target.read_bytes()

        await engine._process_local_file(rel, notify=False)

        engine.transfers[rel] = PendingTransfer(
            source_path=rel, dest_path=rel, expected=set(),
            chunks={0: b"TAMPERED" * 100},
            remote_clock=VectorClock("remote", {"remote": 5}),
            remote_size=800,
            # Leaf hashes the peer claimed -- deliberately not the hash of
            # what we are about to assemble.
            remote_leaves=["0" * 64],
        )
        await engine._apply_transfer(rel)

        assert target.read_bytes() == original, (
            "content that failed verification was published anyway"
        )
        assert rel not in engine.transfers

    @pytest.mark.asyncio
    async def test_matching_content_is_published(self, engine):
        """The converse: correct content must pass verification."""
        rel = "verified.bin"
        payload = b"GOOD" * 1000
        tree = MerkleTree.from_leaf_hashes(
            [MerkleTree._compute_hash(payload[i:i + CHUNK_SIZE])
             for i in range(0, len(payload), CHUNK_SIZE)],
            len(payload), CHUNK_SIZE,
        )

        chunks = {i // CHUNK_SIZE: payload[i:i + CHUNK_SIZE]
                  for i in range(0, len(payload), CHUNK_SIZE)}

        engine.transfers[rel] = PendingTransfer(
            source_path=rel, dest_path=rel, expected=set(), chunks=chunks,
            remote_clock=VectorClock("remote", {"remote": 1}),
            remote_size=len(payload),
            remote_leaves=tree.leaf_hashes(),
        )
        await engine._apply_transfer(rel)

        assert (Path(engine.config.sync_directory) / rel).read_bytes() == payload

    @pytest.mark.asyncio
    async def test_unsolicited_chunk_is_dropped(self, engine):
        """
        A peer must not be able to push content for a path we never asked
        about. Accepting it would bypass clock comparison entirely.
        """
        import base64

        from app.core.protocol import Message, MessageType

        rel = "unrequested.bin"
        message = Message(
            msg_type=MessageType.CHUNK_DATA,
            payload={
                "file_path": rel, "chunk_index": 0,
                "chunk_data": base64.b64encode(b"INJECTED").decode(),
                "chunk_size": 8,
            },
            node_id="remote",
        )

        class FakePeer:
            peer_id = "remote"

        await engine._handle_chunk_data(message, FakePeer())

        assert not (Path(engine.config.sync_directory) / rel).exists()

    @pytest.mark.asyncio
    async def test_partial_transfer_does_not_touch_destination(self, engine):
        rel = "partial.bin"
        target = Path(engine.config.sync_directory) / rel
        original = b"A" * CHUNK_SIZE + b"B" * CHUNK_SIZE
        target.write_bytes(original)
        await engine._process_local_file(rel, notify=False)

        import base64

        from app.core.protocol import Message, MessageType

        engine.transfers[rel] = PendingTransfer(
            source_path=rel, dest_path=rel, expected={0, 1},
            remote_clock=VectorClock("remote", {"remote": 1}),
            remote_size=len(original),
        )

        class FakePeer:
            peer_id = "remote"

        await engine._handle_chunk_data(Message(
            msg_type=MessageType.CHUNK_DATA,
            payload={"file_path": rel, "chunk_index": 0,
                     "chunk_data": base64.b64encode(b"X" * CHUNK_SIZE).decode(),
                     "chunk_size": CHUNK_SIZE},
            node_id="remote",
        ), FakePeer())

        assert target.read_bytes() == original, "destination touched mid-transfer"


class TestPathContainmentInEngine:
    """The engine must refuse escaping paths from every entry point."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("evil", [
        "../escape.txt", "/etc/passwd", "a/../../b.txt", "C:\\x.txt",
    ])
    async def test_chunk_data_with_escaping_path_rejected(self, engine, evil):
        import base64

        from app.core.protocol import Message, MessageType

        class FakePeer:
            peer_id = "attacker"

        message = Message(
            msg_type=MessageType.CHUNK_DATA,
            payload={"file_path": evil, "chunk_index": 0,
                     "chunk_data": base64.b64encode(b"PWNED").decode(),
                     "chunk_size": 5},
            node_id="attacker",
        )

        # handle_message swallows PathTraversalError after logging it, which is
        # the production behaviour: drop the message, keep the connection.
        await engine.handle_message(message, FakePeer())

        outside = Path(engine.config.sync_directory).parent / "escape.txt"
        assert not outside.exists()

    @pytest.mark.asyncio
    async def test_sync_req_with_escaping_path_rejected(self, engine):
        from app.core.protocol import Message, MessageType

        class FakePeer:
            peer_id = "attacker"

        await engine.handle_message(Message(
            msg_type=MessageType.SYNC_REQ,
            payload={"file_path": "../../evil.txt", "root_hash": "x" * 64,
                     "vector_clock": {"node_id": "attacker", "clock": {"attacker": 1}}},
            node_id="attacker",
        ), FakePeer())

        assert "../../evil.txt" not in engine.file_metadata
