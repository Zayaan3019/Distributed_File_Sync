"""
Tests that metadata actually survives a restart.

Phase 0 finding (HIGH): the catalog module existed but had **zero callers**,
so every Merkle tree and vector clock still died with the process. The audit
probe measured a node's counter going from 4 back to 1 across a restart, which
makes a stale peer compare AFTER the restarted node and silently overwrite
good data with old content.

These tests fail if the catalog is ever disconnected again.
"""

import asyncio
import shutil
import tempfile
from pathlib import Path

import pytest

from app.algorithms.merkle import CHUNK_SIZE, MerkleTree
from app.algorithms.vector_clock import ClockRelation, VectorClock
from app.core.catalog import Catalog
from app.core.config import SyncConfig
from app.services.sync_engine import SyncEngine


class StubNetwork:
    """Records broadcasts instead of sending them."""

    def __init__(self):
        self.broadcasts = []
        self.connections = {}

    async def broadcast(self, message, exclude_peer=None):
        self.broadcasts.append(message)

    async def send_to_peer(self, peer_id, message):
        self.broadcasts.append(message)
        return True


@pytest.fixture
def sync_dir():
    """A temporary sync root that is cleaned up afterwards."""
    path = tempfile.mkdtemp(prefix="dfs_persist_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def make_config(sync_dir, node_id="node_a", port=9400):
    """
    Build a config with persistence enabled.

    Args:
        sync_dir: Sync root.
        node_id: Node identifier.
        port: Listen port (unused; no server is started).

    Returns:
        A SyncConfig.
    """
    return SyncConfig(
        node_id=node_id,
        sync_directory=sync_dir,
        listen_host="127.0.0.1",
        listen_port=port,
        psk="test-preshared-key",
    )


async def restart(sync_dir, node_id="node_a"):
    """
    Start a fresh engine over the same directory, as a process restart would.

    Args:
        sync_dir: Sync root.
        node_id: Node identifier.

    Returns:
        An initialised SyncEngine.
    """
    engine = SyncEngine(make_config(sync_dir, node_id), StubNetwork())
    await engine.initialize()
    return engine


class TestClockSurvivesRestart:
    """The finding that motivated the whole catalog."""

    @pytest.mark.asyncio
    async def test_counter_is_not_reset(self, sync_dir):
        engine = await restart(sync_dir)
        path = Path(sync_dir) / "notes.txt"

        for revision in range(4):
            path.write_text(f"revision {revision}")
            await engine.on_file_changed("notes.txt")

        before = engine.file_metadata["notes.txt"].vector_clock.copy()
        assert before.clock["node_a"] == 4
        await engine.close()

        revived = await restart(sync_dir)
        after = revived.file_metadata["notes.txt"].vector_clock

        assert after.clock["node_a"] == 4, (
            "the restarted node forgot its causal history, so a peer holding "
            "the older clock now compares AFTER it and will overwrite good data"
        )
        assert after.compare(before) == ClockRelation.EQUAL
        await revived.close()

    @pytest.mark.asyncio
    async def test_peer_entries_survive(self, sync_dir):
        """A clock is not just our own counter; peer entries matter too."""
        engine = await restart(sync_dir)
        path = Path(sync_dir) / "shared.txt"
        path.write_text("content")
        await engine.on_file_changed("shared.txt")

        metadata = engine.file_metadata["shared.txt"]
        metadata.vector_clock.merge(VectorClock("node_b", {"node_b": 7}))
        await engine._persist("shared.txt", metadata)
        await engine.close()

        revived = await restart(sync_dir)
        restored = revived.file_metadata["shared.txt"].vector_clock
        assert restored.clock["node_b"] == 7
        await revived.close()

    @pytest.mark.asyncio
    async def test_merkle_root_survives(self, sync_dir):
        engine = await restart(sync_dir)
        path = Path(sync_dir) / "data.bin"
        path.write_bytes(b"X" * (CHUNK_SIZE * 3 + 17))
        await engine.on_file_changed("data.bin")
        before = engine.file_metadata["data.bin"].merkle_tree.root.hash
        await engine.close()

        revived = await restart(sync_dir)
        after = revived.file_metadata["data.bin"].merkle_tree.root.hash

        assert after == before
        fresh = await MerkleTree.from_file(str(path))
        assert after == fresh.root.hash, "restored tree disagrees with the file"
        await revived.close()


class TestReconcileAvoidsRehashing:
    """
    The point of persisting metadata is that a remount costs a stat per file,
    not a full re-hash. This asserts that the reconcile actually skips work.
    """

    @pytest.mark.asyncio
    async def test_unchanged_files_are_not_rehashed(self, sync_dir):
        engine = await restart(sync_dir)
        for index in range(8):
            (Path(sync_dir) / f"f{index}.bin").write_bytes(b"z" * 5000)
            await engine.on_file_changed(f"f{index}.bin")
        await engine.close()

        hashed = []

        async def counting_hasher(absolute_path):
            hashed.append(absolute_path)
            tree = await MerkleTree.from_file(absolute_path)
            return tree.root.hash, tree.leaf_hashes()

        catalog = Catalog(str(Path(sync_dir) / ".dfs" / "catalog.db"))
        await catalog.open()
        report = await catalog.reconcile(
            sync_dir, "node_a", hasher=counting_hasher,
            ignore=lambda p: p.split("/")[0] == ".dfs",
        )
        await catalog.close()

        assert len(report.unchanged) == 8, report.summary()
        assert hashed == [], f"re-hashed files that had not changed: {hashed}"

    @pytest.mark.asyncio
    async def test_changed_file_is_rehashed(self, sync_dir):
        engine = await restart(sync_dir)
        (Path(sync_dir) / "stable.bin").write_bytes(b"a" * 4000)
        (Path(sync_dir) / "edited.bin").write_bytes(b"b" * 4000)
        await engine.on_file_changed("stable.bin")
        await engine.on_file_changed("edited.bin")
        await engine.close()

        # Change one file behind the engine's back, as an offline edit would.
        (Path(sync_dir) / "edited.bin").write_bytes(b"c" * 9000)

        hashed = []

        async def counting_hasher(absolute_path):
            hashed.append(Path(absolute_path).name)
            tree = await MerkleTree.from_file(absolute_path)
            return tree.root.hash, tree.leaf_hashes()

        catalog = Catalog(str(Path(sync_dir) / ".dfs" / "catalog.db"))
        await catalog.open()
        await catalog.reconcile(
            sync_dir, "node_a", hasher=counting_hasher,
            ignore=lambda p: p.split("/")[0] == ".dfs",
        )
        await catalog.close()

        assert hashed == ["edited.bin"], hashed

    @pytest.mark.asyncio
    async def test_file_deleted_while_down_becomes_a_tombstone(self, sync_dir):
        """Otherwise the delete never propagates and the file comes back."""
        engine = await restart(sync_dir)
        (Path(sync_dir) / "gone.txt").write_text("bye")
        await engine.on_file_changed("gone.txt")
        await engine.close()

        (Path(sync_dir) / "gone.txt").unlink()

        revived = await restart(sync_dir)
        tombstone = await revived.catalog.get_tombstone("gone.txt", "node_a")

        assert tombstone is not None, "a delete that happened while offline was lost"
        assert "gone.txt" not in revived.file_metadata
        await revived.close()

    @pytest.mark.asyncio
    async def test_file_created_while_down_is_ingested(self, sync_dir):
        engine = await restart(sync_dir)
        await engine.close()

        (Path(sync_dir) / "appeared.txt").write_text("surprise")

        revived = await restart(sync_dir)
        assert "appeared.txt" in revived.file_metadata
        await revived.close()


class TestCatalogInternalDirectoryIsExcluded:
    """The catalog must never replicate itself to peers."""

    @pytest.mark.asyncio
    async def test_dfs_directory_not_tracked(self, sync_dir):
        engine = await restart(sync_dir)
        (Path(sync_dir) / "real.txt").write_text("data")
        await engine.on_file_changed("real.txt")

        tracked = set(engine.file_metadata)
        assert tracked == {"real.txt"}
        assert not any(p.startswith(".dfs") for p in await engine.catalog.all_paths())
        await engine.close()

    @pytest.mark.asyncio
    async def test_change_events_for_internal_dir_ignored(self, sync_dir):
        engine = await restart(sync_dir)
        await engine.on_file_changed(".dfs/catalog.db")
        assert ".dfs/catalog.db" not in engine.file_metadata
        await engine.close()
