"""
Tombstone lifetime and garbage collection.

Deletion in an eventually consistent system is not "remove the row". A peer
that missed the delete re-advertises the file and it resurrects, so the delete
has to be represented by something that can out-vote a stale advertisement.
That is the tombstone. It cannot be kept forever either, or the catalog grows
without bound for the lifetime of the node.

The compromise, and its cost, are stated in docs/FUSE_DESIGN.md section 5 and
pinned here.
"""

import shutil
import tempfile
import time
from pathlib import Path

import pytest

from app.algorithms.vector_clock import ClockRelation, VectorClock
from app.core.catalog import (
    DEFAULT_TOMBSTONE_HARD_TTL_DAYS,
    DEFAULT_TOMBSTONE_TTL_DAYS,
    Catalog,
)


@pytest.fixture
async def catalog():
    """An open catalog on a temporary database."""
    directory = tempfile.mkdtemp(prefix="dfs_tomb_")
    store = Catalog(str(Path(directory) / "catalog.db"))
    await store.open()
    yield store
    await store.close()
    shutil.rmtree(directory, ignore_errors=True)


DAY = 86400.0


class TestTombstoneCreation:
    """A delete must leave a durable, clock-carrying marker."""

    @pytest.mark.asyncio
    async def test_delete_writes_a_tombstone(self, catalog):
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.record_file("gone.txt", size=3, mtime=1.0, mode=0o100644,
                                  chunk_hashes=["ab"], root_hash="ab", clock=clock)

        clock.increment()
        await catalog.delete_file("gone.txt", clock)

        assert await catalog.get_file("gone.txt") is None
        tombstone = await catalog.get_tombstone("gone.txt", "node_a")
        assert tombstone is not None
        assert tombstone.clock.clock["node_a"] == 2

    @pytest.mark.asyncio
    async def test_tombstone_clock_outranks_the_stale_advertisement(self, catalog):
        """This is the property that stops resurrection."""
        clock = VectorClock("node_a")
        clock.increment()
        stale_peer_view = clock.copy()

        clock.increment()
        await catalog.delete_file("gone.txt", clock)

        tombstone = await catalog.get_tombstone("gone.txt", "node_a")
        assert tombstone.clock.compare(stale_peer_view) == ClockRelation.AFTER

    @pytest.mark.asyncio
    async def test_recreating_a_path_clears_its_tombstone(self, catalog):
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.delete_file("phoenix.txt", clock)
        assert await catalog.get_tombstone("phoenix.txt", "node_a") is not None

        clock.increment()
        await catalog.record_file("phoenix.txt", size=5, mtime=2.0, mode=0o100644,
                                  chunk_hashes=["cd"], root_hash="cd", clock=clock)

        assert await catalog.get_tombstone("phoenix.txt", "node_a") is None
        assert await catalog.get_file("phoenix.txt") is not None

    @pytest.mark.asyncio
    async def test_inode_is_retained_across_a_tombstone(self, catalog):
        """
        Deleting and recreating a path must reuse its inode number, and the
        number must never be handed to a different path.
        """
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.record_file("keep.txt", size=1, mtime=1.0, mode=0o100644,
                                  chunk_hashes=["a"], root_hash="a", clock=clock)
        original_ino = await catalog.inode_for_path("keep.txt")

        await catalog.delete_file("keep.txt", clock)
        await catalog.record_file("keep.txt", size=2, mtime=2.0, mode=0o100644,
                                  chunk_hashes=["b"], root_hash="b", clock=clock)

        assert await catalog.inode_for_path("keep.txt") == original_ino


class TestTombstoneGarbageCollection:
    """GC must be bounded, and honest about what boundedness costs."""

    @pytest.mark.asyncio
    async def test_fresh_tombstones_are_kept(self, catalog):
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.delete_file("recent.txt", clock)

        collected = await catalog.gc_tombstones(acked_by_all_peers=["recent.txt"])
        assert collected == []
        assert await catalog.get_tombstone("recent.txt", "node_a") is not None

    @pytest.mark.asyncio
    async def test_old_and_acked_tombstone_is_collected(self, catalog):
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.delete_file("old.txt", clock)

        future = time.time() + (DEFAULT_TOMBSTONE_TTL_DAYS + 1) * DAY
        collected = await catalog.gc_tombstones(
            acked_by_all_peers=["old.txt"], now=future
        )

        assert collected == ["old.txt"]
        assert await catalog.get_tombstone("old.txt", "node_a") is None

    @pytest.mark.asyncio
    async def test_old_but_unacked_tombstone_is_kept(self, catalog):
        """
        A peer that has not acknowledged the delete may still be offline
        holding the file. Collecting early is how resurrection happens.
        """
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.delete_file("unacked.txt", clock)

        future = time.time() + (DEFAULT_TOMBSTONE_TTL_DAYS + 1) * DAY
        collected = await catalog.gc_tombstones(acked_by_all_peers=[], now=future)

        assert collected == []
        assert await catalog.get_tombstone("unacked.txt", "node_a") is not None

    @pytest.mark.asyncio
    async def test_hard_ttl_collects_even_without_acknowledgement(self, catalog):
        """
        The accepted failure mode, asserted rather than hidden: one
        permanently dead peer must not pin tombstones forever, so past the
        hard limit they are collected regardless. A peer offline longer than
        this WILL resurrect files deleted during its absence.
        """
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.delete_file("ancient.txt", clock)

        future = time.time() + (DEFAULT_TOMBSTONE_HARD_TTL_DAYS + 1) * DAY
        collected = await catalog.gc_tombstones(acked_by_all_peers=[], now=future)

        assert collected == ["ancient.txt"]

    @pytest.mark.asyncio
    async def test_gc_is_selective(self, catalog):
        clock = VectorClock("node_a")
        clock.increment()
        for name in ("a.txt", "b.txt", "c.txt"):
            await catalog.delete_file(name, clock)

        future = time.time() + (DEFAULT_TOMBSTONE_TTL_DAYS + 1) * DAY
        collected = await catalog.gc_tombstones(
            acked_by_all_peers=["b.txt"], now=future
        )

        assert collected == ["b.txt"]
        assert {t.path for t in await catalog.list_tombstones()} == {"a.txt", "c.txt"}

    @pytest.mark.asyncio
    async def test_gc_survives_a_reopen(self, catalog):
        """Tombstones are durable, not an in-memory convenience."""
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.delete_file("durable.txt", clock)

        path = catalog.db_path
        await catalog.close()

        reopened = Catalog(path)
        await reopened.open()
        try:
            assert await reopened.get_tombstone("durable.txt", "node_a") is not None
        finally:
            await reopened.close()
            await catalog.open()  # so the fixture teardown has something to close


class TestJournalRecovery:
    """Unreplicated mutations must be replayable after a crash."""

    @pytest.mark.asyncio
    async def test_mutations_are_journalled(self, catalog):
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.record_file("j.txt", size=1, mtime=1.0, mode=0o100644,
                                  chunk_hashes=["a"], root_hash="a", clock=clock)

        entries = await catalog.unapplied_journal("node_a")
        assert [e.op for e in entries] == ["write"]
        assert entries[0].path == "j.txt"

    @pytest.mark.asyncio
    async def test_marking_applied_clears_the_backlog(self, catalog):
        clock = VectorClock("node_a")
        clock.increment()
        await catalog.record_file("j.txt", size=1, mtime=1.0, mode=0o100644,
                                  chunk_hashes=["a"], root_hash="a", clock=clock)

        entries = await catalog.unapplied_journal("node_a")
        await catalog.mark_journal_applied([e.id for e in entries])

        assert await catalog.unapplied_journal("node_a") == []

    @pytest.mark.asyncio
    async def test_journal_preserves_order(self, catalog):
        clock = VectorClock("node_a")
        for index in range(5):
            clock.increment()
            await catalog.record_file(f"f{index}.txt", size=1, mtime=1.0,
                                      mode=0o100644, chunk_hashes=["a"],
                                      root_hash="a", clock=clock)

        entries = await catalog.unapplied_journal("node_a")
        assert [e.path for e in entries] == [f"f{i}.txt" for i in range(5)]
        assert entries == sorted(entries, key=lambda e: e.id)
