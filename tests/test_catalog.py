"""
Tests for the persistent metadata catalog.

Covers the Phase 1 requirements from docs/FUSE_DESIGN.md section 9:
atomic multi-table mutations, crash-mid-transaction rollback, remount
recovery that re-hashes only what actually changed, stable inode allocation
across remounts, and tombstone GC including the hard-TTL escape hatch.
"""

import asyncio
import json
import time

import pytest

from app.algorithms.vector_clock import VectorClock
from app.core.catalog import (
    Catalog,
    ROOT_INODE,
    normalize_path,
    split_path,
)


@pytest.fixture
async def catalog(tmp_path):
    """An open catalog backed by a temporary database file."""
    cat = Catalog(str(tmp_path / "catalog.db"))
    await cat.open()
    yield cat
    await cat.close()


def clock_for(node_id: str, **counters) -> VectorClock:
    """Build a vector clock with explicit counters."""
    return VectorClock(node_id, dict(counters) or {node_id: 1})


class TestPathNormalization:
    """Paths must key identically on Windows and Linux peers."""

    def test_backslashes_become_forward_slashes(self):
        assert normalize_path(r"docs\notes\a.txt") == "docs/notes/a.txt"

    def test_leading_and_trailing_separators_stripped(self):
        assert normalize_path("/docs/a.txt/") == "docs/a.txt"

    def test_dot_segments_collapsed(self):
        assert normalize_path("./docs/./a.txt") == "docs/a.txt"

    def test_root_is_empty_string(self):
        assert normalize_path("") == ""
        assert normalize_path(".") == ""
        assert normalize_path("/") == ""

    def test_split_path_top_level(self):
        assert split_path("a.txt") == ("", "a.txt")

    def test_split_path_nested(self):
        assert split_path("docs/sub/a.txt") == ("docs/sub", "a.txt")


class TestSchemaAndPragmas:
    """WAL is required so the FUSE thread can read while the engine writes."""

    @pytest.mark.asyncio
    async def test_wal_mode_enabled(self, catalog):
        async with catalog.db.execute("PRAGMA journal_mode") as cursor:
            row = await cursor.fetchone()
        assert row[0].lower() == "wal"

    @pytest.mark.asyncio
    async def test_expected_tables_exist(self, catalog):
        async with catalog.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ) as cursor:
            rows = await cursor.fetchall()
        names = {row["name"] for row in rows}

        for table in ("files", "chunks", "clocks", "tombstones",
                      "inodes", "sync_journal"):
            assert table in names

    @pytest.mark.asyncio
    async def test_expected_indexes_exist(self, catalog):
        async with catalog.db.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ) as cursor:
            rows = await cursor.fetchall()
        names = {row["name"] for row in rows}

        assert "idx_files_parent" in names
        assert "idx_tombstones_deleted_at" in names
        assert "idx_journal_applied" in names

    @pytest.mark.asyncio
    async def test_opening_twice_is_idempotent(self, tmp_path):
        """Schema creation must be safe on every start, not just the first."""
        path = str(tmp_path / "c.db")
        for _ in range(3):
            cat = Catalog(path)
            await cat.open()
            await cat.close()


class TestFileRecords:
    """Basic durability of file metadata."""

    @pytest.mark.asyncio
    async def test_record_and_read_back(self, catalog):
        await catalog.record_file(
            "docs/a.txt", size=9, mtime=123.5, mode=0o100644,
            chunk_hashes=["h0", "h1"], root_hash="root",
            clock=clock_for("n1", n1=1),
        )

        record = await catalog.get_file("docs/a.txt")
        assert record is not None
        assert record.size == 9
        assert record.mtime == 123.5
        assert record.chunk_count == 2
        assert record.root_hash == "root"
        assert record.parent == "docs"
        assert record.name == "a.txt"
        assert not record.is_dir

    @pytest.mark.asyncio
    async def test_chunk_hashes_round_trip_in_order(self, catalog):
        hashes = [f"h{i}" for i in range(10)]
        await catalog.record_file(
            "a.bin", size=40960, mtime=1.0, mode=0o100644,
            chunk_hashes=hashes, root_hash="r", clock=clock_for("n1"),
        )
        assert await catalog.get_chunk_hashes("a.bin") == hashes

    @pytest.mark.asyncio
    async def test_update_replaces_chunks_not_appends(self, catalog):
        await catalog.record_file(
            "a.bin", size=3, mtime=1.0, mode=0o100644,
            chunk_hashes=["x", "y", "z"], root_hash="r1", clock=clock_for("n1"),
        )
        await catalog.record_file(
            "a.bin", size=1, mtime=2.0, mode=0o100644,
            chunk_hashes=["w"], root_hash="r2", clock=clock_for("n1", n1=2),
        )

        assert await catalog.get_chunk_hashes("a.bin") == ["w"]
        record = await catalog.get_file("a.bin")
        assert record.chunk_count == 1

    @pytest.mark.asyncio
    async def test_windows_and_posix_paths_are_the_same_row(self, catalog):
        await catalog.record_file(
            r"docs\a.txt", size=1, mtime=1.0, mode=0o100644,
            chunk_hashes=["h"], root_hash="r", clock=clock_for("n1"),
        )
        assert await catalog.get_file("docs/a.txt") is not None

    @pytest.mark.asyncio
    async def test_missing_file_returns_none(self, catalog):
        assert await catalog.get_file("nope.txt") is None

    @pytest.mark.asyncio
    async def test_list_children_is_direct_only(self, catalog):
        for path in ("a.txt", "docs/b.txt", "docs/c.txt", "docs/sub/d.txt"):
            await catalog.record_file(
                path, size=1, mtime=1.0, mode=0o100644,
                chunk_hashes=["h"], root_hash="r", clock=clock_for("n1"),
            )

        names = [record.name for record in await catalog.list_children("docs")]
        assert names == ["b.txt", "c.txt"]

        top = [record.name for record in await catalog.list_children("")]
        assert top == ["a.txt"]


class TestClocks:
    """Vector clocks must survive a restart, or every restart looks new."""

    @pytest.mark.asyncio
    async def test_clock_round_trip(self, catalog):
        clock = VectorClock("n1", {"n1": 3, "n2": 7})
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644,
            chunk_hashes=["h"], root_hash="r", clock=clock,
        )

        restored = await catalog.get_clock("a.txt", "n1")
        assert restored is not None
        assert restored.clock == {"n1": 3, "n2": 7}

    @pytest.mark.asyncio
    async def test_clock_update_replaces_all_peers(self, catalog):
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=VectorClock("n1", {"n1": 1, "n2": 1, "n3": 1}),
        )
        await catalog.record_file(
            "a.txt", size=1, mtime=2.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=VectorClock("n1", {"n1": 2}),
        )

        restored = await catalog.get_clock("a.txt", "n1")
        assert restored.clock == {"n1": 2}

    @pytest.mark.asyncio
    async def test_clock_survives_reopen(self, tmp_path):
        db = str(tmp_path / "c.db")

        cat = Catalog(db)
        await cat.open()
        await cat.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=VectorClock("n1", {"n1": 5, "n2": 2}),
        )
        await cat.close()

        cat = Catalog(db)
        await cat.open()
        restored = await cat.get_clock("a.txt", "n1")
        await cat.close()

        assert restored.clock == {"n1": 5, "n2": 2}


class TestAtomicity:
    """A partially applied vector clock is corruption."""

    @pytest.mark.asyncio
    async def test_failed_mutation_rolls_back_completely(self, catalog):
        """Crash mid-transaction: no file row, no chunks, no clock, no journal."""
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["old"],
            root_hash="r1", clock=VectorClock("n1", {"n1": 1}),
        )

        original_executemany = catalog.db.executemany
        calls = {"n": 0}

        async def explode(sql, params):
            # Fail partway through: after the file row and the chunk rows are
            # written, but before the clock rows land.
            calls["n"] += 1
            if "clocks" in sql:
                raise RuntimeError("simulated crash mid-transaction")
            return await original_executemany(sql, params)

        catalog.db.executemany = explode
        try:
            with pytest.raises(RuntimeError, match="simulated crash"):
                await catalog.record_file(
                    "a.txt", size=999, mtime=99.0, mode=0o100644,
                    chunk_hashes=["new1", "new2"], root_hash="r2",
                    clock=VectorClock("n1", {"n1": 2, "n2": 4}),
                )
        finally:
            catalog.db.executemany = original_executemany

        # Everything must still be the pre-crash state.
        record = await catalog.get_file("a.txt")
        assert record.size == 1, "file row survived a rolled-back transaction"
        assert record.root_hash == "r1"
        assert await catalog.get_chunk_hashes("a.txt") == ["old"]
        assert (await catalog.get_clock("a.txt", "n1")).clock == {"n1": 1}

    @pytest.mark.asyncio
    async def test_rollback_leaves_database_usable(self, catalog):
        """After a rollback the connection must not be stuck in a transaction."""
        original = catalog.db.executemany

        async def explode(sql, params):
            raise RuntimeError("boom")

        catalog.db.executemany = explode
        try:
            with pytest.raises(RuntimeError):
                await catalog.record_file(
                    "x.txt", size=1, mtime=1.0, mode=0o100644,
                    chunk_hashes=["a"], root_hash="r", clock=clock_for("n1"),
                )
        finally:
            catalog.db.executemany = original

        await catalog.record_file(
            "y.txt", size=1, mtime=1.0, mode=0o100644,
            chunk_hashes=["a"], root_hash="r", clock=clock_for("n1"),
        )
        assert await catalog.get_file("y.txt") is not None

    @pytest.mark.asyncio
    async def test_journal_entry_written_with_the_data(self, catalog):
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=VectorClock("n1", {"n1": 1}),
        )

        entries = await catalog.unapplied_journal("n1")
        assert len(entries) == 1
        assert entries[0].op == "write"
        assert entries[0].path == "a.txt"
        assert entries[0].clock.clock == {"n1": 1}

    @pytest.mark.asyncio
    async def test_marking_journal_applied(self, catalog):
        for name in ("a.txt", "b.txt"):
            await catalog.record_file(
                name, size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
                root_hash="r", clock=clock_for("n1"),
            )

        entries = await catalog.unapplied_journal("n1")
        assert len(entries) == 2

        await catalog.mark_journal_applied([entries[0].id])
        remaining = await catalog.unapplied_journal("n1")
        assert [entry.path for entry in remaining] == ["b.txt"]


class TestInodes:
    """The same path must keep its inode across remounts."""

    @pytest.mark.asyncio
    async def test_root_inode_is_one(self, catalog):
        assert await catalog.inode_for_path("") == ROOT_INODE
        assert await catalog.path_for_inode(ROOT_INODE) == ""

    @pytest.mark.asyncio
    async def test_inode_is_stable_within_a_session(self, catalog):
        first = await catalog.inode_for_path("a.txt")
        second = await catalog.inode_for_path("a.txt")
        assert first == second

    @pytest.mark.asyncio
    async def test_inodes_are_unique(self, catalog):
        inos = {await catalog.inode_for_path(f"f{i}.txt") for i in range(50)}
        assert len(inos) == 50
        assert ROOT_INODE not in inos

    @pytest.mark.asyncio
    async def test_inode_survives_remount(self, tmp_path):
        db = str(tmp_path / "c.db")

        cat = Catalog(db)
        await cat.open()
        before = await cat.inode_for_path("docs/a.txt")
        await cat.close()

        cat = Catalog(db)
        await cat.open()
        after = await cat.inode_for_path("docs/a.txt")
        await cat.close()

        assert before == after

    @pytest.mark.asyncio
    async def test_inode_survives_delete_and_recreate(self, catalog):
        """A tombstone retains the inode so a recreate reuses it."""
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        before = await catalog.inode_for_path("a.txt")

        await catalog.delete_file("a.txt", VectorClock("n1", {"n1": 2}))
        await catalog.record_file(
            "a.txt", size=2, mtime=2.0, mode=0o100644, chunk_hashes=["h2"],
            root_hash="r2", clock=VectorClock("n1", {"n1": 3}),
        )

        assert await catalog.inode_for_path("a.txt") == before

    @pytest.mark.asyncio
    async def test_inode_not_allocated_when_disabled(self, catalog):
        assert await catalog.inode_for_path("ghost.txt", allocate=False) is None

    @pytest.mark.asyncio
    async def test_reverse_lookup(self, catalog):
        ino = await catalog.inode_for_path("docs/a.txt")
        assert await catalog.path_for_inode(ino) == "docs/a.txt"

    @pytest.mark.asyncio
    async def test_concurrent_allocation_is_consistent(self, catalog):
        """Concurrent allocation of the same path must not double-assign."""
        results = await asyncio.gather(
            *[catalog.inode_for_path("shared.txt") for _ in range(20)]
        )
        assert len(set(results)) == 1


class TestDeletionAndTombstones:
    """Without tombstones, deletes resurrect."""

    @pytest.mark.asyncio
    async def test_delete_removes_rows_and_writes_tombstone(self, catalog):
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=VectorClock("n1", {"n1": 1}),
        )

        await catalog.delete_file("a.txt", VectorClock("n1", {"n1": 2}))

        assert await catalog.get_file("a.txt") is None
        assert await catalog.get_chunk_hashes("a.txt") == []
        assert await catalog.get_clock("a.txt", "n1") is None

        tombstone = await catalog.get_tombstone("a.txt", "n1")
        assert tombstone is not None
        assert tombstone.clock.clock == {"n1": 2}

    @pytest.mark.asyncio
    async def test_delete_writes_journal_entry(self, catalog):
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        await catalog.mark_journal_applied(
            [entry.id for entry in await catalog.unapplied_journal("n1")]
        )

        await catalog.delete_file("a.txt", VectorClock("n1", {"n1": 2}))

        entries = await catalog.unapplied_journal("n1")
        assert [entry.op for entry in entries] == ["unlink"]

    @pytest.mark.asyncio
    async def test_recreating_clears_the_tombstone(self, catalog):
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        await catalog.delete_file("a.txt", VectorClock("n1", {"n1": 2}))
        assert await catalog.get_tombstone("a.txt") is not None

        await catalog.record_file(
            "a.txt", size=1, mtime=3.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=VectorClock("n1", {"n1": 3}),
        )
        assert await catalog.get_tombstone("a.txt") is None

    @pytest.mark.asyncio
    async def test_tombstone_survives_reopen(self, tmp_path):
        db = str(tmp_path / "c.db")

        cat = Catalog(db)
        await cat.open()
        await cat.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        await cat.delete_file("a.txt", VectorClock("n1", {"n1": 2}))
        await cat.close()

        cat = Catalog(db)
        await cat.open()
        tombstone = await cat.get_tombstone("a.txt", "n1")
        await cat.close()

        assert tombstone is not None
        assert tombstone.clock.clock == {"n1": 2}


class TestTombstoneGC:
    """GC must not collect a tombstone a peer still needs."""

    @pytest.mark.asyncio
    async def test_fresh_tombstone_is_not_collected(self, catalog):
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        await catalog.delete_file("a.txt", VectorClock("n1", {"n1": 2}))

        collected = await catalog.gc_tombstones(acked_by_all_peers=["a.txt"])
        assert collected == []
        assert await catalog.get_tombstone("a.txt") is not None

    @pytest.mark.asyncio
    async def test_expired_and_acked_is_collected(self, catalog):
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        await catalog.delete_file("a.txt", VectorClock("n1", {"n1": 2}))

        future = time.time() + (catalog.tombstone_ttl_days + 1) * 86400
        collected = await catalog.gc_tombstones(
            acked_by_all_peers=["a.txt"], now=future
        )

        assert collected == ["a.txt"]
        assert await catalog.get_tombstone("a.txt") is None

    @pytest.mark.asyncio
    async def test_expired_but_unacked_is_retained(self, catalog):
        """A peer that has not acknowledged the delete still needs it."""
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        await catalog.delete_file("a.txt", VectorClock("n1", {"n1": 2}))

        future = time.time() + (catalog.tombstone_ttl_days + 1) * 86400
        collected = await catalog.gc_tombstones(acked_by_all_peers=[], now=future)

        assert collected == []
        assert await catalog.get_tombstone("a.txt") is not None

    @pytest.mark.asyncio
    async def test_hard_ttl_collects_without_acknowledgement(self, catalog):
        """
        The escape hatch: a permanently dead peer must not pin tombstones
        forever. Accepted cost is that such a peer, if it ever returns, will
        resurrect files deleted during its absence.
        """
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        await catalog.delete_file("a.txt", VectorClock("n1", {"n1": 2}))

        far_future = time.time() + (catalog.tombstone_hard_ttl_days + 1) * 86400
        collected = await catalog.gc_tombstones(
            acked_by_all_peers=[], now=far_future
        )

        assert collected == ["a.txt"]

    @pytest.mark.asyncio
    async def test_gc_only_touches_expired_entries(self, catalog):
        for name in ("old.txt", "new.txt"):
            await catalog.record_file(
                name, size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
                root_hash="r", clock=clock_for("n1"),
            )

        await catalog.delete_file("old.txt", VectorClock("n1", {"n1": 2}))
        # Backdate the first tombstone well past the hard TTL.
        await catalog.db.execute(
            "UPDATE tombstones SET deleted_at = ? WHERE path = 'old.txt'",
            (time.time() - (catalog.tombstone_hard_ttl_days + 5) * 86400,),
        )
        await catalog.delete_file("new.txt", VectorClock("n1", {"n1": 2}))

        collected = await catalog.gc_tombstones(acked_by_all_peers=[])
        assert collected == ["old.txt"]
        assert await catalog.get_tombstone("new.txt") is not None


class TestRename:
    """Rename must move every table together or none of them."""

    @pytest.mark.asyncio
    async def test_rename_moves_metadata(self, catalog):
        await catalog.record_file(
            "old.txt", size=7, mtime=1.0, mode=0o100644,
            chunk_hashes=["h0", "h1"], root_hash="r",
            clock=VectorClock("n1", {"n1": 1}),
        )
        ino = await catalog.inode_for_path("old.txt")

        await catalog.rename("old.txt", "docs/new.txt", VectorClock("n1", {"n1": 2}))

        assert await catalog.get_file("old.txt") is None
        moved = await catalog.get_file("docs/new.txt")
        assert moved is not None
        assert moved.size == 7
        assert moved.parent == "docs"
        assert moved.name == "new.txt"
        assert await catalog.get_chunk_hashes("docs/new.txt") == ["h0", "h1"]
        assert (await catalog.get_clock("docs/new.txt", "n1")).clock == {"n1": 1}

    @pytest.mark.asyncio
    async def test_rename_preserves_local_inode(self, catalog):
        await catalog.record_file(
            "old.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        before = await catalog.inode_for_path("old.txt")

        await catalog.rename("old.txt", "new.txt", VectorClock("n1", {"n1": 2}))

        assert await catalog.inode_for_path("new.txt") == before
        assert await catalog.path_for_inode(before) == "new.txt"

    @pytest.mark.asyncio
    async def test_rename_onto_existing_replaces_it(self, catalog):
        await catalog.record_file(
            "src.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["src"],
            root_hash="r1", clock=clock_for("n1"),
        )
        await catalog.record_file(
            "dst.txt", size=2, mtime=1.0, mode=0o100644, chunk_hashes=["dst"],
            root_hash="r2", clock=clock_for("n1"),
        )

        await catalog.rename("src.txt", "dst.txt", VectorClock("n1", {"n1": 2}))

        assert await catalog.get_file("src.txt") is None
        assert await catalog.get_chunk_hashes("dst.txt") == ["src"]

    @pytest.mark.asyncio
    async def test_rename_writes_journal_entry(self, catalog):
        await catalog.record_file(
            "a.txt", size=1, mtime=1.0, mode=0o100644, chunk_hashes=["h"],
            root_hash="r", clock=clock_for("n1"),
        )
        await catalog.mark_journal_applied(
            [entry.id for entry in await catalog.unapplied_journal("n1")]
        )

        await catalog.rename("a.txt", "b.txt", VectorClock("n1", {"n1": 2}))

        entries = await catalog.unapplied_journal("n1")
        assert len(entries) == 1
        assert entries[0].op == "rename"
        assert entries[0].path.split("\x00") == ["a.txt", "b.txt"]


class TestReconcile:
    """Remount recovery: re-hash only what actually changed."""

    async def _seed(self, catalog, root, files):
        """Write files to disk and record them accurately in the catalog."""
        for name, content in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            stat = path.stat()
            await catalog.record_file(
                name, size=stat.st_size, mtime=stat.st_mtime,
                mode=stat.st_mode, chunk_hashes=["seed"], root_hash="seed",
                clock=clock_for("n1"),
            )

    @pytest.mark.asyncio
    async def test_unchanged_files_are_not_rehashed(self, catalog, tmp_path):
        root = tmp_path / "sync"
        root.mkdir()
        await self._seed(catalog, root, {"a.txt": b"aaa", "b.txt": b"bbb"})

        hashed = []

        async def tracking_hasher(path):
            hashed.append(path)
            return "new", ["new"]

        report = await catalog.reconcile(str(root), "n1", hasher=tracking_hasher)

        assert hashed == [], "unchanged files were re-hashed"
        assert sorted(report.unchanged) == ["a.txt", "b.txt"]
        assert report.rehashed == []

    @pytest.mark.asyncio
    async def test_changed_file_is_rehashed(self, catalog, tmp_path):
        root = tmp_path / "sync"
        root.mkdir()
        await self._seed(catalog, root, {"a.txt": b"aaa", "b.txt": b"bbb"})

        # Change one file's size and mtime.
        changed = root / "b.txt"
        changed.write_bytes(b"bbbbbbbbbb")

        hashed = []

        async def tracking_hasher(path):
            hashed.append(path)
            return "new", ["new"]

        report = await catalog.reconcile(str(root), "n1", hasher=tracking_hasher)

        assert report.unchanged == ["a.txt"]
        assert report.rehashed == ["b.txt"]
        assert len(hashed) == 1
        assert await catalog.get_chunk_hashes("b.txt") == ["new"]

    @pytest.mark.asyncio
    async def test_new_file_on_disk_is_ingested(self, catalog, tmp_path):
        root = tmp_path / "sync"
        root.mkdir()
        await self._seed(catalog, root, {"a.txt": b"aaa"})
        (root / "fresh.txt").write_bytes(b"new content")

        async def hasher(path):
            return "h", ["h"]

        report = await catalog.reconcile(str(root), "n1", hasher=hasher)

        assert report.ingested == ["fresh.txt"]
        assert await catalog.get_file("fresh.txt") is not None

    @pytest.mark.asyncio
    async def test_vanished_file_becomes_a_tombstone(self, catalog, tmp_path):
        """Deleted while we were down: the delete must still propagate."""
        root = tmp_path / "sync"
        root.mkdir()
        await self._seed(catalog, root, {"a.txt": b"aaa", "gone.txt": b"x"})
        (root / "gone.txt").unlink()

        async def hasher(path):
            return "h", ["h"]

        report = await catalog.reconcile(str(root), "n1", hasher=hasher)

        assert report.vanished == ["gone.txt"]
        assert await catalog.get_file("gone.txt") is None
        assert await catalog.get_tombstone("gone.txt") is not None

    @pytest.mark.asyncio
    async def test_directories_are_recorded(self, catalog, tmp_path):
        root = tmp_path / "sync"
        (root / "docs" / "sub").mkdir(parents=True)
        (root / "docs" / "sub" / "deep.txt").write_bytes(b"z")

        async def hasher(path):
            return "h", ["h"]

        report = await catalog.reconcile(str(root), "n1", hasher=hasher)

        assert "docs" in report.dirs
        assert "docs/sub" in report.dirs

        docs = await catalog.get_file("docs")
        assert docs is not None and docs.is_dir

    @pytest.mark.asyncio
    async def test_ignore_callback_is_respected(self, catalog, tmp_path):
        root = tmp_path / "sync"
        root.mkdir()
        (root / "keep.txt").write_bytes(b"a")
        (root / "skip.tmp").write_bytes(b"b")

        async def hasher(path):
            return "h", ["h"]

        await catalog.reconcile(
            str(root), "n1", hasher=hasher,
            ignore=lambda rel: rel.endswith(".tmp"),
        )

        assert await catalog.get_file("keep.txt") is not None
        assert await catalog.get_file("skip.tmp") is None

    @pytest.mark.asyncio
    async def test_first_run_builds_catalog_from_existing_tree(self, tmp_path):
        """Migration path: an existing daemon user starts with no catalog."""
        root = tmp_path / "sync"
        (root / "docs").mkdir(parents=True)
        (root / "docs" / "a.txt").write_bytes(b"hello")
        (root / "b.txt").write_bytes(b"world")

        cat = Catalog(str(tmp_path / "catalog.db"))
        await cat.open()
        try:
            report = await cat.reconcile(str(root), "n1")

            assert sorted(report.ingested) == ["b.txt", "docs/a.txt"]
            record = await cat.get_file("docs/a.txt")
            assert record is not None
            assert record.size == 5
            assert record.root_hash is not None
            assert len(await cat.get_chunk_hashes("docs/a.txt")) == 1
        finally:
            await cat.close()

    @pytest.mark.asyncio
    async def test_reconcile_uses_real_merkle_hashes_by_default(self, tmp_path):
        """The default hasher must agree with MerkleTree."""
        from app.algorithms.merkle import MerkleTree

        root = tmp_path / "sync"
        root.mkdir()
        (root / "a.bin").write_bytes(b"Q" * 9000)

        cat = Catalog(str(tmp_path / "catalog.db"))
        await cat.open()
        try:
            await cat.reconcile(str(root), "n1")
            record = await cat.get_file("a.bin")
            tree = await MerkleTree.from_file(str(root / "a.bin"))

            assert record.root_hash == tree.root.hash
            assert record.chunk_count == 3
            assert len(await cat.get_chunk_hashes("a.bin")) == 3
        finally:
            await cat.close()

    @pytest.mark.asyncio
    async def test_remount_after_reconcile_is_cheap(self, tmp_path):
        """
        The point of the catalog: a second mount of an unchanged tree
        re-hashes nothing at all.
        """
        root = tmp_path / "sync"
        root.mkdir()
        for i in range(5):
            (root / f"f{i}.bin").write_bytes(bytes([i]) * 5000)

        db = str(tmp_path / "catalog.db")

        cat = Catalog(db)
        await cat.open()
        await cat.reconcile(str(root), "n1")
        await cat.close()

        hashed = []

        async def tracking_hasher(path):
            hashed.append(path)
            return "x", ["x"]

        cat = Catalog(db)
        await cat.open()
        try:
            report = await cat.reconcile(str(root), "n1", hasher=tracking_hasher)
        finally:
            await cat.close()

        assert hashed == []
        assert len(report.unchanged) == 5
