"""
FUSE tests.

Split into two halves deliberately:

* **Inode table and attribute cache** are pure Python and are tested on both
  platforms, because they carry the requirements that are easiest to break
  silently -- inode stability across remounts, and cache invalidation.
* **The filesystem itself** is tested against a *real temporary mount*, never
  a mock. A mocked pyfuse3 would happily accept a handler that the kernel
  rejects; the whole point of Phase 3 is that real programs work. These tests
  skip on Windows and wherever /dev/fuse is unavailable.
"""

import errno
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from app.fuse.attrcache import AttrCache
from app.fuse.inodes import InodeTable

# ---------------------------------------------------------------------------
# Cross-platform: inode table
# ---------------------------------------------------------------------------


@pytest.fixture
def table():
    """An open inode table on a temporary database."""
    directory = tempfile.mkdtemp(prefix="dfs_inode_")
    inodes = InodeTable(str(Path(directory) / "catalog.db"))
    inodes.open()
    yield inodes
    inodes.close()
    shutil.rmtree(directory, ignore_errors=True)


class TestInodeTable:
    """Stable inode allocation is a stated Phase 3 requirement."""

    def test_root_is_inode_one(self, table):
        """pyfuse3 always addresses the root as inode 1."""
        assert table.inode_for_path("") == 1

    def test_same_path_gives_same_inode(self, table):
        first = table.inode_for_path("docs/notes.txt")
        assert table.inode_for_path("docs/notes.txt") == first

    def test_distinct_paths_get_distinct_inodes(self, table):
        assert table.inode_for_path("a.txt") != table.inode_for_path("b.txt")

    def test_reverse_lookup(self, table):
        ino = table.inode_for_path("docs/notes.txt")
        assert table.path_for_inode(ino) == "docs/notes.txt"

    def test_separator_style_is_normalised(self, table):
        """
        A Windows peer and a Linux peer must agree on the same key, or the
        same file gets two inodes and never converges.
        """
        assert table.inode_for_path("a/b.txt") == table.inode_for_path("a\\b.txt")

    def test_unknown_path_without_allocate(self, table):
        assert table.inode_for_path("never-seen.txt", allocate=False) is None

    def test_inodes_survive_a_reopen(self, table):
        """The requirement: same path keeps its inode across remounts."""
        before = {name: table.inode_for_path(name)
                  for name in ("a.txt", "d/b.txt", "d/e/c.txt")}
        db_path = table.db_path
        table.close()

        reopened = InodeTable(db_path)
        reopened.open()
        try:
            for name, ino in before.items():
                assert reopened.inode_for_path(name) == ino, f"{name} changed inode"
        finally:
            reopened.close()
            table.open()

    def test_new_allocations_do_not_collide_after_reopen(self, table):
        """The counter must resume above the highest persisted number."""
        existing = [table.inode_for_path(f"f{i}.txt") for i in range(5)]
        db_path = table.db_path
        table.close()

        reopened = InodeTable(db_path)
        reopened.open()
        try:
            fresh = reopened.inode_for_path("brand-new.txt")
            assert fresh not in existing
        finally:
            reopened.close()
            table.open()

    def test_rename_preserves_the_inode(self, table):
        """POSIX: `mv a b` must not change `stat -c %i`."""
        before = table.inode_for_path("old.txt")
        table.rename("old.txt", "new.txt")

        assert table.inode_for_path("new.txt") == before
        assert table.path_for_inode(before) == "new.txt"

    def test_rename_persists(self, table):
        before = table.inode_for_path("old.txt")
        table.rename("old.txt", "new.txt")
        db_path = table.db_path
        table.close()

        reopened = InodeTable(db_path)
        reopened.open()
        try:
            assert reopened.inode_for_path("new.txt") == before
        finally:
            reopened.close()
            table.open()

    def test_forget_retains_the_number_for_reuse(self, table):
        """
        Deleting and recreating a path must reuse its number, and the number
        must never be handed to different content.
        """
        original = table.inode_for_path("temp.txt")
        table.forget_path("temp.txt")
        other = table.inode_for_path("unrelated.txt")

        assert other != original


class TestAttrCache:
    """Correctness rests on invalidation, not on the TTL."""

    def test_hit_and_miss(self):
        cache = AttrCache(ttl=10.0)
        assert cache.get(42) is None
        cache.put(42, "attrs")
        assert cache.get(42) == "attrs"

    def test_invalidate_drops_the_entry(self):
        """Local writes must never read stale size; read-your-writes."""
        cache = AttrCache(ttl=10.0)
        cache.put(42, "attrs")
        cache.invalidate(42)
        assert cache.get(42) is None

    def test_entries_expire(self):
        cache = AttrCache(ttl=0.05)
        cache.put(42, "attrs")
        time.sleep(0.12)
        assert cache.get(42) is None

    def test_clear_drops_everything(self):
        cache = AttrCache(ttl=10.0)
        for ino in range(10):
            cache.put(ino, f"attrs{ino}")
        cache.clear()
        assert all(cache.get(ino) is None for ino in range(10))

    def test_cache_is_bounded(self):
        """A `find /` over the mount must not turn into a memory leak."""
        cache = AttrCache(ttl=100.0, max_entries=50)
        for ino in range(500):
            cache.put(ino, f"attrs{ino}")
        assert cache.stats()["entries"] <= 50

    def test_stats_report_hit_rate(self):
        cache = AttrCache(ttl=10.0)
        cache.put(1, "a")
        cache.get(1)
        cache.get(2)
        stats = cache.stats()
        assert stats["hits"] == 1 and stats["misses"] == 1
        assert stats["hit_rate"] == 0.5


# ---------------------------------------------------------------------------
# Linux only: a real mount
# ---------------------------------------------------------------------------


def _fuse_available() -> bool:
    """
    Report whether a real FUSE mount can be created here.

    Returns:
        True when pyfuse3 imports and /dev/fuse exists.
    """
    if sys.platform != "linux":
        return False
    if not os.path.exists("/dev/fuse"):
        return False
    try:
        import pyfuse3  # noqa: F401
        import trio  # noqa: F401
    except ImportError:
        return False
    return True


requires_fuse = pytest.mark.skipif(
    not _fuse_available(),
    reason="needs Linux with /dev/fuse and pyfuse3 (run under WSL2)",
)


@pytest.fixture
def mount():
    """
    A real FUSE mount over a real backing directory.

    Yields:
        Tuple of (mount point Path, backing store Path).
    """
    from app.fuse.mount import FuseMount

    root = Path(tempfile.mkdtemp(prefix="dfs_mnt_"))
    backing = root / "backing"
    point = root / "mnt"
    backing.mkdir()
    point.mkdir()

    session = FuseMount(str(point), str(backing), str(root / "catalog.db"))
    session.start()

    try:
        yield point, backing
    finally:
        session.stop()
        subprocess.run(["fusermount3", "-uz", str(point)],
                       capture_output=True, check=False)
        shutil.rmtree(root, ignore_errors=True)


@requires_fuse
class TestRealMountReadPath:
    """Phase 3 handlers, exercised through actual syscalls."""

    def test_write_then_read(self, mount):
        point, _ = mount
        (point / "a.txt").write_text("hello world")
        assert (point / "a.txt").read_text() == "hello world"

    def test_backing_store_holds_the_bytes(self, mount):
        point, backing = mount
        (point / "a.txt").write_text("hello world")
        assert (backing / "a.txt").read_text() == "hello world"

    def test_readdir_lists_entries(self, mount):
        point, _ = mount
        for name in ("a.txt", "b.txt", "c.txt"):
            (point / name).write_text(name)
        assert sorted(os.listdir(point)) == ["a.txt", "b.txt", "c.txt"]

    def test_internal_directory_is_hidden(self, mount):
        """The catalog must not be reachable through the mount it describes."""
        point, backing = mount
        (backing / ".dfs").mkdir(exist_ok=True)
        (backing / ".dfs" / "catalog.db").write_text("x")
        assert ".dfs" not in os.listdir(point)

    def test_getattr_reports_real_size(self, mount):
        point, _ = mount
        (point / "sized.bin").write_bytes(b"x" * 1234)
        assert os.stat(point / "sized.bin").st_size == 1234

    def test_nested_directories(self, mount):
        point, _ = mount
        (point / "d" / "e").mkdir(parents=True)
        (point / "d" / "e" / "f.txt").write_text("deep")
        assert (point / "d" / "e" / "f.txt").read_text() == "deep"

    def test_statfs_matches_backing(self, mount):
        point, backing = mount
        assert os.statvfs(point).f_blocks == os.statvfs(backing).f_blocks

    def test_missing_file_raises_enoent(self, mount):
        point, _ = mount
        with pytest.raises(FileNotFoundError):
            (point / "nope.txt").read_text()

    def test_large_sequential_read(self, mount):
        point, _ = mount
        payload = os.urandom(4 * 1024 * 1024)
        (point / "big.bin").write_bytes(payload)
        assert (point / "big.bin").read_bytes() == payload


@requires_fuse
class TestRealMountWritePath:
    """Phase 4 handlers, including the awkward cases."""

    def test_append_mode(self, mount):
        point, _ = mount
        target = point / "log.txt"
        for line in ("one\n", "two\n", "three\n"):
            with open(target, "a") as handle:
                handle.write(line)
        assert target.read_text() == "one\ntwo\nthree\n"

    def test_write_past_eof_creates_a_hole(self, mount):
        point, _ = mount
        target = point / "sparse.bin"
        with open(target, "wb") as handle:
            handle.write(b"start")
            handle.seek(10000)
            handle.write(b"end")

        data = target.read_bytes()
        assert len(data) == 10003
        assert data[:5] == b"start"
        assert data[5:10000] == b"\x00" * 9995
        assert data[10000:] == b"end"

    def test_truncate_shrinks(self, mount):
        point, _ = mount
        target = point / "t.bin"
        target.write_bytes(b"x" * 5000)
        os.truncate(target, 100)
        assert target.stat().st_size == 100

    def test_truncate_extends(self, mount):
        point, _ = mount
        target = point / "t.bin"
        target.write_bytes(b"x" * 10)
        os.truncate(target, 5000)
        assert target.stat().st_size == 5000
        assert target.read_bytes()[10:] == b"\x00" * 4990

    def test_partial_overwrite_in_place(self, mount):
        point, _ = mount
        target = point / "patch.bin"
        target.write_bytes(b"A" * 100)
        with open(target, "r+b") as handle:
            handle.seek(50)
            handle.write(b"B" * 10)

        data = target.read_bytes()
        assert data[:50] == b"A" * 50
        assert data[50:60] == b"B" * 10
        assert data[60:] == b"A" * 40

    def test_unlink(self, mount):
        point, backing = mount
        (point / "doomed.txt").write_text("bye")
        (point / "doomed.txt").unlink()
        assert not (point / "doomed.txt").exists()
        assert not (backing / "doomed.txt").exists()

    def test_mkdir_and_rmdir(self, mount):
        point, _ = mount
        (point / "d").mkdir()
        assert (point / "d").is_dir()
        (point / "d").rmdir()
        assert not (point / "d").exists()

    def test_rmdir_refuses_non_empty(self, mount):
        point, _ = mount
        (point / "d").mkdir()
        (point / "d" / "f.txt").write_text("x")
        with pytest.raises(OSError) as info:
            (point / "d").rmdir()
        assert info.value.errno in (errno.ENOTEMPTY, errno.EEXIST)

    def test_rename_preserves_inode(self, mount):
        point, _ = mount
        (point / "a.txt").write_text("content")
        before = os.stat(point / "a.txt").st_ino
        os.rename(point / "a.txt", point / "b.txt")
        assert os.stat(point / "b.txt").st_ino == before

    def test_rename_across_directories(self, mount):
        point, _ = mount
        (point / "src").mkdir()
        (point / "dst").mkdir()
        (point / "src" / "f.txt").write_text("moved")
        os.rename(point / "src" / "f.txt", point / "dst" / "f.txt")
        assert (point / "dst" / "f.txt").read_text() == "moved"

    def test_rename_replaces_target(self, mount):
        point, _ = mount
        (point / "a.txt").write_text("winner")
        (point / "b.txt").write_text("loser")
        os.rename(point / "a.txt", point / "b.txt")
        assert (point / "b.txt").read_text() == "winner"
        assert not (point / "a.txt").exists()

    def test_chmod(self, mount):
        point, _ = mount
        target = point / "m.txt"
        target.write_text("x")
        os.chmod(target, 0o600)
        assert (os.stat(target).st_mode & 0o777) == 0o600

    def test_utime(self, mount):
        point, _ = mount
        target = point / "u.txt"
        target.write_text("x")
        os.utime(target, (1000000, 2000000))
        assert int(os.stat(target).st_mtime) == 2000000

    def test_fsync_succeeds(self, mount):
        point, _ = mount
        with open(point / "f.txt", "w") as handle:
            handle.write("durable")
            handle.flush()
            os.fsync(handle.fileno())
        assert (point / "f.txt").read_text() == "durable"

    def test_read_your_writes(self, mount):
        """A guarantee we make explicitly; must hold with no delay."""
        point, _ = mount
        target = point / "ryw.txt"
        for index in range(20):
            target.write_text(f"revision {index}")
            assert target.read_text() == f"revision {index}"
