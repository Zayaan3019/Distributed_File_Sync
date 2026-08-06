"""
Regression tests for Merkle diff correctness.

These cover the two defects documented in docs/FUSE_DESIGN.md sections 1.1 and
1.2, which the FUSE write path depends on:

  1.1  Interior nodes collapsed two children into one whenever their hashes
       matched. MerkleNode compares by hash, so two *distinct positions*
       holding *identical content* -- zero-filled regions, repeated padding,
       any sparse file -- were merged. The positional walk in
       _find_diff_recursive then misaligned and dropped the changed chunk,
       returning an EMPTY diff for a file that had genuinely changed.

  1.2  Chunks present on the remote but not locally were never recorded, so a
       file that grew never replicated its tail.

The brute-force test is the important one: it checks the diff against a direct
chunk-by-chunk comparison over randomly generated file pairs.
"""

import os
import random

import pytest

from app.algorithms.merkle import MerkleTree, CHUNK_SIZE


def _chunks_of(data: bytes, chunk_size: int = CHUNK_SIZE):
    """Split bytes into chunk-sized pieces, mirroring MerkleTree.from_file."""
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


def _expected_diff(local: bytes, remote: bytes, chunk_size: int = CHUNK_SIZE):
    """
    Ground truth: which remote chunk indices must be fetched?

    Every remote chunk that the local file either lacks or holds different
    bytes for. Local-only chunks (the file shrank) need no transfer; the
    truncation comes from the remote file size.
    """
    local_chunks = _chunks_of(local, chunk_size)
    remote_chunks = _chunks_of(remote, chunk_size)

    return {
        idx
        for idx, remote_chunk in enumerate(remote_chunks)
        if idx >= len(local_chunks) or local_chunks[idx] != remote_chunk
    }


async def _diff_of(tmp_path, local: bytes, remote: bytes):
    """Build both trees from real files and return the computed diff."""
    local_path = tmp_path / "local.bin"
    remote_path = tmp_path / "remote.bin"
    local_path.write_bytes(local)
    remote_path.write_bytes(remote)

    local_tree = await MerkleTree.from_file(str(local_path))
    remote_tree = await MerkleTree.from_file(str(remote_path))

    return MerkleTree.get_diff(local_tree, remote_tree).changed_chunks


Z = b"\x00" * CHUNK_SIZE          # zero-filled: the sparse-file case
Q = b"Q" * CHUNK_SIZE
R = b"R" * CHUNK_SIZE
S = b"S" * CHUNK_SIZE


class TestMerkleDiffDuplicateChunks:
    """Defect 1.1 -- identical adjacent chunks must not hide a real change."""

    @pytest.mark.asyncio
    async def test_local_siblings_identical(self, tmp_path):
        """A remote edit to one of two identical local chunks is not lost."""
        assert await _diff_of(tmp_path, Z + Z, Z + Q) == {1}

    @pytest.mark.asyncio
    async def test_remote_siblings_identical(self, tmp_path):
        """The mirror image: identical chunks on the remote side."""
        assert await _diff_of(tmp_path, Z + Q, Z + Z) == {1}

    @pytest.mark.asyncio
    async def test_sparse_tail_edit(self, tmp_path):
        """An edit at the end of a fully zero-filled file."""
        assert await _diff_of(tmp_path, Z * 4, Z * 3 + Q) == {3}

    @pytest.mark.asyncio
    async def test_repeating_pattern(self, tmp_path):
        """Repeated blocks collapse at several tree levels at once."""
        assert await _diff_of(tmp_path, Q + R + Q + R, Q + R + Q + S) == {3}

    @pytest.mark.asyncio
    async def test_all_chunks_identical_large(self, tmp_path):
        """16 identical chunks, one edited: only that one is fetched."""
        local = Z * 16
        remote = Z * 9 + Q + Z * 6
        assert await _diff_of(tmp_path, local, remote) == {9}

    @pytest.mark.asyncio
    async def test_tree_keeps_both_children(self, tmp_path):
        """Structural check: no interior node may have a single child."""
        path = tmp_path / "dup.bin"
        path.write_bytes(Z * 4)
        tree = await MerkleTree.from_file(str(path))

        stack = [tree.root]
        while stack:
            node = stack.pop()
            if not node.is_leaf:
                assert len(node.children) == 2, "interior node collapsed"
                stack.extend(node.children)


class TestMerkleDiffFileGrowth:
    """Defect 1.2 -- chunks that exist only on the remote must be fetched."""

    @pytest.mark.asyncio
    async def test_append_two_chunks(self, tmp_path):
        assert await _diff_of(tmp_path, Q, Q + R + S) == {1, 2}

    @pytest.mark.asyncio
    async def test_append_is_minimal(self, tmp_path):
        """Appending must not re-fetch the unchanged prefix."""
        local = Q * 8
        remote = Q * 8 + R
        assert await _diff_of(tmp_path, local, remote) == {8}

    @pytest.mark.asyncio
    async def test_growth_from_empty(self, tmp_path):
        assert await _diff_of(tmp_path, b"", Q + R) == {0, 1}

    @pytest.mark.asyncio
    async def test_shrink_fetches_nothing(self, tmp_path):
        """A shrunk remote needs no chunks; truncation comes from file size."""
        assert await _diff_of(tmp_path, Q + R + S, Q) == set()

    @pytest.mark.asyncio
    async def test_shrink_with_edit(self, tmp_path):
        """Shrunk *and* edited: only the surviving changed chunk transfers."""
        assert await _diff_of(tmp_path, Q + R + S, Z) == {0}


class TestMerkleDiffBruteForce:
    """The diff must equal a direct chunk-by-chunk comparison. Always."""

    @pytest.mark.asyncio
    async def test_random_file_pairs(self, tmp_path):
        """
        200 randomised local/remote pairs drawn from a deliberately small
        alphabet, so identical chunks occur often and the collapse path is
        exercised hard. Seeded, so a failure is reproducible.
        """
        rng = random.Random(20260731)
        alphabet = [Z, Q, R, S]

        for case in range(200):
            n_local = rng.randint(0, 9)
            n_remote = rng.randint(0, 9)
            local = b"".join(rng.choice(alphabet) for _ in range(n_local))
            remote = b"".join(rng.choice(alphabet) for _ in range(n_remote))

            got = await _diff_of(tmp_path, local, remote)
            want = _expected_diff(local, remote)

            assert got == want, (
                f"case {case}: local={n_local} chunks remote={n_remote} chunks\n"
                f"  expected {sorted(want)}\n"
                f"  got      {sorted(got)}"
            )

    @pytest.mark.asyncio
    async def test_random_partial_chunks(self, tmp_path):
        """Same, but with sizes that do not land on a chunk boundary."""
        rng = random.Random(20260801)

        for case in range(60):
            local = bytes(rng.getrandbits(8) for _ in range(rng.randint(0, 3000)))
            remote = bytearray(local)

            # Mutate a random slice, then grow or shrink the tail.
            if remote and rng.random() < 0.7:
                pos = rng.randrange(len(remote))
                remote[pos] ^= 0xFF
            delta = rng.randint(-500, 500)
            if delta > 0:
                remote.extend(bytes(rng.getrandbits(8) for _ in range(delta)))
            elif delta < 0:
                del remote[max(0, len(remote) + delta):]

            remote = bytes(remote)
            got = await _diff_of(tmp_path, local, remote)
            want = _expected_diff(local, remote)

            assert got == want, (
                f"case {case}: len(local)={len(local)} len(remote)={len(remote)}\n"
                f"  expected {sorted(want)}\n"
                f"  got      {sorted(got)}"
            )

    @pytest.mark.asyncio
    async def test_identical_files_produce_empty_diff(self, tmp_path):
        rng = random.Random(7)
        data = bytes(rng.getrandbits(8) for _ in range(5000))
        assert await _diff_of(tmp_path, data, data) == set()

    @pytest.mark.asyncio
    async def test_single_byte_edit_in_large_file_fetches_one_chunk(self, tmp_path):
        """
        The headline claim of the whole design: a one-byte edit costs one
        chunk on the wire, not the whole file.
        """
        rng = random.Random(99)
        local = bytes(rng.getrandbits(8) for _ in range(256 * CHUNK_SIZE))  # 1 MB
        edit_at = 200 * CHUNK_SIZE + 17
        remote = local[:edit_at] + bytes([local[edit_at] ^ 0xFF]) + local[edit_at + 1:]

        diff = await _diff_of(tmp_path, local, remote)
        assert diff == {200}
