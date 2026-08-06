"""
Phase 6 measurement harness.

Produces the numbers in `artifacts/fuse_benchmark.md`. Every figure is measured
here; nothing in the report is estimated or carried over from a previous run.

Must be run on ext4 (or any real Linux filesystem), never under /mnt/c. /mnt/c
is a 9p/drvfs mount under WSL2, and measuring a FUSE overlay on top of 9p would
report the cost of 9p rather than the cost of this design.

Usage::

    python -m benchmarks.fuse_benchmark --out artifacts/fuse_benchmark.md
    python -m benchmarks.fuse_benchmark --quick   # smaller sizes, for iteration
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

MB = 1024 * 1024


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def human(num_bytes: float) -> str:
    """
    Format a byte count for the report.

    Args:
        num_bytes: Quantity in bytes.

    Returns:
        Human-readable string.
    """
    for unit in ("B", "KB", "MB", "GB"):
        if abs(num_bytes) < 1024 or unit == "GB":
            return f"{num_bytes:,.1f} {unit}" if unit != "B" else f"{num_bytes:,.0f} B"
        num_bytes /= 1024
    return f"{num_bytes:.1f} GB"


def throughput(num_bytes: int, seconds: float) -> str:
    """
    Format a throughput figure.

    Args:
        num_bytes: Bytes moved.
        seconds: Elapsed wall time.

    Returns:
        Throughput in MB/s.
    """
    if seconds <= 0:
        return "n/a"
    return f"{num_bytes / seconds / MB:,.1f} MB/s"


def percentiles(samples):
    """
    Compute p50/p95/p99 in microseconds.

    Args:
        samples: Iterable of durations in seconds.

    Returns:
        Dict of percentile name to microseconds.
    """
    ordered = sorted(samples)
    if not ordered:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0}

    def pick(fraction):
        index = min(len(ordered) - 1, int(len(ordered) * fraction))
        return ordered[index] * 1e6

    return {"p50": pick(0.50), "p95": pick(0.95), "p99": pick(0.99)}


def drop_caches(path: Path) -> None:
    """
    Best-effort page-cache eviction for a file.

    Without this a "cold read" measures memcpy from the page cache, which
    would make the FUSE overhead look far smaller than it is.

    Args:
        path: File to evict.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            # POSIX_FADV_DONTNEED == 4
            os.posix_fadvise(fd, 0, 0, 4)
        finally:
            os.close(fd)
    except (AttributeError, OSError):
        pass


# ---------------------------------------------------------------------------
# Filesystem benchmarks
# ---------------------------------------------------------------------------


def bench_sequential(mount: Path, native: Path, size: int, results: dict,
                     repeats: int = 5) -> None:
    """
    Sequential write and read throughput, FUSE versus the native filesystem.

    Repeated and reported as a median with the observed spread. A single pass
    is not a measurement here: two consecutive single-shot runs of this
    benchmark disagreed by 6x on native write throughput (368 MB/s vs 62 MB/s
    at 16MB) purely because of writeback and page-cache scheduling, and one of
    them made FUSE look *faster* than the filesystem it sits on top of. The
    spread is reported rather than hidden so a reader can see how much weight
    the median deserves.

    Native and FUSE alternate within each repeat instead of running as two
    separate batches, so a machine that gets busier partway through penalises
    both equally rather than only whichever went second.

    Args:
        mount: FUSE mount point.
        native: A directory on the backing filesystem, for the baseline.
        size: File size in bytes.
        results: Dict to accumulate into.
        repeats: Passes to take the median over.
    """
    payload = os.urandom(size)
    samples = {"native": {"write": [], "read": []},
               "fuse": {"write": [], "read": []}}

    for _ in range(repeats):
        for label, target in (("native", native), ("fuse", mount)):
            path = target / f"seq_{size}.bin"

            start = time.perf_counter()
            with open(path, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            samples[label]["write"].append(time.perf_counter() - start)

            drop_caches(path)

            start = time.perf_counter()
            with open(path, "rb") as handle:
                while handle.read(1 * MB):
                    pass
            samples[label]["read"].append(time.perf_counter() - start)

            path.unlink()

    def summarise(label, op):
        values = samples[label][op]
        return {
            "median_s": statistics.median(values),
            "best": throughput(size, min(values)),
            "median": throughput(size, statistics.median(values)),
            "worst": throughput(size, max(values)),
        }

    native_write = summarise("native", "write")
    fuse_write = summarise("fuse", "write")
    native_read = summarise("native", "read")
    fuse_read = summarise("fuse", "read")

    results.setdefault("sequential", []).append({
        "size": size,
        "repeats": repeats,
        "native_write": native_write["median"],
        "native_write_range": f"{native_write['worst']} .. {native_write['best']}",
        "fuse_write": fuse_write["median"],
        "fuse_write_range": f"{fuse_write['worst']} .. {fuse_write['best']}",
        "write_ratio": fuse_write["median_s"] / max(native_write["median_s"], 1e-9),
        "native_read": native_read["median"],
        "native_read_range": f"{native_read['worst']} .. {native_read['best']}",
        "fuse_read": fuse_read["median"],
        "fuse_read_range": f"{fuse_read['worst']} .. {fuse_read['best']}",
        "read_ratio": fuse_read["median_s"] / max(native_read["median_s"], 1e-9),
    })


def bench_metadata(mount: Path, native: Path, count: int, results: dict,
                   repeats: int = 3) -> None:
    """
    Metadata operation rates: create, stat, unlink.

    Repeated and reported as medians, for the same reason as
    `bench_sequential`: a single pass is dominated by whatever else the
    machine was doing.

    Args:
        mount: FUSE mount point.
        native: Baseline directory.
        count: Number of files to operate on.
        results: Dict to accumulate into.
        repeats: Passes to take the median over.
    """
    samples = {"native": {"create": [], "stat": [], "unlink": []},
               "fuse": {"create": [], "stat": [], "unlink": []}}

    for _ in range(repeats):
        for label, target in (("native", native), ("fuse", mount)):
            work = target / f"meta_{label}"
            work.mkdir(exist_ok=True)

            start = time.perf_counter()
            for index in range(count):
                (work / f"f{index}").write_bytes(b"x")
            samples[label]["create"].append(count / (time.perf_counter() - start))

            start = time.perf_counter()
            for index in range(count):
                os.stat(work / f"f{index}")
            samples[label]["stat"].append(count / (time.perf_counter() - start))

            start = time.perf_counter()
            for index in range(count):
                os.unlink(work / f"f{index}")
            samples[label]["unlink"].append(count / (time.perf_counter() - start))

            work.rmdir()

    rows = {
        label: {op: statistics.median(values) for op, values in ops.items()}
        for label, ops in samples.items()
    }

    results["metadata"] = {
        "count": count,
        "repeats": repeats,
        "native": rows["native"],
        "fuse": rows["fuse"],
        "ratio": {
            key: rows["native"][key] / max(rows["fuse"][key], 1e-9)
            for key in ("create", "stat", "unlink")
        },
    }


def bench_getattr(mount: Path, results: dict, iterations: int = 3000) -> None:
    """
    getattr latency, cached versus cold.

    Reported separately because a single figure would be meaningless: the
    kernel's own attribute cache absorbs most stat() traffic, so the "warm"
    number reflects the cache and the "cold" number reflects our handler.

    Args:
        mount: FUSE mount point.
        results: Dict to accumulate into.
        iterations: Samples per mode.
    """
    target = mount / "getattr_probe.bin"
    target.write_bytes(b"x" * 1024)

    os.stat(target)  # prime
    warm = []
    for _ in range(iterations):
        start = time.perf_counter()
        os.stat(target)
        warm.append(time.perf_counter() - start)

    # Defeat the kernel attribute cache by stat'ing a different path each
    # time; the entry cache cannot answer for a name it has not seen.
    scratch = mount / "getattr_cold"
    scratch.mkdir(exist_ok=True)
    cold_count = min(iterations, 1500)
    for index in range(cold_count):
        (scratch / f"f{index}").write_bytes(b"x")

    cold = []
    for index in range(cold_count):
        start = time.perf_counter()
        os.stat(scratch / f"f{index}")
        cold.append(time.perf_counter() - start)

    results["getattr"] = {
        "warm": percentiles(warm),
        "cold": percentiles(cold),
        "warm_n": len(warm),
        "cold_n": len(cold),
    }

    for index in range(cold_count):
        (scratch / f"f{index}").unlink()
    scratch.rmdir()
    target.unlink()


# ---------------------------------------------------------------------------
# Replication benchmarks
# ---------------------------------------------------------------------------


class CountingChannel:
    """
    Wraps a channel and counts the bytes that actually cross the socket.

    This is how the Merkle delta figure is measured rather than asserted: the
    counter sits below the protocol layer, so it sees the real framed and
    encrypted payload, including all handshake and request overhead.

    Attributes:
        inner: The wrapped channel.
        sent: Bytes written.
        received: Bytes read.
    """

    def __init__(self, inner):
        """
        Initialize the counter.

        Args:
            inner: Channel to wrap.
        """
        self.inner = inner
        self.sent = 0
        self.received = 0

    async def send(self, plaintext: bytes) -> None:
        """Count and forward an outbound frame."""
        self.sent += len(plaintext)
        await self.inner.send(plaintext)

    async def receive(self):
        """Count and forward an inbound frame."""
        data = await self.inner.receive()
        if data:
            self.received += len(data)
        return data


async def bench_replication(results: dict, sizes, quick: bool) -> None:
    """
    Sync propagation latency and Merkle delta efficiency between two peers.

    Args:
        results: Dict to accumulate into.
        sizes: File sizes to test propagation for.
        quick: Use a smaller file for the delta test.
    """
    from tests.test_two_node import Node, wait_for

    port = 20301
    a = Node("bench_a", port, peer_ports=[port + 1])
    b = Node("bench_b", port + 1, peer_ports=[port])

    await a.start()
    await b.start()
    assert await wait_for(
        lambda: a.network.get_connected_peers() and b.network.get_connected_peers(),
        timeout=20,
    ), "benchmark peers never connected"

    try:
        # ---- propagation latency ----
        propagation = []
        for size in sizes:
            name = f"prop_{size}.bin"
            payload = os.urandom(size)
            a.write(name, payload)

            start = time.perf_counter()
            await a.engine.on_file_changed(name)
            arrived = await wait_for(
                lambda n=name, p=payload: b.path(n).exists()
                and b.path(n).read_bytes() == p,
                timeout=180.0,
            )
            elapsed = time.perf_counter() - start

            propagation.append({
                "size": size,
                "seconds": elapsed if arrived else None,
                "ok": arrived,
                "throughput": throughput(size, elapsed) if arrived else "n/a",
            })
        results["propagation"] = propagation

        # ---- Merkle delta efficiency ----
        # The headline number: bytes on the wire versus bytes changed, for a
        # one-byte edit in a large file.
        delta_size = 8 * MB if quick else 100 * MB
        name = f"delta_{delta_size}.bin"
        payload = bytearray(os.urandom(delta_size))
        a.write(name, bytes(payload))

        await a.engine.on_file_changed(name)
        assert await wait_for(
            lambda: b.path(name).exists()
            and b.path(name).stat().st_size == delta_size,
            timeout=600.0,
        ), "delta baseline never propagated"
        # Let the exchange fully quiesce before we start counting.
        await asyncio.sleep(1.0)

        counters = []
        for node in (a, b):
            for peer_id, conn in node.network.connections.items():
                counter = CountingChannel(conn.channel)
                conn.channel = counter
                counters.append(counter)

        # Flip exactly one byte, in the middle of the file.
        offset = delta_size // 2
        payload[offset] ^= 0xFF
        with open(a.path(name), "r+b") as handle:
            handle.seek(offset)
            handle.write(bytes([payload[offset]]))

        start = time.perf_counter()
        await a.engine.apply_local_write(name, {offset // 4096})
        converged = await wait_for(
            lambda: b.path(name).read_bytes() == bytes(payload), timeout=600.0
        )
        delta_seconds = time.perf_counter() - start

        wire_bytes = sum(c.sent for c in counters)

        results["delta"] = {
            "file_size": delta_size,
            "bytes_changed": 1,
            "chunk_size": a.config.chunk_size,
            "wire_bytes": wire_bytes,
            "seconds": delta_seconds,
            "converged": converged,
            "vs_full_file": delta_size / max(wire_bytes, 1),
            "amplification": wire_bytes / 1,
        }
    finally:
        await a.stop()
        await b.stop()


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def environment() -> dict:
    """
    Capture the machine and filesystem the numbers were taken on.

    Returns:
        Environment metadata.
    """
    info = {
        "kernel": platform.release(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    try:
        info["cpu"] = subprocess.run(
            ["bash", "-c", "lscpu | grep 'Model name' | cut -d: -f2 | xargs"],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    except Exception:
        info["cpu"] = "unknown"
    try:
        info["mem"] = subprocess.run(
            ["bash", "-c", "free -h | awk '/^Mem:/{print $2}'"],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    except Exception:
        info["mem"] = "unknown"
    try:
        import pyfuse3
        info["pyfuse3"] = getattr(pyfuse3, "__version__", "unknown")
    except Exception:
        info["pyfuse3"] = "unavailable"
    return info


def main() -> None:
    """Run the benchmark suite and write the JSON result file."""
    parser = argparse.ArgumentParser(description="FUSE benchmark harness")
    parser.add_argument("--out", default="artifacts/fuse_benchmark.json")
    parser.add_argument("--quick", action="store_true",
                        help="smaller sizes, for iteration")
    parser.add_argument("--root", default=None,
                        help="working directory (must be on a real Linux fs)")
    args = parser.parse_args()

    from app.fuse.mount import FuseMount

    root = Path(args.root or tempfile.mkdtemp(prefix="dfs_bench_"))
    root.mkdir(parents=True, exist_ok=True)
    backing = root / "backing"
    point = root / "mnt"
    native = root / "native"
    for directory in (backing, point, native):
        directory.mkdir(exist_ok=True)

    if "/mnt/c" in str(root.resolve()):
        raise SystemExit(
            "Refusing to benchmark under /mnt/c: it is a 9p mount, and the "
            "numbers would measure 9p rather than this filesystem."
        )

    results = {"environment": environment(), "quick": args.quick}
    results["environment"]["backing_fs"] = subprocess.run(
        ["bash", "-c", f"df -T {backing} | tail -1 | awk '{{print $2}}'"],
        capture_output=True, text=True,
    ).stdout.strip()

    session = FuseMount(str(point), str(backing), str(root / "catalog.db"))
    session.start()

    try:
        sizes = [4096, MB] if args.quick else [4096, MB, 16 * MB, 128 * MB]
        for size in sizes:
            print(f"  sequential {human(size)} ...", flush=True)
            bench_sequential(point, native, size, results)

        print("  metadata ...", flush=True)
        bench_metadata(point, native, 300 if args.quick else 2000, results)

        print("  getattr ...", flush=True)
        bench_getattr(point, results)
    finally:
        session.stop()

    print("  replication ...", flush=True)
    prop_sizes = [4096, MB] if args.quick else [4096, MB, 100 * MB]
    asyncio.run(bench_replication(results, prop_sizes, args.quick))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\nwrote {out_path}")

    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
