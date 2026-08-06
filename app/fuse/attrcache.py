"""
Inode attribute cache.

`getattr` is called constantly -- an `ls -l` of N entries issues N of them,
`git status` issues one per tracked file, and the kernel re-validates on almost
every path resolution. Serving each one with a fresh `os.lstat` on the backing
store is what makes naive FUSE filesystems feel broken.

Two layers absorb that traffic:

1. **The kernel's own attribute cache.** Handing pyfuse3 a non-zero
   `attr_timeout` means the kernel answers most `stat()` calls without
   entering our process at all. This is by far the larger effect, and it is
   why `artifacts/fuse_benchmark.md` reports cached and cold numbers
   separately -- a single figure would be meaningless.
2. **This cache**, for the calls that do reach us: revalidations after the
   kernel timeout, and lookups the kernel cannot cache.

The cost of caching is staleness, and staleness here is *deliberate and
bounded*: docs/FUSE_DESIGN.md section 8 already states that remote-originated
changes may be up to one second stale in the mount. Local changes are never
stale, because every local mutation invalidates explicitly.
"""

from __future__ import annotations

import threading
import time
from typing import Dict, Optional, Tuple


class AttrCache:
    """
    A small TTL cache of inode attributes with explicit invalidation.

    Correctness rests on one rule: **every code path that mutates a file must
    invalidate it here.** A TTL alone is not sufficient for local writes --
    read-your-writes is a guarantee we make (design section 8), and a stale
    size for even 100ms would break `cat` on a file just written.

    Attributes:
        ttl: Seconds an entry stays valid without explicit invalidation.

    Example:
        >>> cache = AttrCache(ttl=1.0)
        >>> cache.put(42, some_stat_result)
        >>> cache.get(42) is not None
        True
        >>> cache.invalidate(42)
        >>> cache.get(42) is None
        True
    """

    def __init__(self, ttl: float = 1.0, max_entries: int = 100_000):
        """
        Initialize the cache.

        Args:
            ttl: Entry lifetime in seconds.
            max_entries: Hard cap on cached entries. A mount walked by
                something like `find /` would otherwise grow the cache without
                bound and turn a metadata scan into a memory leak.
        """
        self.ttl = ttl
        self.max_entries = max_entries
        self._entries: Dict[int, Tuple[float, object]] = {}
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    def get(self, ino: int) -> Optional[object]:
        """
        Fetch a cached attribute record.

        Args:
            ino: Inode number.

        Returns:
            The cached value, or None if absent or expired.
        """
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(ino)
            if entry is None:
                self.misses += 1
                return None

            stamp, value = entry
            if now - stamp > self.ttl:
                del self._entries[ino]
                self.misses += 1
                return None

            self.hits += 1
            return value

    def put(self, ino: int, value: object) -> None:
        """
        Store an attribute record.

        Args:
            ino: Inode number.
            value: Value to cache.
        """
        with self._lock:
            if len(self._entries) >= self.max_entries:
                # Cheap eviction: drop everything expired, and if that frees
                # nothing, drop the whole cache. A precise LRU would need a
                # linked structure on the hottest path in the filesystem; the
                # measured hit rate does not justify it.
                now = time.monotonic()
                self._entries = {
                    key: item for key, item in self._entries.items()
                    if now - item[0] <= self.ttl
                }
                if len(self._entries) >= self.max_entries:
                    self._entries.clear()

            self._entries[ino] = (time.monotonic(), value)

    def invalidate(self, ino: int) -> None:
        """
        Drop one entry.

        Args:
            ino: Inode number to invalidate.
        """
        with self._lock:
            self._entries.pop(ino, None)

    def clear(self) -> None:
        """Drop every entry. Used when the engine applies a bulk remote update."""
        with self._lock:
            self._entries.clear()

    def stats(self) -> Dict[str, float]:
        """
        Return hit/miss counters, for the Phase 6 benchmark.

        Returns:
            Mapping with hits, misses, entry count and hit rate.
        """
        with self._lock:
            total = self.hits + self.misses
            return {
                "hits": self.hits,
                "misses": self.misses,
                "entries": len(self._entries),
                "hit_rate": (self.hits / total) if total else 0.0,
            }
