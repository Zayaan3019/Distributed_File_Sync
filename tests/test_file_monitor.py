"""
Tests for the watchdog -> asyncio bridge.

This module had no tests at all, which is why a lost-update bug lived in it:
`_schedule_callback` built a coroutine, then discarded it if the event was
debounced. Two consequences, neither of which any other test could see --
every debounced event leaked an un-awaited coroutine, and, worse, a file
modified twice inside the 0.5s window was hashed once at its *first* content
and the second edit never propagated.

The bug surfaced as a `RuntimeWarning` buried in Docker container logs. These
tests assert the behaviour directly so it cannot come back quietly.

The event handler is deliberately exercised on its own rather than through a
real `Observer`: watchdog's own delivery is not what is being tested, and
driving the handler directly makes the coalescing timing deterministic.
"""

import asyncio
import warnings
from pathlib import Path

import pytest
from watchdog.events import (
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
)

from app.services.file_monitor import AsyncFileMonitor


class Recorder:
    """
    Collects the calls the monitor makes, with the content seen at call time.

    Reading the file inside the callback is the point: a debounce that drops
    the later event still *calls back*, just with stale content, so a test
    that only counted calls would pass against the bug.

    Attributes:
        changed: (path, content-at-call-time) for each change callback.
        deleted: Paths passed to the delete callback.
    """

    def __init__(self, root: Path):
        """
        Initialize the recorder.

        Args:
            root: Sync directory the paths are relative to.
        """
        self.root = root
        self.changed = []
        self.deleted = []

    async def on_changed(self, relative_path: str) -> None:
        """Record a change and the content visible when it was processed."""
        target = self.root / relative_path
        content = target.read_text() if target.exists() else None
        self.changed.append((relative_path, content))

    async def on_deleted(self, relative_path: str) -> None:
        """Record a deletion."""
        self.deleted.append(relative_path)


@pytest.fixture
async def monitor(tmp_path):
    """
    Build a monitor bound to the running loop and a temp sync directory.

    Async on purpose: a synchronous fixture would resolve
    `asyncio.get_event_loop()` to a loop that is not the one the test body
    runs on, and every scheduled callback would be posted somewhere nothing
    ever drains -- the tests would fail identically whether or not the code
    under test worked.

    Returns:
        Tuple of (AsyncFileMonitor, Recorder, root Path).
    """
    recorder = Recorder(tmp_path)
    handler = AsyncFileMonitor(
        str(tmp_path), recorder.on_changed, recorder.on_deleted,
        asyncio.get_running_loop(),
    )
    return handler, recorder, tmp_path


class TestEventDelivery:
    """Events reach the engine at all."""

    @pytest.mark.asyncio
    async def test_created_file_is_reported(self, monitor):
        handler, recorder, root = monitor
        target = root / "new.txt"
        target.write_text("fresh")

        handler.on_created(FileCreatedEvent(str(target)))
        await asyncio.sleep(0.1)

        assert recorder.changed == [("new.txt", "fresh")]

    @pytest.mark.asyncio
    async def test_deleted_file_is_reported(self, monitor):
        handler, recorder, root = monitor
        target = root / "gone.txt"
        target.write_text("x")
        target.unlink()

        handler.on_deleted(FileDeletedEvent(str(target)))
        await asyncio.sleep(0.1)

        assert recorder.deleted == ["gone.txt"]

    @pytest.mark.asyncio
    async def test_move_reports_both_sides(self, monitor):
        handler, recorder, root = monitor
        source = root / "old.txt"
        dest = root / "new.txt"
        dest.write_text("moved")

        handler.on_moved(FileMovedEvent(str(source), str(dest)))
        await asyncio.sleep(0.1)

        assert recorder.deleted == ["old.txt"]
        assert recorder.changed == [("new.txt", "moved")]


class TestDebounceCoalescing:
    """
    The regression tests. A burst must cost fewer hashes than it has events,
    and must still end up processing the file's final content.
    """

    @pytest.mark.asyncio
    async def test_burst_is_coalesced(self, monitor):
        """Several events in one window must not each trigger a full hash."""
        handler, recorder, root = monitor
        target = root / "busy.txt"
        target.write_text("v1")

        for _ in range(5):
            handler.on_modified(FileModifiedEvent(str(target)))
        await asyncio.sleep(0.1)

        assert len(recorder.changed) == 1, (
            f"burst was not coalesced: {recorder.changed}"
        )

    @pytest.mark.asyncio
    async def test_edit_during_the_window_is_not_lost(self, monitor):
        """
        The bug, stated directly.

        Write "v1", fire an event, then write "v2" and fire again inside the
        debounce window. The old code dropped the second event and the engine
        never saw "v2" -- the edit was lost until something unrelated touched
        the file again. The final content must be processed.
        """
        handler, recorder, root = monitor
        target = root / "raced.txt"

        target.write_text("v1")
        handler.on_modified(FileModifiedEvent(str(target)))
        await asyncio.sleep(0.05)

        target.write_text("v2")
        handler.on_modified(FileModifiedEvent(str(target)))

        # Long enough for the window to close and the trailing run to happen.
        await asyncio.sleep(1.0)

        seen = [content for _, content in recorder.changed]
        assert "v2" in seen, (
            f"edit made during the debounce window was lost; saw {seen}"
        )

    @pytest.mark.asyncio
    async def test_debounced_events_do_not_leak_coroutines(self, monitor):
        """
        No `coroutine ... was never awaited` warnings.

        A leaked coroutine is not just noise: it means a callback the code
        believed it had scheduled never ran.
        """
        handler, recorder, root = monitor
        target = root / "leaky.txt"
        target.write_text("content")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            for _ in range(10):
                handler.on_modified(FileModifiedEvent(str(target)))
            await asyncio.sleep(1.0)

        leaked = [
            w for w in caught
            if issubclass(w.category, RuntimeWarning)
            and "never awaited" in str(w.message)
        ]
        assert not leaked, f"leaked coroutines: {[str(w.message) for w in leaked]}"

    @pytest.mark.asyncio
    async def test_distinct_paths_are_not_coalesced_together(self, monitor):
        """Coalescing is per path; two files must not shadow each other."""
        handler, recorder, root = monitor
        first = root / "a.txt"
        second = root / "b.txt"
        first.write_text("A")
        second.write_text("B")

        handler.on_modified(FileModifiedEvent(str(first)))
        handler.on_modified(FileModifiedEvent(str(second)))
        await asyncio.sleep(0.1)

        assert sorted(recorder.changed) == [("a.txt", "A"), ("b.txt", "B")]


class TestIgnoreRules:
    """Internal and editor scratch files must never enter the sync stream."""

    @pytest.mark.asyncio
    async def test_temp_files_are_ignored(self, monitor):
        handler, recorder, root = monitor
        target = root / "document.swp"
        target.write_text("vim scratch")

        handler.on_modified(FileModifiedEvent(str(target)))
        await asyncio.sleep(0.1)

        assert recorder.changed == []

    @pytest.mark.asyncio
    async def test_paths_outside_the_root_are_ignored(self, monitor, tmp_path):
        handler, recorder, root = monitor
        outside = tmp_path.parent / "outside_the_root.txt"
        outside.write_text("not ours")

        handler.on_modified(FileModifiedEvent(str(outside)))
        await asyncio.sleep(0.1)

        assert recorder.changed == []
        outside.unlink()
