"""
FUSE mode for the distributed file sync engine.

FUSE is Linux/macOS only. Everything in this package is imported lazily by
`app.main`, so the existing watchdog daemon mode keeps working unchanged on
Windows, where `pyfuse3` cannot even be installed. Importing this package on
Windows raises ImportError from pyfuse3 itself; that is expected and is why
nothing here is imported at application start-up.

Layout, per docs/FUSE_DESIGN.md section 7::

    <sync_directory>/       backing store   <- SyncEngine operates here
    <sync_directory>/.dfs/  catalog + scratch
    <mount point>           FUSE mount      <- applications see this

The backing store and the mount point are deliberately *different*
directories. If the asyncio engine ever performed I/O through the mount, a
FUSE handler waiting on the engine while the engine waits on FUSE would
deadlock unrecoverably.
"""

from .inodes import InodeTable
from .attrcache import AttrCache

__all__ = ["InodeTable", "AttrCache", "FileSyncFs", "FuseMount", "mount_blocking"]


def __getattr__(name):
    """
    Import the pyfuse3-dependent pieces on first use.

    Keeping these out of module import means `import app.fuse` succeeds on
    Windows for anything that only needs the inode table or the attr cache --
    which is what lets those two be unit-tested on both platforms.

    Args:
        name: Attribute being looked up.

    Returns:
        The requested class.

    Raises:
        AttributeError: If the name is not exported.
    """
    if name in ("FileSyncFs",):
        from .operations import FileSyncFs
        return FileSyncFs
    if name in ("FuseMount", "mount_blocking"):
        from . import mount
        return getattr(mount, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
