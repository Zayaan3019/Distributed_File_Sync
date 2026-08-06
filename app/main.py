"""
Main Entry Point for Distributed File Sync.

This module bootstraps the entire application, coordinating:
- Configuration loading
- Network manager initialization
- Sync engine setup
- File monitor startup
- Graceful shutdown handling
"""

import asyncio
import os
import signal
import sys
import argparse
from pathlib import Path

from app.core import SyncConfig, setup_logger, get_logger
from app.services import NetworkManager, FileMonitorManager, SyncEngine

logger = get_logger(__name__)

#: How often expired deletion tombstones are collected, in seconds.
TOMBSTONE_GC_INTERVAL = 3600.0

#: How often stalled chunk transfers are checked for, in seconds. Much shorter
#: than the tombstone sweep because a stalled transfer means a file is missing
#: right now, whereas an uncollected tombstone only wastes a table row.
TRANSFER_REAP_INTERVAL = 15.0


class FileSyncApp:
    """
    Main application class that coordinates all components.
    
    Attributes:
        config: Application configuration.
        network: Network manager.
        sync_engine: Synchronization engine.
        file_monitor: File system monitor.
        event_loop: AsyncIO event loop.
    """
    
    def __init__(self, config: SyncConfig):
        """
        Initialize the application.
        
        Args:
            config: Application configuration.
        """
        self.config = config
        self.event_loop = asyncio.get_event_loop()

        # Initialize components (order matters)
        self.network = NetworkManager(config, self._handle_message)
        self.sync_engine = SyncEngine(config, self.network)
        self.file_monitor = FileMonitorManager(
            config.sync_directory,
            self.sync_engine.on_file_changed,
            self.sync_engine.on_file_deleted,
            self.event_loop
        )

        #: FUSE session, when --fuse-mount was given. None keeps the daemon in
        #: its original watchdog mode, byte-for-byte, on Windows and Linux.
        self.fuse_mount = None

        self._shutdown_event = asyncio.Event()
        self._running = False
        self._gc_task = None
        self._reap_task = None

    async def _handle_message(self, message, peer_conn) -> None:
        """
        Route incoming messages to sync engine.
        
        Args:
            message: Received message.
            peer_conn: Peer connection.
        """
        await self.sync_engine.handle_message(message, peer_conn)
    
    async def start(self) -> None:
        """
        Start all application components.
        """
        try:
            logger.info("=" * 60)
            logger.info("Starting Distributed File Sync Engine")
            logger.info(f"Node ID: {self.config.node_id}")
            logger.info(f"Sync Directory: {self.config.sync_directory}")
            logger.info(f"Listen Address: {self.config.listen_host}:{self.config.listen_port}")
            logger.info(f"Peers: {len(self.config.peers)}")
            logger.info(f"Security: {'PSK auth + AES-256-GCM' if self.config.is_secure() else 'DISABLED (--allow-insecure)'}")
            logger.info(f"Mode: {'FUSE @ ' + self.config.fuse_mount if self.config.fuse_mount else 'daemon (watchdog)'}")
            logger.info("=" * 60)

            self._running = True

            # Initialize sync engine (restores state from the catalog)
            await self.sync_engine.initialize()

            # Start network manager
            await self.network.start()

            if self.config.fuse_mount:
                self._start_fuse()
            else:
                # In FUSE mode the mount is the source of truth for local
                # mutations, so the watchdog observer would double-report
                # every write the FUSE layer already told the engine about.
                self.file_monitor.start()

            self._gc_task = asyncio.create_task(self._tombstone_gc_loop())
            self._reap_task = asyncio.create_task(self._transfer_reap_loop())

            logger.info("All components started successfully")
            logger.info("Press Ctrl+C to stop")

            # Wait for shutdown signal
            await self._shutdown_event.wait()

        except Exception as e:
            logger.error(f"Error during startup: {e}", exc_info=True)
            raise

    def _start_fuse(self) -> None:
        """
        Mount the synced tree through FUSE.

        Imported here rather than at module scope: `pyfuse3` does not exist on
        Windows, and the daemon must keep starting there.

        Raises:
            RuntimeError: If FUSE is unavailable on this platform.
        """
        if sys.platform == "win32":
            raise RuntimeError(
                "FUSE mode is not available on Windows. Run the daemon without "
                "--fuse-mount, or use WSL2."
            )

        from app.fuse.mount import FuseMount

        self.fuse_mount = FuseMount(
            mount_point=self.config.fuse_mount,
            backing_root=self.config.sync_directory,
            catalog_path=self.config.catalog_path,
            engine=self.sync_engine,
            loop=self.event_loop,
            chunk_size=self.config.chunk_size,
            allow_other=self.config.fuse_allow_other,
        )
        self.fuse_mount.start()

    async def _tombstone_gc_loop(self) -> None:
        """
        Collect expired tombstones on a timer.

        Runs one sweep at startup and hourly thereafter. Without this the
        tombstone table grows for the lifetime of the node.
        """
        try:
            await self.sync_engine.gc_tombstones()
            while self._running:
                await asyncio.sleep(TOMBSTONE_GC_INTERVAL)
                await self.sync_engine.gc_tombstones()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Tombstone GC failed: {e}", exc_info=True)

    async def _transfer_reap_loop(self) -> None:
        """
        Restart chunk transfers whose sender stopped sending.

        Without this a transfer interrupted mid-stream never completes and the
        file simply never appears -- no error, no retry, no symptom other than
        an absence. See `SyncEngine.reap_stalled_transfers`.
        """
        try:
            while self._running:
                await asyncio.sleep(TRANSFER_REAP_INTERVAL)
                await self.sync_engine.reap_stalled_transfers()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Transfer reaper failed: {e}", exc_info=True)


    async def stop(self) -> None:
        """
        Stop all application components gracefully.
        """
        if not self._running:
            return
        
        logger.info("Shutting down...")
        self._running = False

        try:
            if self._gc_task is not None:
                self._gc_task.cancel()
            if self._reap_task is not None:
                self._reap_task.cancel()

            # Unmount first. While the mount is live, applications can still
            # be issuing writes that post work to the engine we are about to
            # tear down.
            if self.fuse_mount is not None:
                self.fuse_mount.stop()

            # Stop file monitor (synchronous)
            self.file_monitor.stop()

            # Stop network manager (async)
            await self.network.stop()

            # Close the catalog last: everything above may still be flushing.
            await self.sync_engine.close()

            logger.info("Shutdown complete")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)
    
    def _signal_handler(self, signum, frame) -> None:
        """
        Handle shutdown signals.
        
        Args:
            signum: Signal number.
            frame: Current stack frame.
        """
        logger.info(f"Received signal {signum}")
        self._shutdown_event.set()


def parse_args():
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description='Distributed File Synchronization Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start with config file
  python -m app.main --config config.json
  
  # Start with environment variables
  export SYNC_NODE_ID=node1
  export SYNC_DIRECTORY=/path/to/sync
  export SYNC_LISTEN_PORT=8001
  export SYNC_PEERS=localhost:8002,localhost:8003
  python -m app.main
  
  # Start with command-line arguments
  python -m app.main --node-id node1 --dir /path/to/sync --port 8001 \\
    --peer localhost:8002 --peer localhost:8003
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to configuration JSON file'
    )
    
    parser.add_argument(
        '--node-id', '-n',
        type=str,
        help='Unique node identifier'
    )
    
    parser.add_argument(
        '--dir', '-d',
        type=str,
        help='Directory to synchronize'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default='127.0.0.1',
        help='Host address to bind to (default: 127.0.0.1)'
    )
    
    parser.add_argument(
        '--port', '-p',
        type=int,
        help='Port to listen on'
    )
    
    parser.add_argument(
        '--peer',
        action='append',
        dest='peers',
        help='Peer address in format host:port (can be specified multiple times)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        help='Log file path (optional)'
    )

    security = parser.add_argument_group('security')
    security.add_argument(
        '--psk',
        type=str,
        help='Pre-shared key for mutual peer authentication and AES-256-GCM '
             'transit encryption. Must be identical on every peer. Prefer '
             'SYNC_PSK in the environment so the key does not appear in "ps".'
    )
    security.add_argument(
        '--allow-insecure',
        action='store_true',
        help='Run WITHOUT peer authentication or encryption. Any host that '
             'can reach the port will be able to read and write your files.'
    )

    fuse_group = parser.add_argument_group('FUSE mode (Linux/macOS only)')
    fuse_group.add_argument(
        '--fuse-mount',
        type=str,
        metavar='PATH',
        help='Additionally expose the synced tree as a filesystem at PATH. '
             'The directory given by --dir remains the backing store; it must '
             'not be the same directory as PATH.'
    )
    fuse_group.add_argument(
        '--fuse-allow-other',
        action='store_true',
        help='Allow users other than the mounting user to access the mount '
             '(requires user_allow_other in /etc/fuse.conf).'
    )
    fuse_group.add_argument(
        '--no-catalog',
        action='store_true',
        help='Disable the SQLite metadata catalog. Metadata is then lost on '
             'restart and every file is re-hashed at startup. Incompatible '
             'with --fuse-mount.'
    )

    return parser.parse_args()


def create_config_from_args(args) -> SyncConfig:
    """
    Create configuration from command-line arguments.
    
    Args:
        args: Parsed command-line arguments.
    
    Returns:
        SyncConfig instance.
    """
    def _apply_overrides(config: SyncConfig) -> SyncConfig:
        """
        Let explicit command-line flags win over file/env values.

        Args:
            config: Configuration built from a file or the environment.

        Returns:
            The same config, with CLI overrides applied.
        """
        if args.psk:
            config.psk = args.psk
        if args.allow_insecure:
            config.allow_insecure = True
        if args.fuse_mount:
            config.fuse_mount = args.fuse_mount
        if args.fuse_allow_other:
            config.fuse_allow_other = True
        if args.no_catalog:
            config.enable_catalog = False
        return config

    # Try loading from config file first
    if args.config:
        logger.info(f"Loading configuration from {args.config}")
        return _apply_overrides(SyncConfig.load(args.config))

    # Try environment variables
    try:
        return _apply_overrides(SyncConfig.from_env())
    except ValueError:
        pass

    # Build from command-line arguments
    if not args.node_id or not args.dir:
        print("Error: --node-id and --dir are required when not using config file or environment variables")
        sys.exit(1)

    config = SyncConfig(
        node_id=args.node_id,
        sync_directory=args.dir,
        listen_host=args.host,
        listen_port=args.port or 8000,
        # The environment is preferred over the flag: an argv value is visible
        # to every user on the box via `ps`.
        psk=args.psk or os.getenv('SYNC_PSK') or None,
        allow_insecure=args.allow_insecure,
        fuse_mount=args.fuse_mount,
        fuse_allow_other=args.fuse_allow_other,
        enable_catalog=not args.no_catalog,
    )

    # Add peers
    if args.peers:
        for peer_addr in args.peers:
            if ':' in peer_addr:
                host, port_str = peer_addr.rsplit(':', 1)
                try:
                    port = int(port_str)
                    config.add_peer(host, port)
                except ValueError:
                    print(f"Error: Invalid peer address: {peer_addr}")
                    sys.exit(1)
    
    return config


async def main_async():
    """
    Async main function.
    """
    # Parse arguments
    args = parse_args()
    
    # Setup logging
    import logging
    log_level = getattr(logging, args.log_level)
    # Configure the root logger only. Every module logs through
    # `logging.getLogger(__name__)`, and those records propagate up to root.
    #
    # This used to configure `app` *and* root, which gave records from
    # `app.services.network` two console handlers on the way up and printed
    # every line twice. Harmless-looking, but it doubles the size of any log a
    # reader is trying to follow, and it made a duplicated message look like a
    # duplicated event.
    setup_logger(
        name='',
        level=log_level,
        log_file=args.log_file,
        console=True
    )
    
    # Create configuration
    config = create_config_from_args(args)
    
    # Create application
    app = FileSyncApp(config)
    
    # Setup signal handlers.
    #
    # loop.add_signal_handler() is not implemented on Windows -- it raises
    # NotImplementedError on the ProactorEventLoop, which took the daemon down
    # at start-up on the platform Definition-of-Done #5 requires it to run on.
    # signal.signal() works on both, and the handler only sets an Event.
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(
                sig,
                lambda s=sig: app._signal_handler(s, None)
            )
        except (NotImplementedError, AttributeError, ValueError):
            # Windows, or a non-main thread. Fall back to the portable API.
            try:
                signal.signal(
                    sig,
                    lambda s, f: loop.call_soon_threadsafe(
                        app._signal_handler, s, f
                    )
                )
            except (ValueError, OSError) as exc:
                logger.warning(f"Could not install handler for {sig!r}: {exc}")

    try:
        # Start application
        await app.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        # Cleanup
        await app.stop()


def main():
    """
    Main entry point.
    """
    try:
        # Run async main
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
