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
import signal
import sys
import argparse
from pathlib import Path

from app.core import SyncConfig, setup_logger, get_logger
from app.services import NetworkManager, FileMonitorManager, SyncEngine

logger = get_logger(__name__)


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
        
        self._shutdown_event = asyncio.Event()
        self._running = False
    
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
            logger.info("=" * 60)
            
            self._running = True
            
            # Initialize sync engine (scan existing files)
            await self.sync_engine.initialize()
            
            # Start network manager
            await self.network.start()
            
            # Start file monitor
            self.file_monitor.start()
            
            logger.info("All components started successfully")
            logger.info("Press Ctrl+C to stop")
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
        
        except Exception as e:
            logger.error(f"Error during startup: {e}", exc_info=True)
            raise
    
    async def stop(self) -> None:
        """
        Stop all application components gracefully.
        """
        if not self._running:
            return
        
        logger.info("Shutting down...")
        self._running = False
        
        try:
            # Stop file monitor (synchronous)
            self.file_monitor.stop()
            
            # Stop network manager (async)
            await self.network.stop()
            
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
    
    return parser.parse_args()


def create_config_from_args(args) -> SyncConfig:
    """
    Create configuration from command-line arguments.
    
    Args:
        args: Parsed command-line arguments.
    
    Returns:
        SyncConfig instance.
    """
    # Try loading from config file first
    if args.config:
        logger.info(f"Loading configuration from {args.config}")
        return SyncConfig.load(args.config)
    
    # Try environment variables
    try:
        return SyncConfig.from_env()
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
        listen_port=args.port or 8000
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
    setup_logger(
        name='app',
        level=log_level,
        log_file=args.log_file,
        console=True
    )
    
    # Also setup root logger for all modules
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
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda s=sig: app._signal_handler(s, None)
        )
    
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
