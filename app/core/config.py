"""
Configuration Management for Distributed File Sync.

This module handles peer configuration including node identification,
network settings, and synchronization parameters.
"""

import os
import json
from typing import List, Dict, Optional
from dataclasses import dataclass, field, asdict
from pathlib import Path


@dataclass
class PeerConfig:
    """
    Configuration for a peer node in the distributed system.
    
    Attributes:
        host: IP address or hostname to bind to.
        port: TCP port to listen on.
    """
    host: str
    port: int
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'PeerConfig':
        """Create from dictionary."""
        return cls(host=data['host'], port=data['port'])


@dataclass
class SyncConfig:
    """
    Main configuration for the file synchronization node.
    
    Attributes:
        node_id: Unique identifier for this node.
        sync_directory: Path to the directory to synchronize.
        listen_host: Host address to bind server to.
        listen_port: Port to bind server to.
        peers: List of peer configurations to connect to.
        chunk_size: Size of file chunks for Merkle tree (bytes).
        max_retries: Maximum number of connection retry attempts.
        retry_delay: Initial delay between retries (seconds).
        heartbeat_interval: Interval for sending heartbeats (seconds).
        sync_interval: Interval for checking sync status (seconds).
        psk: Pre-shared key for mutual peer authentication and transit
            encryption. Required unless `allow_insecure` is set.
        allow_insecure: Explicitly run without authentication or encryption.
            Off by default; the Phase 0 audit showed an unauthenticated peer
            could write arbitrary files, so this must be a deliberate choice.
        catalog_path: SQLite catalog location. Defaults to
            ``<sync_directory>/.dfs/catalog.db``.
        enable_catalog: Persist metadata to the catalog. On by default; FUSE
            mode requires it.
        fuse_mount: Mount point for FUSE mode. When None the daemon runs in
            its original watchdog mode, unchanged, on Windows and Linux alike.
        fuse_allow_other: Pass ``allow_other`` to the FUSE mount.
        tombstone_ttl_days: Soft retention for deletion tombstones.
    """
    node_id: str
    sync_directory: str
    listen_host: str = "127.0.0.1"
    listen_port: int = 8000
    peers: List[PeerConfig] = field(default_factory=list)
    chunk_size: int = 4096
    max_retries: int = 5
    retry_delay: float = 1.0
    heartbeat_interval: float = 30.0
    sync_interval: float = 5.0
    psk: Optional[str] = None
    allow_insecure: bool = False
    catalog_path: Optional[str] = None
    enable_catalog: bool = True
    fuse_mount: Optional[str] = None
    fuse_allow_other: bool = False
    tombstone_ttl_days: float = 30.0

    def __post_init__(self):
        """Validate and normalize configuration."""
        # Ensure sync directory exists
        Path(self.sync_directory).mkdir(parents=True, exist_ok=True)

        # Validate node_id
        if not self.node_id or not self.node_id.strip():
            raise ValueError("node_id cannot be empty")

        # Validate ports
        if not (1024 <= self.listen_port <= 65535):
            raise ValueError(f"Invalid listen_port: {self.listen_port}")

        for peer in self.peers:
            if not (1024 <= peer.port <= 65535):
                raise ValueError(f"Invalid peer port: {peer.port}")

        # The catalog lives beside the data, not inside it: a database file
        # sitting in the synced tree would be picked up by the file monitor,
        # hashed, and replicated to peers, which is both wasteful and wrong.
        # `.dfs` is in the ignore list for exactly this reason.
        if self.catalog_path is None:
            self.catalog_path = str(Path(self.sync_directory) / ".dfs" / "catalog.db")

        if self.psk is not None and not self.psk.strip():
            raise ValueError("psk cannot be blank; omit it or set allow_insecure")

        if self.fuse_mount is not None and not self.enable_catalog:
            raise ValueError(
                "FUSE mode requires the catalog: inode numbers must be stable "
                "across remounts and vector clocks must survive a restart"
            )

    def is_secure(self) -> bool:
        """
        Report whether peer traffic will be authenticated and encrypted.

        Returns:
            True when a pre-shared key is configured.
        """
        return bool(self.psk)
    
    def add_peer(self, host: str, port: int) -> None:
        """
        Add a peer to the configuration.
        
        Args:
            host: Peer host address.
            port: Peer port number.
        """
        if not (1024 <= port <= 65535):
            raise ValueError(f"Invalid port: {port}")
        
        # Avoid adding self
        if host == self.listen_host and port == self.listen_port:
            return
        
        # Avoid duplicates
        for peer in self.peers:
            if peer.host == host and peer.port == port:
                return
        
        self.peers.append(PeerConfig(host=host, port=port))
    
    def remove_peer(self, host: str, port: int) -> bool:
        """
        Remove a peer from the configuration.
        
        Args:
            host: Peer host address.
            port: Peer port number.
        
        Returns:
            True if peer was removed, False if not found.
        """
        for i, peer in enumerate(self.peers):
            if peer.host == host and peer.port == port:
                self.peers.pop(i)
                return True
        return False
    
    def get_peer_address(self, peer: PeerConfig) -> str:
        """
        Get a string representation of peer address.
        
        Args:
            peer: Peer configuration.
        
        Returns:
            String in format "host:port".
        """
        return f"{peer.host}:{peer.port}"
    
    def to_dict(self) -> Dict:
        """Serialize configuration to dictionary."""
        data = asdict(self)
        data['peers'] = [peer.to_dict() for peer in self.peers]
        return data
    
    def to_json(self, indent: int = 2) -> str:
        """
        Serialize configuration to JSON string.
        
        Args:
            indent: Number of spaces for indentation.
        
        Returns:
            JSON string representation.
        """
        return json.dumps(self.to_dict(), indent=indent)
    
    def save(self, file_path: str) -> None:
        """
        Save configuration to a JSON file.
        
        Args:
            file_path: Path to save configuration to.
        """
        with open(file_path, 'w') as f:
            f.write(self.to_json())
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'SyncConfig':
        """
        Deserialize configuration from dictionary.
        
        Args:
            data: Dictionary containing configuration.
        
        Returns:
            New SyncConfig instance.
        """
        # Convert peer dicts to PeerConfig objects
        peers_data = data.pop('peers', [])
        peers = [PeerConfig.from_dict(p) for p in peers_data]
        
        return cls(peers=peers, **data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'SyncConfig':
        """
        Deserialize configuration from JSON string.
        
        Args:
            json_str: JSON string containing configuration.
        
        Returns:
            New SyncConfig instance.
        """
        return cls.from_dict(json.loads(json_str))
    
    @classmethod
    def load(cls, file_path: str) -> 'SyncConfig':
        """
        Load configuration from a JSON file.
        
        Args:
            file_path: Path to configuration file.
        
        Returns:
            New SyncConfig instance.
        
        Raises:
            FileNotFoundError: If configuration file doesn't exist.
        """
        with open(file_path, 'r') as f:
            return cls.from_json(f.read())
    
    @classmethod
    def from_env(cls) -> 'SyncConfig':
        """
        Create configuration from environment variables.
        
        Environment variables:
            SYNC_NODE_ID: Node identifier
            SYNC_DIRECTORY: Directory to synchronize
            SYNC_LISTEN_HOST: Host to listen on (default: 127.0.0.1)
            SYNC_LISTEN_PORT: Port to listen on (default: 8000)
            SYNC_PEERS: Comma-separated list of peer addresses (host:port)
            SYNC_PSK: Pre-shared key for peer authentication and encryption
            SYNC_ALLOW_INSECURE: Set to 1/true to run without either
            SYNC_FUSE_MOUNT: Mount point to expose the tree through FUSE
            SYNC_CATALOG_PATH: Override the SQLite catalog location

        Returns:
            New SyncConfig instance.

        Raises:
            ValueError: If required environment variables are missing.
        """
        node_id = os.getenv('SYNC_NODE_ID')
        if not node_id:
            raise ValueError("SYNC_NODE_ID environment variable is required")

        sync_directory = os.getenv('SYNC_DIRECTORY')
        if not sync_directory:
            raise ValueError("SYNC_DIRECTORY environment variable is required")

        listen_host = os.getenv('SYNC_LISTEN_HOST', '127.0.0.1')
        listen_port = int(os.getenv('SYNC_LISTEN_PORT', '8000'))

        insecure = os.getenv('SYNC_ALLOW_INSECURE', '').strip().lower()

        config = cls(
            node_id=node_id,
            sync_directory=sync_directory,
            listen_host=listen_host,
            listen_port=listen_port,
            psk=os.getenv('SYNC_PSK') or None,
            allow_insecure=insecure in ('1', 'true', 'yes', 'on'),
            fuse_mount=os.getenv('SYNC_FUSE_MOUNT') or None,
            catalog_path=os.getenv('SYNC_CATALOG_PATH') or None,
        )
        
        # Parse peers from environment
        peers_str = os.getenv('SYNC_PEERS', '')
        if peers_str:
            for peer_addr in peers_str.split(','):
                peer_addr = peer_addr.strip()
                if ':' in peer_addr:
                    host, port_str = peer_addr.rsplit(':', 1)
                    try:
                        port = int(port_str)
                        config.add_peer(host, port)
                    except ValueError:
                        raise ValueError(f"Invalid peer address: {peer_addr}")
        
        return config
    
    def __str__(self) -> str:
        """String representation of configuration."""
        return f"SyncConfig(node={self.node_id}, dir={self.sync_directory}, peers={len(self.peers)})"
    
    def __repr__(self) -> str:
        """Detailed representation of configuration."""
        return (f"SyncConfig(node_id='{self.node_id}', sync_directory='{self.sync_directory}', "
                f"listen={self.listen_host}:{self.listen_port}, peers={len(self.peers)})")
