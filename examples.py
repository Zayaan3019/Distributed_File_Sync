"""
Example: Programmatic usage of the Distributed File Sync Engine.

This demonstrates how to use the sync engine as a library in your own applications.
"""

import asyncio
import logging
from pathlib import Path
import tempfile

from app.core import SyncConfig, setup_logger
from app.services import NetworkManager, FileMonitorManager, SyncEngine


async def example_basic_sync():
    """
    Example: Basic two-node sync setup.
    """
    print("=" * 60)
    print("Example: Basic Two-Node Sync")
    print("=" * 60)
    
    # Setup logging
    setup_logger(level=logging.INFO)
    
    # Create temporary directories for the example
    temp_dir1 = tempfile.mkdtemp(prefix="sync_node1_")
    temp_dir2 = tempfile.mkdtemp(prefix="sync_node2_")
    
    print(f"Node 1 directory: {temp_dir1}")
    print(f"Node 2 directory: {temp_dir2}")
    
    # Configuration for Node 1
    config1 = SyncConfig(
        node_id="example_node1",
        sync_directory=temp_dir1,
        listen_host="127.0.0.1",
        listen_port=9001
    )
    config1.add_peer("127.0.0.1", 9002)
    
    # Configuration for Node 2
    config2 = SyncConfig(
        node_id="example_node2",
        sync_directory=temp_dir2,
        listen_host="127.0.0.1",
        listen_port=9002
    )
    config2.add_peer("127.0.0.1", 9001)
    
    # Create components for Node 1
    async def handle_message_node1(message, peer_conn):
        await sync_engine1.handle_message(message, peer_conn)
    
    network1 = NetworkManager(config1, handle_message_node1)
    sync_engine1 = SyncEngine(config1, network1)
    file_monitor1 = FileMonitorManager(
        config1.sync_directory,
        sync_engine1.on_file_changed,
        sync_engine1.on_file_deleted,
        asyncio.get_event_loop()
    )
    
    # Create components for Node 2
    async def handle_message_node2(message, peer_conn):
        await sync_engine2.handle_message(message, peer_conn)
    
    network2 = NetworkManager(config2, handle_message_node2)
    sync_engine2 = SyncEngine(config2, network2)
    file_monitor2 = FileMonitorManager(
        config2.sync_directory,
        sync_engine2.on_file_changed,
        sync_engine2.on_file_deleted,
        asyncio.get_event_loop()
    )
    
    try:
        # Start both nodes
        print("\nStarting Node 1...")
        await sync_engine1.initialize()
        await network1.start()
        file_monitor1.start()
        
        print("Starting Node 2...")
        await sync_engine2.initialize()
        await network2.start()
        file_monitor2.start()
        
        # Wait for connection
        print("\nWaiting for nodes to connect...")
        await asyncio.sleep(2)
        
        # Create a test file on Node 1
        print("\nCreating test file on Node 1...")
        test_file1 = Path(temp_dir1) / "example.txt"
        test_file1.write_text("Hello from Node 1!\nThis is a test file.")
        
        # Wait for sync
        print("Waiting for synchronization...")
        await asyncio.sleep(3)
        
        # Check if file appeared on Node 2
        test_file2 = Path(temp_dir2) / "example.txt"
        if test_file2.exists():
            content = test_file2.read_text()
            print(f"\n✅ Success! File synced to Node 2:")
            print(f"Content: {content}")
        else:
            print("\n❌ File did not sync")
        
        # Modify file on Node 2
        print("\nModifying file on Node 2...")
        test_file2.write_text("Hello from Node 1!\nThis is a test file.\nModified by Node 2!")
        
        # Wait for sync back to Node 1
        print("Waiting for reverse synchronization...")
        await asyncio.sleep(3)
        
        # Check if modification synced back
        content1 = test_file1.read_text()
        if "Modified by Node 2" in content1:
            print(f"\n✅ Success! Modification synced back to Node 1:")
            print(f"Content: {content1}")
        else:
            print("\n❌ Modification did not sync")
        
        print("\n" + "=" * 60)
        print("Example completed successfully!")
        print("=" * 60)
        
        # Keep running for a bit
        await asyncio.sleep(2)
        
    finally:
        # Cleanup
        print("\nCleaning up...")
        file_monitor1.stop()
        file_monitor2.stop()
        await network1.stop()
        await network2.stop()
        
        # Remove temp directories
        import shutil
        shutil.rmtree(temp_dir1, ignore_errors=True)
        shutil.rmtree(temp_dir2, ignore_errors=True)


async def example_conflict_handling():
    """
    Example: Demonstrate conflict detection and resolution.
    """
    print("\n" + "=" * 60)
    print("Example: Conflict Detection and Resolution")
    print("=" * 60)
    
    # This would demonstrate concurrent edits and conflict resolution
    # Implementation similar to above but with simultaneous edits
    print("\n(See example_basic_sync for implementation pattern)")


async def example_merkle_tree_usage():
    """
    Example: Direct usage of Merkle trees for file comparison.
    """
    print("\n" + "=" * 60)
    print("Example: Merkle Tree Usage")
    print("=" * 60)
    
    from app.algorithms import MerkleTree
    
    # Create two temporary files
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f1:
        f1.write(b'A' * 10000)  # 10KB of 'A's
        file1 = f1.name
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f2:
        f2.write(b'A' * 10000)  # Same content
        file2 = f2.name
    
    try:
        # Build Merkle trees
        print("\nBuilding Merkle trees...")
        tree1 = await MerkleTree.from_file(file1)
        tree2 = await MerkleTree.from_file(file2)
        
        print(f"Tree 1 root hash: {tree1.root.hash[:32]}...")
        print(f"Tree 2 root hash: {tree2.root.hash[:32]}...")
        
        # Compare
        diff = MerkleTree.get_diff(tree1, tree2)
        print(f"\nDifference: {len(diff.changed_chunks)} chunks")
        
        if diff.is_empty():
            print("✅ Files are identical!")
        else:
            print(f"❌ Files differ in chunks: {diff.changed_chunks}")
        
        # Modify file2
        print("\nModifying file2...")
        with open(file2, 'r+b') as f:
            f.seek(0)
            f.write(b'B' * 4096)  # Change first chunk
        
        # Rebuild tree2 and compare
        tree2_modified = await MerkleTree.from_file(file2)
        diff = MerkleTree.get_diff(tree1, tree2_modified)
        
        print(f"\nAfter modification:")
        print(f"Changed chunks: {diff.changed_chunks}")
        print(f"Bytes to transfer: {diff.total_bytes()}")
        
        print("\n✅ Merkle tree example completed!")
        
    finally:
        # Cleanup
        Path(file1).unlink(missing_ok=True)
        Path(file2).unlink(missing_ok=True)


async def example_vector_clock_usage():
    """
    Example: Direct usage of vector clocks for causality tracking.
    """
    print("\n" + "=" * 60)
    print("Example: Vector Clock Usage")
    print("=" * 60)
    
    from app.algorithms import VectorClock, ClockRelation
    
    # Create clocks for three nodes
    clock_node1 = VectorClock("node1")
    clock_node2 = VectorClock("node2")
    clock_node3 = VectorClock("node3")
    
    print("\nInitial state:")
    print(f"Node 1: {clock_node1}")
    print(f"Node 2: {clock_node2}")
    print(f"Node 3: {clock_node3}")
    
    # Node 1 performs an operation
    print("\nNode 1 edits file...")
    clock_node1.increment()
    print(f"Node 1: {clock_node1}")
    
    # Node 1 syncs to Node 2
    print("\nNode 1 syncs to Node 2...")
    clock_node2.merge(clock_node1)
    print(f"Node 2 after merge: {clock_node2}")
    
    # Compare clocks
    relation = clock_node1.compare(clock_node2)
    print(f"\nClock relationship: {relation.value}")
    
    # Meanwhile, Node 3 edits offline (concurrent)
    print("\nNode 3 edits offline...")
    clock_node3.increment()
    print(f"Node 3: {clock_node3}")
    
    # Node 1 and Node 3 try to sync
    print("\nComparing Node 1 and Node 3:")
    relation = clock_node1.compare(clock_node3)
    print(f"Relationship: {relation.value}")
    
    if relation == ClockRelation.CONCURRENT:
        print("⚠️  Conflict detected! These edits are concurrent.")
    
    print("\n✅ Vector clock example completed!")


async def main():
    """
    Run all examples.
    """
    print("\n" + "=" * 60)
    print("Distributed File Sync Engine - Examples")
    print("=" * 60)
    
    # Run examples
    await example_merkle_tree_usage()
    await example_vector_clock_usage()
    await example_basic_sync()
    
    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    # Run examples
    asyncio.run(main())
