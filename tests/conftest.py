"""
Test configuration and fixtures.
"""

import pytest
import asyncio
import sys


# Configure pytest-asyncio
@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    if sys.platform == 'win32':
        # Use ProactorEventLoop on Windows for subprocess support
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.new_event_loop()
    
    yield loop
    loop.close()


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    from app.core.config import SyncConfig
    import tempfile
    
    temp_dir = tempfile.mkdtemp()
    
    config = SyncConfig(
        node_id='test_node',
        sync_directory=temp_dir,
        listen_host='127.0.0.1',
        listen_port=9000
    )
    
    yield config
    
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)
