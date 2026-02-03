"""
Tests for Protocol implementation.
"""

import pytest
import asyncio
from io import BytesIO

from app.core.protocol import Protocol, Message, MessageType


class TestProtocol:
    """Test suite for Protocol."""
    
    @pytest.mark.asyncio
    async def test_message_serialization(self):
        """Test message serialization and deserialization."""
        msg = Message(
            msg_type=MessageType.HELLO,
            payload={'version': '1.0'},
            node_id='node1'
        )
        
        # To dict
        data = msg.to_dict()
        assert data['type'] == 'HELLO'
        assert data['payload']['version'] == '1.0'
        assert data['node_id'] == 'node1'
        
        # From dict
        msg2 = Message.from_dict(data)
        assert msg2.msg_type == MessageType.HELLO
        assert msg2.payload == msg.payload
        assert msg2.node_id == msg.node_id
    
    @pytest.mark.asyncio
    async def test_message_json(self):
        """Test message JSON serialization."""
        msg1 = Message(
            msg_type=MessageType.SYNC_REQ,
            payload={'file': 'test.txt'},
            node_id='node1'
        )
        
        json_str = msg1.to_json()
        msg2 = Message.from_json(json_str)
        
        assert msg2.msg_type == msg1.msg_type
        assert msg2.payload == msg1.payload
        assert msg2.node_id == msg1.node_id
    
    @pytest.mark.asyncio
    async def test_send_receive_message(self):
        """Test sending and receiving messages over streams."""
        # Create in-memory streams
        reader = asyncio.StreamReader()
        writer_transport, writer_protocol = None, None
        
        # Mock writer
        class MockWriter:
            def __init__(self):
                self.data = BytesIO()
            
            def write(self, data):
                self.data.write(data)
            
            async def drain(self):
                pass
            
            def get_data(self):
                return self.data.getvalue()
        
        mock_writer = MockWriter()
        
        # Send message
        msg1 = Protocol.create_hello_message('node1')
        await Protocol.send_message(mock_writer, msg1)
        
        # Feed data to reader
        data = mock_writer.get_data()
        reader.feed_data(data)
        
        # Receive message
        msg2 = await Protocol.receive_message(reader)
        
        assert msg2 is not None
        assert msg2.msg_type == MessageType.HELLO
        assert msg2.node_id == 'node1'
    
    def test_create_hello_message(self):
        """Test creating HELLO message."""
        msg = Protocol.create_hello_message('node1')
        
        assert msg.msg_type == MessageType.HELLO
        assert msg.node_id == 'node1'
        assert 'version' in msg.payload
    
    def test_create_sync_request(self):
        """Test creating SYNC_REQ message."""
        vclock = {'node1': 1}
        msg = Protocol.create_sync_request('file.txt', 'hash123', vclock, 'node1')
        
        assert msg.msg_type == MessageType.SYNC_REQ
        assert msg.payload['file_path'] == 'file.txt'
        assert msg.payload['root_hash'] == 'hash123'
        assert msg.payload['vector_clock'] == vclock
    
    def test_create_error_message(self):
        """Test creating ERROR message."""
        msg = Protocol.create_error_message('Test error', 'node1')
        
        assert msg.msg_type == MessageType.ERROR
        assert msg.payload['error'] == 'Test error'
        assert msg.node_id == 'node1'
    
    def test_create_heartbeat(self):
        """Test creating HEARTBEAT message."""
        msg = Protocol.create_heartbeat('node1')
        
        assert msg.msg_type == MessageType.HEARTBEAT
        assert msg.node_id == 'node1'
        assert 'timestamp' in msg.payload
    
    @pytest.mark.asyncio
    async def test_chunk_data_encoding(self):
        """Test chunk data encoding and decoding."""
        chunk_data = b'Hello World!' * 100
        
        # Create mock writer
        class MockWriter:
            def __init__(self):
                self.messages = []
            
            def write(self, data):
                pass
            
            async def drain(self):
                pass
        
        mock_writer = MockWriter()
        
        # This would normally send the message
        # We'll test the decode separately
        import base64
        chunk_b64 = base64.b64encode(chunk_data).decode('ascii')
        
        msg = Message(
            msg_type=MessageType.CHUNK_DATA,
            payload={
                'file_path': 'test.txt',
                'chunk_index': 5,
                'chunk_data': chunk_b64,
                'chunk_size': len(chunk_data)
            },
            node_id='node1'
        )
        
        file_path, chunk_index, decoded_data = Protocol.decode_chunk_data(msg)
        
        assert file_path == 'test.txt'
        assert chunk_index == 5
        assert decoded_data == chunk_data
