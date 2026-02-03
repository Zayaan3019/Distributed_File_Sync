# Quick Start Guide

Get up and running with the Distributed File Sync Engine in 5 minutes!

## Prerequisites

- Python 3.11 or higher
- pip (Python package manager)

## Installation

```bash
# 1. Clone or download the project
cd Distributed_File_Sync

# 2. Install dependencies
pip install -r requirements.txt
```

## Quick Test - Two Node Setup

### Terminal 1: Start Node 1

```bash
python -m app.main \
  --node-id node1 \
  --dir ./sync_node1 \
  --port 8001 \
  --peer localhost:8002
```

### Terminal 2: Start Node 2

```bash
python -m app.main \
  --node-id node2 \
  --dir ./sync_node2 \
  --port 8002 \
  --peer localhost:8001
```

### Test the Sync

```bash
# Terminal 3: Create a test file in node1's directory
echo "Hello from Node 1!" > sync_node1/test.txt

# Wait a moment, then check node2's directory
cat sync_node2/test.txt
# Output: Hello from Node 1!

# Modify the file from node2
echo "Reply from Node 2!" >> sync_node2/test.txt

# Check node1's directory
cat sync_node1/test.txt
# Output:
# Hello from Node 1!
# Reply from Node 2!
```

🎉 **Success!** Your files are syncing across nodes!

## Docker Quick Start

### Single Command Setup

```bash
# Build and start 3-node cluster
docker-compose up --build

# In another terminal, create a test file
echo "Docker test" > data/node1/test.txt

# Check it appeared on other nodes
ls data/node2/
ls data/node3/
```

### Access Logs

```bash
# View node1 logs
docker-compose logs -f node1

# View all logs
docker-compose logs -f
```

### Stop Cluster

```bash
docker-compose down
```

## Configuration Examples

### Example 1: Home Network Sync

Sync documents across your home computers.

**Computer 1 (192.168.1.100):**
```bash
python -m app.main \
  --node-id home-desktop \
  --dir ~/Documents/Sync \
  --host 0.0.0.0 \
  --port 8001 \
  --peer 192.168.1.101:8001
```

**Computer 2 (192.168.1.101):**
```bash
python -m app.main \
  --node-id home-laptop \
  --dir ~/Documents/Sync \
  --host 0.0.0.0 \
  --port 8001 \
  --peer 192.168.1.100:8001
```

### Example 2: Configuration File

Create `config.json`:

```json
{
  "node_id": "my-node",
  "sync_directory": "/path/to/sync",
  "listen_host": "0.0.0.0",
  "listen_port": 8001,
  "peers": [
    {"host": "peer1.example.com", "port": 8001},
    {"host": "peer2.example.com", "port": 8001}
  ],
  "chunk_size": 4096,
  "max_retries": 5
}
```

Start with config:
```bash
python -m app.main --config config.json
```

## Common Commands

### Check Logs

```bash
# View real-time logs
tail -f logs/file_sync.log

# Search for errors
grep ERROR logs/file_sync.log

# View connection events
grep "Connected to peer" logs/file_sync.log
```

### Run Tests

```bash
# All tests
pytest tests/

# Specific test
pytest tests/test_vector_clock.py -v

# With coverage
pytest tests/ --cov=app
```

### Debugging

```bash
# Enable debug logging
python -m app.main \
  --node-id debug-node \
  --dir ./sync \
  --port 8001 \
  --log-level DEBUG
```

## Troubleshooting

### Issue: "Address already in use"

```bash
# Find process using the port
# Windows:
netstat -ano | findstr :8001

# Linux/Mac:
lsof -i :8001

# Use a different port
python -m app.main --port 8002 ...
```

### Issue: Peers not connecting

1. Check firewall settings
2. Verify peer addresses are correct
3. Ensure both nodes are running
4. Check logs for connection errors

### Issue: Files not syncing

1. Check both nodes are connected (look for "Connected to peer" in logs)
2. Verify file is not in ignored patterns
3. Check directory permissions
4. Review logs for errors

## Next Steps

- Read the full [README.md](README.md) for architecture details
- Explore [ARCHITECTURE.md](ARCHITECTURE.md) for deep dive
- Check [examples/](examples/) for more use cases
- Join our community for support

## Performance Tips

1. **Large files**: Increase chunk size
   ```bash
   # In config.json
   "chunk_size": 1048576  # 1MB chunks for large media files
   ```

2. **Many small files**: Keep default 4KB chunks

3. **High latency networks**: Increase timeouts
   ```bash
   "heartbeat_interval": 60.0,
   "retry_delay": 5.0
   ```

## Security Note

⚠️ **This implementation does not include encryption or authentication.**

For production use:
- Run over VPN
- Use firewall rules to restrict access
- Implement TLS/SSL wrapper
- Add authentication layer

---

**Need Help?** Check the README.md or open an issue!
