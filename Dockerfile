# Distributed File Synchronization Engine
# Multi-stage Docker build for optimal image size

FROM python:3.11-slim as builder

# Set working directory
WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Production stage
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Create non-root user
RUN useradd -m -u 1000 syncuser && \
    mkdir -p /sync /app/logs && \
    chown -R syncuser:syncuser /sync /app

# Copy installed dependencies from builder
COPY --from=builder /root/.local /home/syncuser/.local

# Copy application code
COPY --chown=syncuser:syncuser app/ /app/app/

# Set user
USER syncuser

# Add local bin to PATH
ENV PATH=/home/syncuser/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

# Expose default port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket; s=socket.socket(); s.connect(('localhost', 8000)); s.close()" || exit 1

# Volume for sync directory
VOLUME ["/sync"]

# Default command
ENTRYPOINT ["python", "-m", "app.main"]
CMD ["--help"]
