"""
Async Logging Configuration for Distributed File Sync.

This module sets up structured logging with async support to avoid blocking
the event loop during log I/O operations.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from logging.handlers import RotatingFileHandler


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter that adds colors to console output.
    """
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        if record.levelname in self.COLORS:
            record.levelname = (f"{self.COLORS[record.levelname]}"
                              f"{record.levelname}{self.COLORS['RESET']}")
        return super().format(record)


def setup_logger(
    name: str = "file_sync",
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_dir: str = "logs",
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
    console: bool = True
) -> logging.Logger:
    """
    Configure and return a logger instance.
    
    Args:
        name: Logger name.
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Optional log file name. If None, uses f"{name}.log".
        log_dir: Directory to store log files.
        max_bytes: Maximum size of each log file before rotation.
        backup_count: Number of backup log files to keep.
        console: Whether to also log to console.
    
    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger
    
    # Create log directory if it doesn't exist
    if log_file:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            log_path / (log_file or f"{name}.log"),
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(level)
        
        # File format (detailed)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        
        # Console format (colored and concise)
        console_formatter = ColoredFormatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str = "file_sync") -> logging.Logger:
    """
    Get an existing logger or create a default one.
    
    Args:
        name: Logger name.
    
    Returns:
        Logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logger(name)
    return logger
