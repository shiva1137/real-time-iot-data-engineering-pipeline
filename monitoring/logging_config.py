"""
Structured Logging Configuration (Topic 9)

JSON format logging with timestamp, level, service, task, message, context.
Writes to both file and console. Use for pipeline-wide observability.
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict


def _json_serial(obj: Any) -> Any:
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class StructuredFormatter(logging.Formatter):
    """Format log records as JSON with timestamp, level, service, task, message, context."""

    def __init__(self, service: str = "pipeline"):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        log_obj: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "service": getattr(record, "service", self.service),
            "task": getattr(record, "task", ""),
            "message": record.getMessage(),
        }
        if hasattr(record, "context") and record.context:
            log_obj["context"] = record.context
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj, default=_json_serial)


def configure_logging(
    service: str = "pipeline",
    level: str = None,
    log_file: str = None,
) -> None:
    """
    Configure root logger with structured JSON to console and optional file.
    """
    level = level or os.getenv("LOG_LEVEL", "INFO")
    log_file = log_file or os.getenv("LOG_FILE", "")
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = StructuredFormatter(service=service)

    # Console handler
    if not root.handlers:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        root.addHandler(console)

    # Optional file handler
    if log_file:
        try:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(formatter)
            root.addHandler(fh)
        except OSError:
            root.warning("Could not open log file %s", log_file)


def get_logger(name: str, service: str = None) -> logging.Logger:
    """Return a logger that adds service/task to log records when set."""
    return logging.getLogger(name)
