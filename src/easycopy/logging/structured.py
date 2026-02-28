"""Structured logger configuration.

Provides a JSON formatter and a file-backed logger for EasyCopy runs.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class JsonFormatter(logging.Formatter):
    """Formats log records as structured JSON lines."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize log record to JSON string."""
        payload: dict[str, object] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            payload["traceback"] = self.formatException(record.exc_info)

        reserved = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
        }
        extra_context = {
            key: value
            for key, value in record.__dict__.items()
            if key not in reserved
        }
        if extra_context:
            payload["context"] = extra_context

        return json.dumps(payload, default=str)


def configure_structured_logger(logs_dir: Optional[Path]) -> logging.Logger:
    """Configure package logger and return it."""
    if logs_dir is None:
        logs_dir = Path("./logs")

    logs_dir = Path(logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "easycopy.log"

    logger = logging.getLogger("easycopy")
    logger.setLevel(logging.INFO)
    # clear existing handlers to avoid duplicate lines
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)

    return logger
