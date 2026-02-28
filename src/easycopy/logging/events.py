"""Structured log event model."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class LogEvent:
    """Represents one structured log event."""

    level: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize event to dictionary."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": self.level,
            "message": self.message,
            "context": self.context,
        }
