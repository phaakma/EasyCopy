"""Shared data models across EasyCopy modules."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ChangeSet:
    """Represents change-detection output."""

    adds: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
