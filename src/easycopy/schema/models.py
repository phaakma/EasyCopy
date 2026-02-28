"""Schema model types used by the schema comparison engine."""

from dataclasses import dataclass, field
from typing import List


@dataclass(slots=True)
class FieldModel:
    """Represents a normalized field definition."""

    name: str
    type_name: str
    length: int | None = None
    nullable: bool = True


@dataclass(slots=True)
class SchemaCompareResult:
    """Represents schema compatibility and detail messages."""

    compatible: bool
    messages: list[str] = field(default_factory=list)
