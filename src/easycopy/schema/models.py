"""Schema models used by comparison utilities."""

from dataclasses import dataclass
from typing import List


@dataclass
class FieldModel:
    name: str
    type: str


@dataclass
class SchemaCompareResult:
    compatible: bool
    messages: List[str]
