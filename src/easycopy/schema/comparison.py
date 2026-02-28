"""Schema comparison utilities (lightweight stub)."""

from easycopy.schema.models import SchemaCompareResult
from typing import Any


def compare_schema(*, source: Any, target: Any, mode: str = "SOFT") -> SchemaCompareResult:
    """Lightweight comparison that always returns compatible for now."""
    return SchemaCompareResult(compatible=True, messages=[])
