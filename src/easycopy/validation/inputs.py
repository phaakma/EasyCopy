"""Input payload validation for API calls (stub).

Detailed validation rules are added in Step 4.
"""

from typing import Any

def validate_inputs(payload: dict[str, Any]) -> None:
    """Basic payload validation placeholder.

    Raises `ValueError` for obviously invalid payload shapes.
    """
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    if "source" not in payload or "target" not in payload:
        raise ValueError("payload must include 'source' and 'target'")
