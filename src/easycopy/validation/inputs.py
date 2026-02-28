"""Input payload validation for API calls."""

from typing import Any

_ALLOWED_METHODS = {"TRUNCATE_APPEND", "CHANGEDETECTION"}
_ALLOWED_SCHEMA_MODES = {"SOFT", "HARD"}


def validate_inputs(payload: dict[str, Any]) -> None:
    """Validate API parameters and required combinations."""
    method = payload["copy_method"]
    schema_mode = payload["schema_comparison_type"]
    batch_size = payload["batch_size"]

    if method not in _ALLOWED_METHODS:
        raise ValueError(
            f"copy_method must be one of {_ALLOWED_METHODS}; got {method!r}"
        )

    if schema_mode not in _ALLOWED_SCHEMA_MODES:
        raise ValueError(
            "schema_comparison_type must be 'SOFT' or 'HARD'"
        )

    if method == "CHANGEDETECTION" and not payload.get("id_field"):
        raise ValueError("id_field is required for CHANGEDETECTION")

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    if payload.get("source") is None:
        raise ValueError("source is required")

    if payload.get("target") is None:
        raise ValueError("target is required")
