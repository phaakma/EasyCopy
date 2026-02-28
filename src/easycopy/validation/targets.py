"""Target contract validation rules."""

from typing import Any


def validate_target_contract(payload: dict[str, Any]) -> None:
    """Validate source/target contracts and operation constraints."""
    method = payload["copy_method"]
    source = payload["source"]
    target = payload["target"]

    source_type = getattr(source, "kind", None) or getattr(source, "type", None)
    target_type = getattr(target, "kind", None) or getattr(target, "type", None)

    allowed_types = {"FEATURE_SERVICE", "FEATURE_CLASS", "TABLE"}

    if source_type and source_type not in allowed_types:
        raise ValueError(f"Unsupported source type: {source_type}")
    if target_type and target_type not in allowed_types:
        raise ValueError(f"Unsupported target type: {target_type}")

    if method == "TRUNCATE_APPEND":
        target_sync_enabled = bool(getattr(target, "sync_enabled", False))
        target_is_versioned = bool(getattr(target, "is_versioned", False))

        if target_type == "FEATURE_SERVICE" and target_sync_enabled:
            raise ValueError(
                "TRUNCATE_APPEND cannot run against sync-enabled feature services"
            )

        if target_type == "FEATURE_CLASS" and target_is_versioned:
            raise ValueError(
                "TRUNCATE_APPEND cannot run against versioned feature classes"
            )

    if method == "CHANGEDETECTION" and not payload.get("id_field"):
        raise ValueError("CHANGEDETECTION requires id_field")
