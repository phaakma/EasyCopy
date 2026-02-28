"""Execution orchestrator for EasyCopy copy methods."""

from typing import Any

from easycopy.change_detection import detect_changes
from easycopy.execution.feature_class import FeatureClassExecutor
from easycopy.execution.feature_service import FeatureServiceExecutor


def execute_copy(payload: dict[str, Any], logger: Any) -> dict[str, Any]:
    """Execute the selected copy method against target type."""
    method = payload["copy_method"]
    source = payload["source"]
    target = payload["target"]
    batch_size = payload["batch_size"]

    target_type = getattr(target, "kind", None) or getattr(target, "type", None)

    fs_executor = FeatureServiceExecutor()
    fc_executor = FeatureClassExecutor()

    logger.info("Executing workflow", extra={"method": method, "target_type": target_type})

    if method == "TRUNCATE_APPEND":
        if target_type == "FEATURE_SERVICE":
            return fs_executor.truncate_append(source, target, batch_size)
        return fc_executor.truncate_append(source, target)

    if method == "CHANGEDETECTION":
        source_records = list(getattr(source, "records", []))
        target_records = list(getattr(target, "records", []))
        changes = detect_changes(
            source_records=source_records,
            target_records=target_records,
            id_field=payload["id_field"],
            indexed_fields=set(getattr(target, "indexed_fields", [])),
        )

        if target_type == "FEATURE_SERVICE":
            return fs_executor.apply_changes(target, changes, batch_size)

        raise ValueError("CHANGEDETECTION is only supported for FEATURE_SERVICE targets")

    raise ValueError(f"Unsupported copy_method: {method}")
