"""Execution orchestrator for EasyCopy copy methods."""

from typing import Any

from easycopy.change_detection import detect_changes
from easycopy.execution.feature_class import FeatureClassExecutor
from easycopy.execution.feature_service import FeatureServiceExecutor


def _resolve_records(
    dataset: Any,
    dataset_type: str | None,
    id_field: str,
    fc_executor: FeatureClassExecutor,
    fs_executor: FeatureServiceExecutor,
) -> list[dict[str, Any]]:
    """Resolve comparable records for change detection from a dataset object."""
    if hasattr(dataset, "records"):
        return list(getattr(dataset, "records") or [])

    normalized_type = (dataset_type or "").upper()

    if normalized_type == "FEATURE_SERVICE":
        return fs_executor.read_records(dataset, id_field)

    try:
        return fc_executor.read_records(dataset, id_field)
    except Exception:
        query_fn = getattr(dataset, "query", None)
        if callable(query_fn):
            return fs_executor.read_records(dataset, id_field)
        raise


def execute_copy(payload: dict[str, Any], logger: Any) -> dict[str, Any]:
    """Execute the selected copy method against target type."""
    method = payload["copy_method"]
    source = payload["source"]
    target = payload["target"]
    batch_size = payload["batch_size"]

    source_type = getattr(source, "kind", None) or getattr(source, "type", None)
    target_type = getattr(target, "kind", None) or getattr(target, "type", None)

    fs_executor = FeatureServiceExecutor()
    fc_executor = FeatureClassExecutor()

    logger.info("Executing workflow", extra={"method": method, "target_type": target_type})

    if method == "TRUNCATE_APPEND":
        if target_type == "FEATURE_SERVICE":
            return fs_executor.truncate_append(source, target, batch_size)
        return fc_executor.truncate_append(source, target)

    if method == "CHANGEDETECTION":
        source_records = _resolve_records(
            dataset=source,
            dataset_type=source_type,
            id_field=payload["id_field"],
            fc_executor=fc_executor,
            fs_executor=fs_executor,
        )
        target_records = _resolve_records(
            dataset=target,
            dataset_type=target_type,
            id_field=payload["id_field"],
            fc_executor=fc_executor,
            fs_executor=fs_executor,
        )

        changes = detect_changes(
            source_records=source_records,
            target_records=target_records,
            id_field=payload["id_field"],
            indexed_fields=set(getattr(target, "indexed_fields", [])),
        )

        if target_type == "FEATURE_SERVICE":
            return fs_executor.apply_changes(target, changes, batch_size)
        return fc_executor.apply_changes(
            target=target,
            changes=changes,
            id_field=payload["id_field"],
        )

    raise ValueError(f"Unsupported copy_method: {method}")
