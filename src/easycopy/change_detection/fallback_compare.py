"""Fallback compare path when SDF comparison is unavailable."""

from typing import Any


def compare_records(
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    id_field: str,
) -> dict[str, Any]:
    """Compare source and target records with deterministic dictionary logic."""
    source_index: dict[Any, dict[str, Any]] = {}
    target_index: dict[Any, dict[str, Any]] = {}

    for row in source_records:
        row_id = row.get(id_field)
        if row_id in source_index:
            raise ValueError(f"Duplicate id in source: {row_id!r}")
        source_index[row_id] = row

    for row in target_records:
        row_id = row.get(id_field)
        if row_id in target_index:
            raise ValueError(f"Duplicate id in target: {row_id!r}")
        target_index[row_id] = row

    source_ids = set(source_index)
    target_ids = set(target_index)

    add_ids = sorted(source_ids - target_ids)
    delete_ids = sorted(target_ids - source_ids)

    update_ids: list[Any] = []
    for row_id in sorted(source_ids & target_ids):
        src_row = source_index[row_id]
        tgt_row = target_index[row_id]
        src_compare = {k: v for k, v in src_row.items() if k != id_field}
        tgt_compare = {k: v for k, v in tgt_row.items() if k != id_field}
        if src_compare != tgt_compare:
            update_ids.append(row_id)

    return {
        "add_ids": add_ids,
        "update_ids": update_ids,
        "delete_ids": delete_ids,
        "strategy": "fallback",
    }
