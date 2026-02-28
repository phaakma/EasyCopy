"""Change detection orchestration engine."""

from typing import Any

from easycopy.change_detection.fallback_compare import compare_records
from easycopy.change_detection.geometry import geometries_equal
from easycopy.change_detection.sdf_compare import try_sdf_compare
from easycopy.models import ChangeSet


def detect_changes(
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    id_field: str,
    indexed_fields: set[str] | None = None,
) -> ChangeSet:
    """Detect adds, updates, and deletes between source and target datasets."""
    if not id_field:
        raise ValueError("id_field is required for change detection")

    if indexed_fields is not None and id_field not in indexed_fields:
        warning = f"id_field '{id_field}' is not indexed; performance may degrade"
    else:
        warning = ""

    source_by_id = {row[id_field]: row for row in source_records}
    target_by_id = {row[id_field]: row for row in target_records}

    if len(source_by_id) != len(source_records):
        raise ValueError("Duplicate id values found in source data")
    if len(target_by_id) != len(target_records):
        raise ValueError("Duplicate id values found in target data")

    try:
        import pandas as pd

        sdf_result = try_sdf_compare(
            source_df=pd.DataFrame(source_records),
            target_df=pd.DataFrame(target_records),
            id_field=id_field,
        )
    except Exception:
        sdf_result = None

    if sdf_result is None:
        compare_result = compare_records(source_records, target_records, id_field)
    else:
        compare_result = sdf_result

    change_set = ChangeSet()

    if warning:
        change_set.warnings.append(warning)

    for row_id in compare_result["add_ids"]:
        change_set.adds.append(source_by_id[row_id])

    for row_id in compare_result["delete_ids"]:
        change_set.deletes.append({id_field: row_id})

    for row_id in compare_result["update_ids"]:
        source_row = source_by_id[row_id]
        target_row = target_by_id[row_id]

        source_geom = source_row.get("geometry")
        target_geom = target_row.get("geometry")

        if not geometries_equal(source_geom, target_geom):
            change_set.updates.append(source_row)
            continue

        src_non_geom = {k: v for k, v in source_row.items() if k != "geometry"}
        tgt_non_geom = {k: v for k, v in target_row.items() if k != "geometry"}
        if src_non_geom != tgt_non_geom:
            change_set.updates.append(source_row)

    return change_set
