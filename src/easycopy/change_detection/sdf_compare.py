"""Spatially enabled DataFrame compare path."""

from typing import Any

import pandas as pd


def try_sdf_compare(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    id_field: str,
) -> dict[str, Any] | None:
    """Try DataFrame merge-based comparison and return None on unsupported path."""
    if id_field not in source_df.columns or id_field not in target_df.columns:
        return None

    if source_df[id_field].duplicated().any() or target_df[id_field].duplicated().any():
        return None

    merged = source_df.merge(
        target_df,
        on=id_field,
        how="outer",
        indicator=True,
        suffixes=("_src", "_tgt"),
    )

    add_ids = merged.loc[merged["_merge"] == "left_only", id_field].tolist()
    delete_ids = merged.loc[merged["_merge"] == "right_only", id_field].tolist()

    common = merged.loc[merged["_merge"] == "both"].copy()
    update_ids: list[Any] = []
    comparable_columns = [
        col for col in source_df.columns if col != id_field and f"{col}_tgt" in common.columns
    ]

    for _, row in common.iterrows():
        changed = False
        for col in comparable_columns:
            src_value = row[f"{col}_src"]
            tgt_value = row[f"{col}_tgt"]
            if pd.isna(src_value) and pd.isna(tgt_value):
                continue
            if src_value != tgt_value:
                changed = True
                break
        if changed:
            update_ids.append(row[id_field])

    return {
        "add_ids": add_ids,
        "update_ids": update_ids,
        "delete_ids": delete_ids,
        "strategy": "sdf",
    }
