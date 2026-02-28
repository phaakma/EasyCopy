"""Changeset artifact generation."""

import csv
from pathlib import Path
from typing import Any

from easycopy.models import ChangeSet


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write dictionaries to CSV with unioned headers."""
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    headers: list[str] = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def write_changesets(change_set: ChangeSet, output_dir: str | Path) -> dict[str, str]:
    """Write adds/updates/deletes CSV files and return file locations."""
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)

    adds_path = base / "adds.csv"
    updates_path = base / "updates.csv"
    deletes_path = base / "deletes.csv"

    _write_rows(adds_path, change_set.adds)
    _write_rows(updates_path, change_set.updates)
    _write_rows(deletes_path, change_set.deletes)

    return {
        "adds": str(adds_path),
        "updates": str(updates_path),
        "deletes": str(deletes_path),
    }
