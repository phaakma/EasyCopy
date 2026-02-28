"""Feature service execution strategies."""

from typing import Any

from easycopy.execution.batching import chunked
from easycopy.models import ChangeSet


class FeatureServiceExecutor:
    """Executes copy operations against ArcGIS Feature Services."""

    def truncate_append(self, source: Any, target: Any, batch_size: int) -> dict[str, Any]:
        """Truncate target and append source rows in batches."""
        rows = list(getattr(source, "records", []))
        # Expect target.manager.truncate() to be present on feature layer object
        if hasattr(target, "manager") and hasattr(target.manager, "truncate"):
            target.manager.truncate()
        total = 0
        for batch in chunked(rows, batch_size):
            response = target.edit_features(adds=batch)
            if not response:
                raise RuntimeError("edit_features returned empty response on append")
            total += len(batch)
        return {"ok": True, "method": "TRUNCATE_APPEND", "processed": total}

    def apply_changes(self, target: Any, changes: ChangeSet, batch_size: int) -> dict[str, Any]:
        """Apply adds, updates, and deletes with batched edit_features calls."""
        add_count = 0
        update_count = 0
        delete_count = 0

        for batch in chunked(changes.adds, batch_size):
            target.edit_features(adds=batch)
            add_count += len(batch)

        for batch in chunked(changes.updates, batch_size):
            target.edit_features(updates=batch)
            update_count += len(batch)

        delete_ids = [row[next(iter(row))] for row in changes.deletes]
        for batch in chunked(delete_ids, batch_size):
            target.edit_features(deletes=",".join(str(value) for value in batch))
            delete_count += len(batch)

        return {
            "ok": True,
            "method": "CHANGEDETECTION",
            "adds": add_count,
            "updates": update_count,
            "deletes": delete_count,
        }
"""Feature service execution strategies."""

from typing import Any

from easycopy.execution.batching import chunked
from easycopy.models import ChangeSet


class FeatureServiceExecutor:
    """Executes copy operations against ArcGIS Feature Services."""

    def truncate_append(self, source: Any, target: Any, batch_size: int) -> dict[str, Any]:
        """Truncate target and append source rows in batches."""
        rows = list(getattr(source, "records", []))
        # target.manager.truncate might be implementation-specific; call if present
        if hasattr(target, "manager") and hasattr(target.manager, "truncate"):
            target.manager.truncate()
        total = 0
        for batch in chunked(rows, batch_size):
            response = target.edit_features(adds=batch)
            if not response:
                raise RuntimeError("edit_features returned empty response on append")
            total += len(batch)
        return {"ok": True, "method": "TRUNCATE_APPEND", "processed": total}

    def apply_changes(self, target: Any, changes: ChangeSet, batch_size: int) -> dict[str, Any]:
        """Apply adds, updates, and deletes with batched edit_features calls."""
        add_count = 0
        update_count = 0
        delete_count = 0

        for batch in chunked(changes.adds, batch_size):
            target.edit_features(adds=batch)
            add_count += len(batch)

        for batch in chunked(changes.updates, batch_size):
            target.edit_features(updates=batch)
            update_count += len(batch)

        delete_ids = [row[next(iter(row))] for row in changes.deletes]
        for batch in chunked(delete_ids, batch_size):
            target.edit_features(deletes=",".join(str(value) for value in batch))
            delete_count += len(batch)

        return {
            "ok": True,
            "method": "CHANGEDETECTION",
            "adds": add_count,
            "updates": update_count,
            "deletes": delete_count,
        }
