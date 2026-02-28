"""Feature class execution strategies."""

from typing import Any


class FeatureClassExecutor:
    """Executes copy operations against geodatabase feature classes."""

    def truncate_append(self, source: Any, target: Any) -> dict[str, Any]:
        """Run truncate + append for feature classes via ArcPy."""
        try:
            import arcpy
        except Exception as exc:
            raise RuntimeError("arcpy is required for feature class execution") from exc

        arcpy.management.TruncateTable(target.path)
        arcpy.management.Append(inputs=source.path, target=target.path, schema_type="NO_TEST")

        return {"ok": True, "method": "TRUNCATE_APPEND", "processed": "all"}
