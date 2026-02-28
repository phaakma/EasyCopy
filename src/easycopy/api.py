"""Public EasyCopy API facade."""

from dataclasses import asdict
from typing import Any

from easycopy.config import RuntimePaths


class _EasyCopyFacade:
    """Singleton facade exposing the public copy operation."""

    def copy_data(
        self,
        *,
        source: Any,
        target: Any,
        copy_method: str = "TRUNCATE_APPEND",
        schema_comparison_type: str = "SOFT",
        log_changesets: bool = False,
        id_field: str | None = None,
        logs_dir: str | None = None,
        changesets_dir: str | None = None,
        batch_size: int = 200,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Validate top-level parameters and return normalized request payload."""
        runtime_paths = RuntimePaths.from_inputs(logs_dir, changesets_dir)
        runtime_paths.ensure()

        normalized = {
            "source": source,
            "target": target,
            "copy_method": copy_method.upper(),
            "schema_comparison_type": schema_comparison_type.upper(),
            "log_changesets": bool(log_changesets),
            "id_field": id_field,
            "batch_size": int(batch_size),
            "dry_run": bool(dry_run),
            "runtime_paths": asdict(runtime_paths),
        }
        return normalized


EasyCopy = _EasyCopyFacade()

__all__ = ["EasyCopy"]
