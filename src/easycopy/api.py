"""Public EasyCopy API facade with modular orchestration."""

from dataclasses import asdict
from typing import Any

from easycopy.change_detection.changesets import write_changesets
from easycopy.config import RuntimePaths
from easycopy.execution import execute_copy
from easycopy.logging import configure_structured_logger
from easycopy.schema import compare_schema
from easycopy.validation import (
    validate_environment,
    validate_inputs,
    validate_target_contract,
)


class _EasyCopyFacade:
    """Singleton facade exposing copy orchestration."""

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
        """Run preflight checks and execute the selected copy workflow."""
        runtime_paths = RuntimePaths.from_inputs(logs_dir, changesets_dir)
        runtime_paths.ensure()

        payload = {
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

        logger = configure_structured_logger(runtime_paths.logs_dir)
        logger.info("EasyCopy run started", extra={"copy_method": payload["copy_method"]})
        validate_environment()

        logger.info("Validating input payload")
        validate_inputs(payload)
        validate_target_contract(payload)

        logger.info("Comparing schemas", extra={"mode": payload["schema_comparison_type"]})
        schema_result = compare_schema(
            source=payload["source"],
            target=payload["target"],
            mode=payload["schema_comparison_type"],
        )
        if not schema_result.compatible:
            logger.error("Schema comparison failed", extra={"messages": schema_result.messages})
            return {
                "ok": False,
                "stage": "schema",
                "errors": schema_result.messages,
            }

        result = execute_copy(payload=payload, logger=logger)

        changeset = getattr(payload.get("target"), "latest_changeset", None)
        if payload["log_changesets"] and changeset is not None:
            files = write_changesets(changeset, runtime_paths.changesets_dir)
            result["changeset_files"] = files

        logger.info("EasyCopy run completed", extra={"ok": result.get("ok", False)})
        return result


EasyCopy = _EasyCopyFacade()

__all__ = ["EasyCopy"]
