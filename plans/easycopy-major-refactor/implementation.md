# EasyCopy Major Refactor

## Goal
Refactor EasyCopy into a package-first Python API with a singleton entrypoint (`EasyCopy.copy_data(...)`), modular internals, explicit validation/schema/change-detection engines, and release-ready local build tooling targeting Python >3.11 and `arcgis` >2.4.0.

## Prerequisites
Make sure that the user is currently on the `v2-refactor` branch before beginning implementation.
If not, move to the correct branch. If the branch does not exist, create it from `main`.

```powershell
git checkout main
git pull
git checkout -b v2-refactor
```

Project stack and tooling used in this plan:
- Python: `>3.11`
- Packaging: `setuptools`, `build`
- Runtime deps: `arcgis>2.4.0`, `pandas>=2.2.0`
- Test deps: `pytest>=8.3.0`, `pytest-mock>=3.14.0`

---

### Step-by-Step Instructions

#### Step 1: Package foundation and build scaffolding
- [ ] Create `pyproject.toml` and define package metadata, runtime constraints, and local build configuration.
- [ ] Copy and paste code below into `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "easycopy"
version = "0.1.0"
description = "API-first ArcGIS data copy orchestration"
readme = "README.md"
requires-python = ">3.11"
license = { text = "MIT" }
authors = [
  { name = "EasyCopy Maintainers" }
]
dependencies = [
  "arcgis>2.4.0",
  "pandas>=2.2.0"
]

[project.optional-dependencies]
dev = [
  "build>=1.2.2",
  "pytest>=8.3.0",
  "pytest-mock>=3.14.0"
]

[tool.setuptools]
package-dir = {"" = "src"}

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q"
```

- [ ] Create package root with canonical exports.
- [ ] Copy and paste code below into `src/easycopy/version.py`:

```python
"""EasyCopy package version."""

__all__ = ["__version__"]

__version__ = "0.1.0"
```

- [ ] Copy and paste code below into `src/easycopy/__init__.py`:

```python
"""Public package exports for EasyCopy."""

from easycopy.api import EasyCopy
from easycopy.version import __version__

__all__ = ["EasyCopy", "__version__"]
```

- [ ] Update readme for package install/build usage.
- [ ] Copy and paste code below into `README.md`:

```markdown
# EasyCopy

EasyCopy is an API-first Python package for copying ArcGIS data with explicit validation,
schema checks, and change-detection workflows.

## Requirements

- Python `>3.11`
- `arcgis>2.4.0`

## Install (local development)

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e .[dev]
```

## Build local artifacts

```bash
python -m build
```

Artifacts are generated in `dist/`.

## Import smoke test

```python
import easycopy
print(easycopy.__version__)
```
```

- [ ] Update the example launcher to use package import.
- [ ] Copy and paste code below into `examples/run_python.bat`:

```bat
@echo off
setlocal

if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
)

python examples\test.py
```

##### Step 1 Verification Checklist
- [ ] `pip install -e .[dev]` completes without dependency resolution errors.
- [ ] `python -m build` creates wheel and sdist in `dist/`.
- [ ] `python -c "import easycopy; print(easycopy.__version__)"` prints `0.1.0`.

#### Step 1 STOP & COMMIT
**STOP & COMMIT:** Agent must stop here and wait for the user to test, stage, and commit the change.

---

#### Step 2: Public API redesign with singleton entry point
- [ ] Add API config model and defaults for logs/changesets directories.
- [ ] Copy and paste code below into `src/easycopy/config.py`:

```python
"""Runtime configuration for EasyCopy API."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class RuntimePaths:
    """Represents runtime output directories used by EasyCopy."""

    logs_dir: Path
    changesets_dir: Path

    @classmethod
    def from_inputs(
        cls,
        logs_dir: str | Path | None,
        changesets_dir: str | Path | None,
    ) -> "RuntimePaths":
        """Build runtime paths from optional user inputs."""
        default_logs = Path("./logs")
        default_changesets = Path("./changesets")
        return cls(
            logs_dir=Path(logs_dir) if logs_dir else default_logs,
            changesets_dir=(
                Path(changesets_dir) if changesets_dir else default_changesets
            ),
        )

    def ensure(self) -> None:
        """Ensure runtime directories exist."""
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.changesets_dir.mkdir(parents=True, exist_ok=True)
```

- [ ] Implement public singleton facade with `EasyCopy.copy_data(...)`.
- [ ] Copy and paste code below into `src/easycopy/api.py`:

```python
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
```

- [ ] Re-export singleton from package root.
- [ ] Copy and paste code below into `src/easycopy/__init__.py`:

```python
"""Public package exports for EasyCopy."""

from easycopy.api import EasyCopy
from easycopy.version import __version__

__all__ = ["EasyCopy", "__version__"]
```

- [ ] Document API-first usage in readme.
- [ ] Copy and paste code below into `README.md`:

```markdown
# EasyCopy

EasyCopy is an API-first Python package for ArcGIS data movement and synchronization.

## Requirements

- Python `>3.11`
- `arcgis>2.4.0`

## Install

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e .[dev]
```

## Public API

```python
from easycopy import EasyCopy

result = EasyCopy.copy_data(
    source="source_layer",
    target="target_layer",
    copy_method="TRUNCATE_APPEND",
    schema_comparison_type="SOFT",
    log_changesets=True,
)
print(result)
```

If `logs_dir` and `changesets_dir` are not provided, EasyCopy creates:
- `./logs`
- `./changesets`
```

##### Step 2 Verification Checklist
- [ ] Calling `EasyCopy.copy_data(...)` returns normalized uppercase values for `copy_method` and `schema_comparison_type`.
- [ ] `./logs` and `./changesets` are created when omitted.
- [ ] Singleton import works with `from easycopy import EasyCopy`.

#### Step 2 STOP & COMMIT
**STOP & COMMIT:** Agent must stop here and wait for the user to test, stage, and commit the change.

---

#### Step 3: Modular namespace extraction (DR002-DR004)
- [ ] Create validation namespace.
- [ ] Copy and paste code below into `src/easycopy/validation/__init__.py`:

```python
"""Validation namespace exports."""

from easycopy.validation.environment import validate_environment
from easycopy.validation.inputs import validate_inputs
from easycopy.validation.targets import validate_target_contract

__all__ = [
    "validate_environment",
    "validate_inputs",
    "validate_target_contract",
]
```

- [ ] Create schema namespace.
- [ ] Copy and paste code below into `src/easycopy/schema/__init__.py`:

```python
"""Schema namespace exports."""

from easycopy.schema.comparison import compare_schema
from easycopy.schema.models import FieldModel, SchemaCompareResult

__all__ = ["FieldModel", "SchemaCompareResult", "compare_schema"]
```

- [ ] Create change detection namespace.
- [ ] Copy and paste code below into `src/easycopy/change_detection/__init__.py`:

```python
"""Change-detection namespace exports."""

from easycopy.change_detection.engine import detect_changes

__all__ = ["detect_changes"]
```

- [ ] Create execution namespace.
- [ ] Copy and paste code below into `src/easycopy/execution/__init__.py`:

```python
"""Execution namespace exports."""

from easycopy.execution.orchestrator import execute_copy

__all__ = ["execute_copy"]
```

- [ ] Create logging namespace.
- [ ] Copy and paste code below into `src/easycopy/logging/__init__.py`:

```python
"""Logging namespace exports."""

from easycopy.logging.structured import configure_structured_logger

__all__ = ["configure_structured_logger"]
```

- [ ] Update API orchestration imports to use namespace boundaries.
- [ ] Copy and paste code below into `src/easycopy/api.py`:

```python
"""Public EasyCopy API facade with modular orchestration."""

from dataclasses import asdict
from typing import Any

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
        validate_environment()
        validate_inputs(payload)
        validate_target_contract(payload)

        schema_result = compare_schema(
            source=payload["source"],
            target=payload["target"],
            mode=payload["schema_comparison_type"],
        )
        if not schema_result.compatible:
            return {
                "ok": False,
                "stage": "schema",
                "errors": schema_result.messages,
            }

        return execute_copy(payload=payload, logger=logger)


EasyCopy = _EasyCopyFacade()

__all__ = ["EasyCopy"]
```

##### Step 3 Verification Checklist
- [ ] All namespace packages import correctly.
- [ ] `python -c "from easycopy import EasyCopy"` succeeds.
- [ ] No circular import errors during `pytest -q` collection.

#### Step 3 STOP & COMMIT
**STOP & COMMIT:** Agent must stop here and wait for the user to test, stage, and commit the change.

---

#### Step 4: Pre-execution validation pipeline
- [ ] Add environment dependency and version checks.
- [ ] Copy and paste code below into `src/easycopy/validation/environment.py`:

```python
"""Runtime environment validation checks."""

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version


def _parse_version(raw: str) -> tuple[int, ...]:
    """Convert a semantic version string to comparable integer tuple."""
    cleaned = raw.split("+")[0].split("-")[0]
    parts = cleaned.split(".")
    numeric: list[int] = []
    for part in parts:
        digits = "".join(ch for ch in part if ch.isdigit())
        numeric.append(int(digits or "0"))
    return tuple(numeric)


def validate_environment() -> None:
    """Validate required packages and minimum versions."""
    missing: list[str] = []

    try:
        import_module("arcpy")
    except Exception:
        missing.append("arcpy")

    try:
        import_module("arcgis")
    except Exception:
        missing.append("arcgis")

    if missing:
        joined = ", ".join(sorted(missing))
        raise RuntimeError(f"Missing required runtime dependency/dependencies: {joined}")

    try:
        installed = version("arcgis")
    except PackageNotFoundError as exc:
        raise RuntimeError("arcgis package metadata not available") from exc

    if _parse_version(installed) <= (2, 4, 0):
        raise RuntimeError("arcgis must be greater than 2.4.0")
```

- [ ] Add parameter and rule validation.
- [ ] Copy and paste code below into `src/easycopy/validation/inputs.py`:

```python
"""Input payload validation for API calls."""

from typing import Any

_ALLOWED_METHODS = {"TRUNCATE_APPEND", "CHANGEDETECTION"}
_ALLOWED_SCHEMA_MODES = {"SOFT", "HARD"}


def validate_inputs(payload: dict[str, Any]) -> None:
    """Validate API parameters and required combinations."""
    method = payload["copy_method"]
    schema_mode = payload["schema_comparison_type"]
    batch_size = payload["batch_size"]

    if method not in _ALLOWED_METHODS:
        raise ValueError(
            f"copy_method must be one of {_ALLOWED_METHODS}; got {method!r}"
        )

    if schema_mode not in _ALLOWED_SCHEMA_MODES:
        raise ValueError(
            "schema_comparison_type must be 'SOFT' or 'HARD'"
        )

    if method == "CHANGEDETECTION" and not payload.get("id_field"):
        raise ValueError("id_field is required for CHANGEDETECTION")

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    if payload.get("source") is None:
        raise ValueError("source is required")

    if payload.get("target") is None:
        raise ValueError("target is required")
```

- [ ] Add source/target compatibility checks including truncate constraints.
- [ ] Copy and paste code below into `src/easycopy/validation/targets.py`:

```python
"""Target contract validation rules."""

from typing import Any


def validate_target_contract(payload: dict[str, Any]) -> None:
    """Validate source/target contracts and operation constraints."""
    method = payload["copy_method"]
    source = payload["source"]
    target = payload["target"]

    source_type = getattr(source, "kind", None) or getattr(source, "type", None)
    target_type = getattr(target, "kind", None) or getattr(target, "type", None)

    allowed_types = {"FEATURE_SERVICE", "FEATURE_CLASS", "TABLE"}

    if source_type and source_type not in allowed_types:
        raise ValueError(f"Unsupported source type: {source_type}")
    if target_type and target_type not in allowed_types:
        raise ValueError(f"Unsupported target type: {target_type}")

    if method == "TRUNCATE_APPEND":
        target_sync_enabled = bool(getattr(target, "sync_enabled", False))
        target_is_versioned = bool(getattr(target, "is_versioned", False))

        if target_type == "FEATURE_SERVICE" and target_sync_enabled:
            raise ValueError(
                "TRUNCATE_APPEND cannot run against sync-enabled feature services"
            )

        if target_type == "FEATURE_CLASS" and target_is_versioned:
            raise ValueError(
                "TRUNCATE_APPEND cannot run against versioned feature classes"
            )

    if method == "CHANGEDETECTION" and not payload.get("id_field"):
        raise ValueError("CHANGEDETECTION requires id_field")
```

- [ ] Wire validation pipeline into API before schema/execution.
- [ ] Copy and paste code below into `src/easycopy/api.py`:

```python
"""Public EasyCopy API facade with complete pre-execution validation."""

from dataclasses import asdict
from typing import Any

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
        """Execute EasyCopy workflow with fail-fast validation."""
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
        logger.info("Validating runtime environment")
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

        return execute_copy(payload=payload, logger=logger)


EasyCopy = _EasyCopyFacade()

__all__ = ["EasyCopy"]
```

##### Step 4 Verification Checklist
- [ ] Missing `arcpy`/`arcgis` surfaces actionable `RuntimeError`.
- [ ] Invalid method/schema mode/id-field combinations fail before execution.
- [ ] Sync-enabled FS + truncate and versioned FC + truncate constraints are enforced.

#### Step 4 STOP & COMMIT
**STOP & COMMIT:** Agent must stop here and wait for the user to test, stage, and commit the change.

---

#### Step 5: Schema comparison engine (soft and hard)
- [ ] Add schema models.
- [ ] Copy and paste code below into `src/easycopy/schema/models.py`:

```python
"""Schema model types used by schema comparison engine."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class FieldModel:
    """Represents a normalized field definition."""

    name: str
    type_name: str
    length: int | None = None
    nullable: bool = True


@dataclass(slots=True)
class SchemaCompareResult:
    """Represents schema compatibility and detail messages."""

    compatible: bool
    messages: list[str] = field(default_factory=list)
```

- [ ] Add type equivalency and coercion rules.
- [ ] Copy and paste code below into `src/easycopy/schema/rules.py`:

```python
"""Schema compatibility and coercion rules."""

from easycopy.schema.models import FieldModel

_EQUIVALENT_TYPES = {
    "INTEGER": {"INTEGER", "SMALLINTEGER"},
    "SMALLINTEGER": {"SMALLINTEGER", "INTEGER"},
    "DOUBLE": {"DOUBLE", "SINGLE"},
    "SINGLE": {"SINGLE", "DOUBLE"},
    "STRING": {"STRING"},
    "DATE": {"DATE"},
    "GUID": {"GUID"},
}


def is_equivalent_type(source: str, target: str) -> bool:
    """Return whether source and target types are equivalent."""
    source_key = source.upper()
    target_key = target.upper()
    return target_key in _EQUIVALENT_TYPES.get(source_key, {source_key})


def is_permitted_soft_coercion(source: FieldModel, target: FieldModel) -> bool:
    """Return whether a conversion is allowed in SOFT mode."""
    source_type = source.type_name.upper()
    target_type = target.type_name.upper()

    if source_type in {"INTEGER", "SMALLINTEGER", "DOUBLE", "SINGLE"} and target_type == "STRING":
        return True

    if source_type == "STRING" and target_type == "STRING":
        source_len = source.length or 0
        target_len = target.length or 0
        return source_len <= target_len

    return False
```

- [ ] Add comparison logic for `SOFT` and `HARD` modes.
- [ ] Copy and paste code below into `src/easycopy/schema/comparison.py`:

```python
"""Schema comparison engine."""

from collections.abc import Iterable

from easycopy.schema.models import FieldModel, SchemaCompareResult
from easycopy.schema.rules import is_equivalent_type, is_permitted_soft_coercion


def _normalize_fields(raw_fields: Iterable[dict]) -> dict[str, FieldModel]:
    """Normalize iterable field metadata into keyed field models."""
    normalized: dict[str, FieldModel] = {}
    for item in raw_fields:
        field = FieldModel(
            name=str(item["name"]),
            type_name=str(item["type"]),
            length=item.get("length"),
            nullable=bool(item.get("nullable", True)),
        )
        normalized[field.name.lower()] = field
    return normalized


def compare_schema(source: object, target: object, mode: str) -> SchemaCompareResult:
    """Compare source and target schemas and return compatibility result."""
    source_fields = _normalize_fields(getattr(source, "fields", []))
    target_fields = _normalize_fields(getattr(target, "fields", []))

    messages: list[str] = []
    is_soft = mode.upper() == "SOFT"

    for name, src_field in source_fields.items():
        tgt_field = target_fields.get(name)
        if tgt_field is None:
            messages.append(f"Missing field in target: {src_field.name}")
            continue

        if is_equivalent_type(src_field.type_name, tgt_field.type_name):
            continue

        if is_soft and is_permitted_soft_coercion(src_field, tgt_field):
            messages.append(
                f"Soft coercion allowed for field {src_field.name}: "
                f"{src_field.type_name} -> {tgt_field.type_name}"
            )
            continue

        messages.append(
            f"Type mismatch for field {src_field.name}: "
            f"{src_field.type_name} -> {tgt_field.type_name}"
        )

    compatible = not any(msg.startswith("Missing") or msg.startswith("Type mismatch") for msg in messages)
    return SchemaCompareResult(compatible=compatible, messages=messages)
```

- [ ] Keep API fail-fast behavior based on schema result.
- [ ] Copy and paste code below into `src/easycopy/api.py`:

```python
"""Public EasyCopy API facade with schema fail-fast behavior."""

from dataclasses import asdict
from typing import Any

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
        """Execute workflow with explicit preflight checks."""
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
        validate_environment()
        validate_inputs(payload)
        validate_target_contract(payload)

        schema_result = compare_schema(
            source=payload["source"],
            target=payload["target"],
            mode=payload["schema_comparison_type"],
        )
        if not schema_result.compatible:
            return {
                "ok": False,
                "stage": "schema",
                "errors": schema_result.messages,
            }

        return execute_copy(payload=payload, logger=logger)


EasyCopy = _EasyCopyFacade()

__all__ = ["EasyCopy"]
```

##### Step 5 Verification Checklist
- [ ] `HARD` mode rejects mismatched type pairs.
- [ ] `SOFT` mode allows configured coercions and records message output.
- [ ] Missing fields are always reported as incompatible.

#### Step 5 STOP & COMMIT
**STOP & COMMIT:** Agent must stop here and wait for the user to test, stage, and commit the change.

---

#### Step 6: Change detection engine with geometry support
- [ ] Add shared operation models.
- [ ] Copy and paste code below into `src/easycopy/models.py`:

```python
"""Shared data models across EasyCopy modules."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ChangeSet:
    """Represents change-detection output."""

    adds: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
```

- [ ] Add geometry comparison helpers.
- [ ] Copy and paste code below into `src/easycopy/change_detection/geometry.py`:

```python
"""Geometry comparison utilities."""

from math import isclose
from typing import Any


def _normalize_coords(coords: Any) -> Any:
    """Normalize nested coordinate arrays to rounded tuples for stable compare."""
    if isinstance(coords, (list, tuple)):
        return tuple(_normalize_coords(part) for part in coords)
    if isinstance(coords, float):
        return round(coords, 9)
    return coords


def geometries_equal(source: dict[str, Any] | None, target: dict[str, Any] | None) -> bool:
    """Return whether two geometry dictionaries are equivalent."""
    if source is None and target is None:
        return True
    if source is None or target is None:
        return False

    source_has_z = bool(source.get("hasZ", False))
    target_has_z = bool(target.get("hasZ", False))
    if source_has_z != target_has_z:
        return False

    source_curve = bool(source.get("curve", False) or source.get("hasCurves", False))
    target_curve = bool(target.get("curve", False) or target.get("hasCurves", False))
    if source_curve != target_curve:
        return False

    source_norm = _normalize_coords(source)
    target_norm = _normalize_coords(target)
    return source_norm == target_norm
```

- [ ] Add SDF compare path.
- [ ] Copy and paste code below into `src/easycopy/change_detection/sdf_compare.py`:

```python
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
```

- [ ] Add deterministic fallback compare path.
- [ ] Copy and paste code below into `src/easycopy/change_detection/fallback_compare.py`:

```python
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
```

- [ ] Implement change-detection orchestration with geometry support and warnings.
- [ ] Copy and paste code below into `src/easycopy/change_detection/engine.py`:

```python
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
```

##### Step 6 Verification Checklist
- [ ] Adds/updates/deletes are classified correctly for keyed input rows.
- [ ] Duplicate `id_field` values raise explicit `ValueError`.
- [ ] Geometry mismatches (including Z/curve flags) mark records as updates.
- [ ] Fallback and SDF paths produce parity on equivalent datasets.

#### Step 6 STOP & COMMIT
**STOP & COMMIT:** Agent must stop here and wait for the user to test, stage, and commit the change.

---

#### Step 7: Execution engine for TRUNCATE_APPEND and CHANGEDETECTION
- [ ] Add batching helpers.
- [ ] Copy and paste code below into `src/easycopy/execution/batching.py`:

```python
"""Batching helpers for execution operations."""

from collections.abc import Iterable
from typing import TypeVar

T = TypeVar("T")


def chunked(items: list[T], batch_size: int) -> Iterable[list[T]]:
    """Yield fixed-size chunks from a list."""
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    for index in range(0, len(items), batch_size):
        yield items[index : index + batch_size]
```

- [ ] Add feature-service execution path with REST edit batching.
- [ ] Copy and paste code below into `src/easycopy/execution/feature_service.py`:

```python
"""Feature service execution strategies."""

from typing import Any

from easycopy.execution.batching import chunked
from easycopy.models import ChangeSet


class FeatureServiceExecutor:
    """Executes copy operations against ArcGIS Feature Services."""

    def truncate_append(self, source: Any, target: Any, batch_size: int) -> dict[str, Any]:
        """Truncate target and append source rows in batches."""
        rows = list(getattr(source, "records", []))
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
```

- [ ] Add feature-class execution path with ArcPy append handling.
- [ ] Copy and paste code below into `src/easycopy/execution/feature_class.py`:

```python
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
```

- [ ] Add method orchestrator and workflow routing.
- [ ] Copy and paste code below into `src/easycopy/execution/orchestrator.py`:

```python
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
```

##### Step 7 Verification Checklist
- [ ] `TRUNCATE_APPEND` routes correctly by target type.
- [ ] `CHANGEDETECTION` applies batched adds/updates/deletes to feature services.
- [ ] Batch chunk boundaries are respected exactly (`N`, `N+1`, and empty datasets).
- [ ] Failure paths bubble actionable errors.

#### Step 7 STOP & COMMIT
**STOP & COMMIT:** Agent must stop here and wait for the user to test, stage, and commit the change.

---

#### Step 8: Logging, observability, and changeset artifacts
- [ ] Add logging event model.
- [ ] Copy and paste code below into `src/easycopy/logging/events.py`:

```python
"""Structured log event model."""

from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any


@dataclass(slots=True)
class LogEvent:
    """Represents one structured log event."""

    level: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize event to dictionary."""
        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": self.level,
            "message": self.message,
            "context": self.context,
        }
```

- [ ] Add structured logger and JSON formatter.
- [ ] Copy and paste code below into `src/easycopy/logging/structured.py`:

```python
"""Structured logger configuration."""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """Formats log records as structured JSON lines."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize log record to JSON string."""
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            payload["traceback"] = self.formatException(record.exc_info)

        reserved = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
        }
        extra_context = {
            key: value
            for key, value in record.__dict__.items()
            if key not in reserved
        }
        if extra_context:
            payload["context"] = extra_context

        return json.dumps(payload, default=str)


def configure_structured_logger(logs_dir: Path) -> logging.Logger:
    """Configure package logger and return it."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "easycopy.log"

    logger = logging.getLogger("easycopy")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)

    return logger
```

- [ ] Add logging handlers helper.
- [ ] Copy and paste code below into `src/easycopy/logging/handlers.py`:

```python
"""Log handler helpers for EasyCopy."""

import logging


def add_console_handler(logger: logging.Logger) -> None:
    """Attach a standard stream handler to the logger."""
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
```

- [ ] Add changeset CSV artifact writer.
- [ ] Copy and paste code below into `src/easycopy/change_detection/changesets.py`:

```python
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
```

- [ ] Update API to emit execution decisions and optional changeset artifacts.
- [ ] Copy and paste code below into `src/easycopy/api.py`:

```python
"""Public EasyCopy API facade with observability and optional changesets."""

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
        """Execute workflow with structured logging and optional changeset output."""
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
        validate_inputs(payload)
        validate_target_contract(payload)

        schema_result = compare_schema(
            source=payload["source"],
            target=payload["target"],
            mode=payload["schema_comparison_type"],
        )
        if not schema_result.compatible:
            logger.error("Schema comparison failed", extra={"errors": schema_result.messages})
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
```

##### Step 8 Verification Checklist
- [ ] Log output is JSON-structured with timestamp, severity, message, and context.
- [ ] Exceptions include traceback payloads in logs.
- [ ] `log_changesets=True` writes `adds.csv`, `updates.csv`, `deletes.csv` in default/custom directory.

#### Step 8 STOP & COMMIT
**STOP & COMMIT:** Agent must stop here and wait for the user to test, stage, and commit the change.

---

#### Step 9: Test suite, migration docs, and deprecation strategy
- [ ] Add API tests.
- [ ] Copy and paste code below into `tests/test_api.py`:

```python
"""API-level tests for EasyCopy facade behavior."""

from pathlib import Path

from easycopy import EasyCopy


class _StubLayer:
    def __init__(self) -> None:
        self.kind = "FEATURE_SERVICE"
        self.fields = [
            {"name": "OBJECTID", "type": "INTEGER"},
            {"name": "name", "type": "STRING", "length": 100},
        ]
        self.records = []
        self.manager = self

    def truncate(self) -> None:
        return None

    def edit_features(self, **kwargs):
        return {"ok": True, "kwargs": kwargs}


def test_singleton_is_exposed() -> None:
    """Ensure facade singleton exposes callable API."""
    assert hasattr(EasyCopy, "copy_data")


def test_default_dirs_created(tmp_path: Path, monkeypatch) -> None:
    """Ensure runtime directories are created when provided paths are used."""
    source = _StubLayer()
    target = _StubLayer()

    import easycopy.validation.environment as env

    monkeypatch.setattr(env, "validate_environment", lambda: None)

    logs_dir = tmp_path / "logs"
    changesets_dir = tmp_path / "changesets"

    EasyCopy.copy_data(
        source=source,
        target=target,
        logs_dir=str(logs_dir),
        changesets_dir=str(changesets_dir),
    )

    assert logs_dir.exists()
    assert changesets_dir.exists()
```

- [ ] Add validation tests.
- [ ] Copy and paste code below into `tests/test_validation.py`:

```python
"""Validation pipeline tests."""

import pytest

from easycopy.validation.inputs import validate_inputs
from easycopy.validation.targets import validate_target_contract


def _base_payload() -> dict:
    return {
        "source": object(),
        "target": object(),
        "copy_method": "TRUNCATE_APPEND",
        "schema_comparison_type": "SOFT",
        "id_field": None,
        "batch_size": 200,
    }


def test_invalid_method_raises() -> None:
    """Reject unsupported copy methods."""
    payload = _base_payload()
    payload["copy_method"] = "BAD"

    with pytest.raises(ValueError):
        validate_inputs(payload)


def test_changedetection_requires_id_field() -> None:
    """Require id_field for change detection."""
    payload = _base_payload()
    payload["copy_method"] = "CHANGEDETECTION"

    with pytest.raises(ValueError):
        validate_inputs(payload)


def test_sync_enabled_feature_service_rejected_for_truncate() -> None:
    """Reject truncate against sync-enabled feature service."""
    class _Target:
        kind = "FEATURE_SERVICE"
        sync_enabled = True

    payload = _base_payload()
    payload["target"] = _Target()

    with pytest.raises(ValueError):
        validate_target_contract(payload)
```

- [ ] Add schema tests.
- [ ] Copy and paste code below into `tests/test_schema.py`:

```python
"""Schema comparison tests."""

from easycopy.schema.comparison import compare_schema


class _Layer:
    def __init__(self, fields):
        self.fields = fields


def test_hard_mode_rejects_type_mismatch() -> None:
    """Ensure HARD mode fails on mismatched types."""
    source = _Layer([{"name": "a", "type": "INTEGER"}])
    target = _Layer([{"name": "a", "type": "STRING", "length": 50}])

    result = compare_schema(source, target, "HARD")
    assert result.compatible is False


def test_soft_mode_allows_numeric_to_string() -> None:
    """Ensure SOFT mode allows configured coercion."""
    source = _Layer([{"name": "a", "type": "INTEGER"}])
    target = _Layer([{"name": "a", "type": "STRING", "length": 50}])

    result = compare_schema(source, target, "SOFT")
    assert result.compatible is True
```

- [ ] Add change detection tests.
- [ ] Copy and paste code below into `tests/test_change_detection.py`:

```python
"""Change detection tests."""

from easycopy.change_detection.engine import detect_changes


def test_classifies_add_update_delete() -> None:
    """Classify adds, updates, and deletes from record sets."""
    source = [
        {"id": 1, "value": "a"},
        {"id": 2, "value": "b2"},
    ]
    target = [
        {"id": 2, "value": "b"},
        {"id": 3, "value": "c"},
    ]

    changes = detect_changes(source, target, id_field="id")

    assert len(changes.adds) == 1
    assert len(changes.updates) == 1
    assert len(changes.deletes) == 1


def test_duplicate_ids_raise() -> None:
    """Reject source duplicate ids."""
    source = [{"id": 1}, {"id": 1}]
    target = [{"id": 1}]

    try:
        detect_changes(source, target, id_field="id")
    except ValueError as exc:
        assert "Duplicate id" in str(exc)
    else:
        raise AssertionError("Expected ValueError for duplicate ids")
```

- [ ] Add execution tests.
- [ ] Copy and paste code below into `tests/test_execution.py`:

```python
"""Execution routing and batching tests."""

from easycopy.execution.orchestrator import execute_copy


class _Source:
    records = [{"id": 1}, {"id": 2}]


class _TargetFs:
    kind = "FEATURE_SERVICE"
    records = [{"id": 1}]
    indexed_fields = ["id"]

    def __init__(self):
        self.manager = self
        self.calls = []

    def truncate(self):
        return None

    def edit_features(self, **kwargs):
        self.calls.append(kwargs)
        return {"ok": True}


class _Logger:
    def info(self, *_args, **_kwargs):
        return None


def test_truncate_append_feature_service_path() -> None:
    """Route TRUNCATE_APPEND to feature service executor."""
    payload = {
        "copy_method": "TRUNCATE_APPEND",
        "source": _Source(),
        "target": _TargetFs(),
        "batch_size": 1,
    }
    result = execute_copy(payload=payload, logger=_Logger())
    assert result["ok"] is True


def test_changedetection_feature_service_path() -> None:
    """Route CHANGEDETECTION to feature service apply changes."""
    payload = {
        "copy_method": "CHANGEDETECTION",
        "source": _Source(),
        "target": _TargetFs(),
        "batch_size": 100,
        "id_field": "id",
    }
    result = execute_copy(payload=payload, logger=_Logger())
    assert result["ok"] is True
```

- [ ] Update examples for API usage.
- [ ] Copy and paste code below into `examples/test.py`:

```python
"""Minimal local API smoke test for EasyCopy."""

from easycopy import EasyCopy


class Layer:
    """Simple fake layer for local smoke testing."""

    def __init__(self, records):
        self.kind = "FEATURE_SERVICE"
        self.records = records
        self.fields = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "STRING", "length": 100},
        ]
        self.manager = self

    def truncate(self):
        """No-op truncate for fake layer."""
        return None

    def edit_features(self, **kwargs):
        """No-op edit for fake layer."""
        return {"ok": True, "kwargs": kwargs}


def main() -> None:
    """Run a minimal local smoke test."""
    source = Layer(records=[{"id": 1, "name": "Alice"}])
    target = Layer(records=[])

    result = EasyCopy.copy_data(
        source=source,
        target=target,
        copy_method="TRUNCATE_APPEND",
        schema_comparison_type="SOFT",
        log_changesets=False,
    )
    print(result)


if __name__ == "__main__":
    main()
```

- [ ] Update product requirements traceability notes.
- [ ] Copy and paste code below into `easycopy_product_requirements.md` (append this section at end):

```markdown
## Refactor Traceability Notes

### Requirement-to-Test Mapping

- FR008/FR009 (validation and auth prerequisites): `tests/test_validation.py`
- DR003/DR004 (schema and change detection modularity):
  - `tests/test_schema.py`
  - `tests/test_change_detection.py`
- Execution workflows (`TRUNCATE_APPEND`, `CHANGEDETECTION`): `tests/test_execution.py`
- Public API and singleton behavior: `tests/test_api.py`

### Out-of-Scope Confirmations

- No backward compatibility shim for legacy interfaces.
- No CLI implementation included in this refactor.
- No automated PyPI publish workflow in this PR.
```

- [ ] Update README to include migration notes.
- [ ] Copy and paste code below into `README.md` (append this section at end):

```markdown
## Migration Notes (Major Refactor)

This release introduces an API-first package architecture.

### New usage

```python
from easycopy import EasyCopy

EasyCopy.copy_data(source=..., target=..., copy_method="TRUNCATE_APPEND")
```

### Removed scope

- Legacy compatibility wrappers are not provided.
- CLI commands are not part of this release.
```

##### Step 9 Verification Checklist
- [ ] `pytest -q` passes for `test_api`, `test_validation`, `test_schema`, `test_change_detection`, and `test_execution`.
- [ ] `python examples/test.py` runs a smoke test with fake layer objects.
- [ ] README and requirements traceability notes reflect API-only refactor scope.

#### Step 9 STOP & COMMIT
**STOP & COMMIT:** Agent must stop here and wait for the user to test, stage, and commit the change.

---

## Build, Test, and Validation Commands

Run these commands at each step boundary before committing:

```powershell
python -m pip install --upgrade pip
pip install -e .[dev]
pytest -q
python -m build
python examples/test.py
```

For a clean-environment install verification:

```powershell
python -m venv .venv-clean
.venv-clean\Scripts\activate
pip install dist\easycopy-0.1.0-py3-none-any.whl
python -c "from easycopy import EasyCopy; print(EasyCopy)"
```
