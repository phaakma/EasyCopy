# EasyCopy Major Refactor

**Branch:** `feature/easycopy-major-refactor`
**Description:** Refactor EasyCopy into a Python API-first package with modernized internals, explicit runtime constraints, and local build tooling setup for release readiness.

## Goal
Reorganize EasyCopy into a package-first, modular Python API architecture with explicit validation, schema, and change-detection engines that satisfy functional and developer requirements. Deliver the `EasyCopy.copy_data(...)` API entrypoint with runtime targets of Python >3.11 and `arcgis` >2.4.0, without backward-compatibility shims or CLI scope.

## Implementation Steps

### Step 1: Package foundation and build scaffolding
**Files:** pyproject.toml, README.md, src/easycopy/__init__.py, src/easycopy/version.py, examples/run_python.bat
**What:** Create installable package structure for `pip install easycopy`, define project metadata/dependencies, and expose canonical import path. Configure build tooling for local wheel/sdist creation only (no automated PyPI publishing workflow in this PR). In `pyproject.toml`, set runtime constraints for Python >3.11 and `arcgis` >2.4.0.
**Testing:** Build wheel/sdist locally, install in clean environment, run import smoke test (`import easycopy`) and a minimal API call with mocked dependencies.

### Step 2: Public API redesign with singleton entry point
**Files:** src/easycopy/__init__.py, src/easycopy/api.py, src/easycopy/config.py, README.md
**What:** Implement singleton-oriented facade so users call `EasyCopy.copy_data(...)` and defaults create `./logs` + `./changesets` when not provided. Introduce parameter model matching requirements (`copy_method`, `schema_comparison_type`, `log_changesets`, etc.) for Python API usage only.
**Testing:** Unit tests for singleton behavior, default folder creation, and API parameter validation/normalization.

### Step 3: Modular namespace extraction (DR002-DR004)
**Files:** src/easycopy/validation/__init__.py, src/easycopy/schema/__init__.py, src/easycopy/change_detection/__init__.py, src/easycopy/execution/__init__.py, src/easycopy/logging/__init__.py, src/easycopy/api.py
**What:** Split monolithic logic from current `src/EasyCopy.py` into focused modules by concern (validation, schema, change detection, execution, logging) and keep orchestration in API layer. Ensure change detection and schema comparison logic are isolated namespaces to satisfy DR003/DR004.
**Testing:** Unit tests per namespace with mocked ArcGIS adapters; import and cycle checks to verify no circular dependencies.

### Step 4: Pre-execution validation pipeline
**Files:** src/easycopy/validation/environment.py, src/easycopy/validation/inputs.py, src/easycopy/validation/targets.py, src/easycopy/api.py
**What:** Build validation pipeline that runs before execution: dependency/license checks (`arcpy`, `arcgis`), source/target type contracts, auth checks, parameter checks, and method/id-field rules for change detection. Add explicit checks for feature-service sync + truncate and versioned feature class + truncate constraints.
**Testing:** Unit tests for each validation rule plus failure-path tests asserting actionable errors and no downstream execution.

### Step 5: Schema comparison engine (soft and hard)
**Files:** src/easycopy/schema/comparison.py, src/easycopy/schema/models.py, src/easycopy/schema/rules.py, src/easycopy/api.py
**What:** Implement configurable `SOFT` and `HARD` schema comparison modes with ArcGIS type equivalency rules and permitted coercions (for example int→string and shorter text→longer text). Return structured schema diff results used by orchestrator for fail-fast behavior and transparent logs.
**Testing:** Rule-driven unit tests for compatibility/equivalency cases and mismatch reporting snapshots.

### Step 6: Change detection engine with geometry support
**Files:** src/easycopy/change_detection/engine.py, src/easycopy/change_detection/geometry.py, src/easycopy/change_detection/sdf_compare.py, src/easycopy/change_detection/fallback_compare.py, src/easycopy/models.py
**What:** Implement `CHANGEDETECTION` flow producing adds/updates/deletes with required `idField`, indexed-field warning, and geometry-aware comparisons including Z/curve handling. Prefer Spatially Enabled DataFrame comparison path where viable with deterministic fallback when SDF cannot be used.
**Testing:** Unit tests for adds/updates/deletes classification, duplicate id handling, geometry equality edge cases, and SDF-vs-fallback parity tests.

### Step 7: Execution engine for TRUNCATE_APPEND and CHANGEDETECTION
**Files:** src/easycopy/execution/feature_class.py, src/easycopy/execution/feature_service.py, src/easycopy/execution/batching.py, src/easycopy/execution/orchestrator.py
**What:** Implement workflow selection and execution paths for `TRUNCATE_APPEND` and `CHANGEDETECTION`, including ArcPy Append for feature classes and REST batch edits for feature services. Add configurable batch controls and safeguards to minimize partial-failure risk.
**Testing:** Adapter-level tests using mocks/fakes for ArcPy and ArcGIS REST edit calls, including retry/error-handling behavior and batch chunking boundaries.

### Step 8: Logging, observability, and changeset artifacts
**Files:** src/easycopy/logging/structured.py, src/easycopy/logging/handlers.py, src/easycopy/logging/events.py, src/easycopy/change_detection/changesets.py, src/easycopy/api.py
**What:** Standardize structured logging with timestamp/severity/context, traceback capture, and workflow decision logs for execution transparency. Add optional CSV changeset outputs (`adds`, `updates`, `deletes`) controlled by `log_changesets=True`.
**Testing:** Tests for log payload schema, error trace emission, and changeset CSV generation in default/custom directories.

### Step 9: Test suite, migration docs, and deprecation strategy
**Files:** tests/test_api.py, tests/test_validation.py, tests/test_schema.py, tests/test_change_detection.py, tests/test_execution.py, README.md, easycopy_product_requirements.md (traceability notes), examples/test.py
**What:** Add comprehensive tests for critical paths and edge cases; document Python API usage and upgrade notes for the refactor without compatibility/deprecation shims. Add requirement-to-test traceability notes to make PR review and release readiness explicit.
**Testing:** Run full test suite in CI matrix and perform one end-to-end dry run in an ArcGIS-enabled environment.

## Out of Scope in this PR
- Backward compatibility with legacy interfaces (including `refreshData` compatibility layers).
- CLI implementation and CLI documentation/tasks.
- Automated publishing workflow to PyPI (local packaging/build setup only).
