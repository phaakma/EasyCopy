"""Execution routing and batching tests."""

import sys
from types import SimpleNamespace

from easycopy.execution.feature_class import FeatureClassExecutor
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


class _TargetFc:
    kind = "FEATURE_CLASS"
    records = [{"id": 1}]
    indexed_fields = ["id"]


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


def test_changedetection_feature_class_path(monkeypatch) -> None:
    """Route CHANGEDETECTION to feature class apply changes."""
    calls: list[dict[str, object]] = []

    def _apply_changes(self, target, changes, id_field):
        calls.append(
            {
                "target": target,
                "changes": changes,
                "id_field": id_field,
            }
        )
        return {
            "ok": True,
            "method": "CHANGEDETECTION",
            "adds": len(changes.adds),
            "updates": len(changes.updates),
            "deletes": len(changes.deletes),
        }

    monkeypatch.setattr(FeatureClassExecutor, "apply_changes", _apply_changes)

    payload = {
        "copy_method": "CHANGEDETECTION",
        "source": _Source(),
        "target": _TargetFc(),
        "batch_size": 100,
        "id_field": "id",
    }

    result = execute_copy(payload=payload, logger=_Logger())

    assert result["ok"] is True
    assert len(calls) == 1
    assert calls[0]["target"] is payload["target"]
    assert calls[0]["id_field"] == "id"


def test_changedetection_reads_arcpy_rows_when_records_missing(monkeypatch) -> None:
    """Load rows from ArcPy datasets and detect add records correctly."""

    class _Field:
        def __init__(self, name: str, field_type: str) -> None:
            self.name = name
            self.type = field_type

    class _SearchCursor:
        def __init__(self, dataset: str, fields, where_clause=None) -> None:
            self._rows = []
            del where_clause

            source_data = {
                "source_layer": [
                    {"id": 1, "name": "A"},
                    {"id": 2, "name": "B"},
                ],
                "target_layer": [
                    {"id": 1, "name": "A"},
                ],
            }

            for row in source_data[dataset]:
                self._rows.append(tuple(row.get(field) for field in fields))

        def __enter__(self):
            return iter(self._rows)

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

    class _Describe:
        dataType = "Table"

    arcpy_stub = SimpleNamespace(
        ListFields=lambda _dataset: [
            _Field("id", "Integer"),
            _Field("name", "String"),
        ],
        Describe=lambda _dataset: _Describe(),
        da=SimpleNamespace(SearchCursor=_SearchCursor),
    )
    monkeypatch.setitem(sys.modules, "arcpy", arcpy_stub)

    calls: list[dict[str, object]] = []

    def _apply_changes(self, target, changes, id_field):
        calls.append(
            {
                "target": target,
                "id_field": id_field,
                "adds": len(changes.adds),
                "updates": len(changes.updates),
                "deletes": len(changes.deletes),
            }
        )
        return {
            "ok": True,
            "method": "CHANGEDETECTION",
            "adds": len(changes.adds),
            "updates": len(changes.updates),
            "deletes": len(changes.deletes),
        }

    monkeypatch.setattr(FeatureClassExecutor, "apply_changes", _apply_changes)

    payload = {
        "copy_method": "CHANGEDETECTION",
        "source": _ResultLike("source_layer"),
        "target": _ResultLike("target_layer"),
        "batch_size": 100,
        "id_field": "id",
    }

    result = execute_copy(payload=payload, logger=_Logger())

    assert result["ok"] is True
    assert result["adds"] == 1
    assert result["updates"] == 0
    assert result["deletes"] == 0
    assert len(calls) == 1
    assert calls[0]["id_field"] == "id"
    assert calls[0]["adds"] == 1


class _ResultLike:
    def __init__(self, value: str) -> None:
        self._value = value

    def getOutput(self, index: int) -> str:
        if index != 0:
            raise IndexError(index)
        return self._value


def test_feature_class_resolves_result_like_inputs(monkeypatch) -> None:
    """Resolve ArcPy Result-like inputs before truncate/append calls."""

    class _Mgmt:
        def __init__(self) -> None:
            self.truncate_input = None
            self.append_kwargs = None

        def TruncateTable(self, dataset: str) -> None:
            self.truncate_input = dataset

        def Append(self, **kwargs) -> None:
            self.append_kwargs = kwargs

    management = _Mgmt()
    monkeypatch.setitem(sys.modules, "arcpy", SimpleNamespace(management=management))

    executor = FeatureClassExecutor()
    result = executor.truncate_append(
        source=_ResultLike("source_layer"),
        target=_ResultLike("target_layer"),
    )

    assert result["ok"] is True
    assert management.truncate_input == "target_layer"
    assert management.append_kwargs == {
        "inputs": "source_layer",
        "target": "target_layer",
        "schema_type": "NO_TEST",
    }


def test_feature_class_falls_back_when_truncate_disallowed(monkeypatch) -> None:
    """Fallback to DeleteRows when truncate is blocked by edit session."""

    class _Mgmt:
        def __init__(self) -> None:
            self.truncate_called_with = None
            self.delete_called_with = None
            self.append_kwargs = None

        def TruncateTable(self, dataset: str) -> None:
            self.truncate_called_with = dataset
            raise RuntimeError("ERROR 160592: Truncation not allowed while editing.")

        def DeleteRows(self, dataset: str) -> None:
            self.delete_called_with = dataset

        def Append(self, **kwargs) -> None:
            self.append_kwargs = kwargs

    management = _Mgmt()
    arcpy_stub = SimpleNamespace(
        management=management,
        GetMessages=lambda _severity: "ERROR 160592: Truncation not allowed while editing.",
    )
    monkeypatch.setitem(sys.modules, "arcpy", arcpy_stub)

    executor = FeatureClassExecutor()
    result = executor.truncate_append(
        source=_ResultLike("source_layer"),
        target=_ResultLike("target_layer"),
    )

    assert result["ok"] is True
    assert management.truncate_called_with == "target_layer"
    assert management.delete_called_with == "target_layer"
    assert management.append_kwargs == {
        "inputs": "source_layer",
        "target": "target_layer",
        "schema_type": "NO_TEST",
    }
