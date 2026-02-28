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
