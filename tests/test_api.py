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
