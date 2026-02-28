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
