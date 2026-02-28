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
