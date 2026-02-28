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
from easycopy.schema.comparison import compare_schema
class Dummy:
    def __init__(self, fields):
        self.fields = fields

s = Dummy([{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING","length":50}])
t = Dummy([{"name":"id","type":"SMALLINTEGER"},{"name":"name","type":"STRING","length":100}])
print(compare_schema(s, t, mode='SOFT'))