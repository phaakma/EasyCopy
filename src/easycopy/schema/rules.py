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
