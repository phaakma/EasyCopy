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
