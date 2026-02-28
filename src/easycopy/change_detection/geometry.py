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
