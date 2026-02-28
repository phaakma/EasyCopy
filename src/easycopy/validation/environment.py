"""Runtime environment validation checks.

Validates presence of `arcpy` and `arcgis` and enforces a minimum
`arcgis` version > 2.4.0.
"""

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version


def _parse_version(raw: str) -> tuple[int, ...]:
    """Convert a semantic version string to comparable integer tuple."""
    cleaned = raw.split("+")[0].split("-")[0]
    parts = cleaned.split(".")
    numeric: list[int] = []
    for part in parts:
        digits = "".join(ch for ch in part if ch.isdigit())
        numeric.append(int(digits or "0"))
    return tuple(numeric)


def validate_environment() -> None:
    """Validate required packages and minimum versions.

    Raises `RuntimeError` with a clear message when requirements are not met.
    """
    missing: list[str] = []

    try:
        import_module("arcpy")
    except Exception:
        missing.append("arcpy")

    try:
        import_module("arcgis")
    except Exception:
        missing.append("arcgis")

    if missing:
        joined = ", ".join(sorted(missing))
        raise RuntimeError(f"Missing required runtime dependency/dependencies: {joined}")

    try:
        installed = version("arcgis")
    except PackageNotFoundError as exc:
        raise RuntimeError("arcgis package metadata not available") from exc

    if _parse_version(installed) <= (2, 4, 0):
        raise RuntimeError("arcgis must be greater than 2.4.0")
