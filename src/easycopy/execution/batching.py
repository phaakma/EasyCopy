"""Batching helpers for execution operations."""

from collections.abc import Iterable
from typing import TypeVar

T = TypeVar("T")


def chunked(items: list[T], batch_size: int) -> Iterable[list[T]]:
    """Yield fixed-size chunks from a list."""
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    for index in range(0, len(items), batch_size):
        yield items[index : index + batch_size]
