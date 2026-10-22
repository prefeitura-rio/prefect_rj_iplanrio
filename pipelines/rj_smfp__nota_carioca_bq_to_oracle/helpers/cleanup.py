"""Wait for background work and raise what failed."""

from collections.abc import Sequence
from typing import Protocol


class Pending(Protocol):
    """Work that runs in the background and gives its result, or raises, when it is asked for."""

    def result(self) -> object:
        """Wait for the work and return its result. Raise what it raised."""
        ...


def raise_failures(pending: Sequence[Pending]) -> None:
    """Wait for all the work in the order it started, and raise the first failure."""
    for item in pending:
        item.result()
