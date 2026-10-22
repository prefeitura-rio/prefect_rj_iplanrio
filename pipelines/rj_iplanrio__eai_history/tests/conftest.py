from collections.abc import Callable
from itertools import count

import pytest


@pytest.fixture
def new_id() -> Callable[[], str]:
    """Return a function that gives `id-1`, `id-2` and so on, so the ids of a test are known."""
    counter = count(1)

    return lambda: f"id-{next(counter)}"
