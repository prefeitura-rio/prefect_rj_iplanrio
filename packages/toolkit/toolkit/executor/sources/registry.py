"""Find the provided sources: every class of `backends/` registers itself."""

from importlib import import_module
from pkgutil import iter_modules

from . import backends
from .base import SourceBase


def get_source(name: str) -> type[SourceBase]:
    """Return the registered source class for a name.

    Ignore case. Raise `KeyError` if the name is unknown.

    ```python
    get_source("Postgres")  # PostgresSource
    ```
    """
    return load_sources()[name]


def load_sources() -> type[SourceBase]:
    """Import every module of `backends/` and return the registry.

    It works like a dict.

    ```python
    load_sources()["postgres"]  # PostgresSource
    ```
    """
    for module in iter_modules(backends.__path__):
        import_module(f"{backends.__name__}.{module.name}")

    return SourceBase
