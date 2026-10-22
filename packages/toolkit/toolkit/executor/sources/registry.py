"""Find the provided sources: every class of `backends/` registers itself."""

from collections.abc import Callable
from importlib import import_module
from pkgutil import iter_modules

from ..constants import OPTIONAL_BACKENDS
from . import backends
from .base import Registered


def get_source(name: str) -> Callable[..., Registered]:
    """Return the registered source class for a name, to build with its options.

    Ignore case. Raise `ValueError` for a source whose extra is not installed, and
    `KeyError` if the name is unknown.

    ```python
    get_source("Postgres")  # PostgresSource
    get_source("postgres")(conn=conn, origin=Table("t"))
    ```
    """
    sources = load_sources()
    key = name.lower()

    if key in OPTIONAL_BACKENDS and key not in sources:
        raise ValueError(f"The {key} source needs the extra: install toolkit[{key}]")

    return sources[key]


def load_sources() -> dict[str, type[Registered]]:
    """Import every module of `backends/` and return the sources by name.

    A backend that needs an extra is left out when the extra is missing: only a missing
    module of that extra is forgiven, and any other import error raises.

    ```python
    load_sources()["postgres"]  # PostgresSource
    ```
    """
    for module in iter_modules(backends.__path__):
        try:
            import_module(f"{backends.__name__}.{module.name}")
        except ModuleNotFoundError as error:
            if error.name not in OPTIONAL_BACKENDS.get(module.name, ()):
                raise

    return Registered.sources
