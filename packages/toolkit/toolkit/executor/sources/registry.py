"""Find the provided sources: every class of ``backends/`` registers itself."""

import importlib
import pkgutil

from . import backends
from .base import SourceBase


def load_sources() -> type[SourceBase]:
    """Import every module of ``backends/`` and return the registry.

    Use it like a dict: ``load_sources()["postgres"]`` is the source class.
    """
    for module in pkgutil.iter_modules(backends.__path__):
        importlib.import_module(f"{backends.__name__}.{module.name}")

    return SourceBase
