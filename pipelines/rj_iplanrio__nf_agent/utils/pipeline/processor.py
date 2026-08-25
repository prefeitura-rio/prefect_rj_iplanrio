"""
POC Pipeline Processor - Processes database rows using core NF pipeline with caching.
Integrates GCS downloading, SQLite caching, and core NF processing modules.

The ``POCProcessor`` class is split into cohesive mixins to keep this module
manageable while preserving the public API (``POCProcessor`` and ``ExecutionMode``).
"""

import logging
import sys

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Capture all levels, filter in handler

# Create handler that writes to stdout (visible in Prefect Cloud logs)
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setLevel(logging.INFO)  # Default to INFO level

# Format: [timestamp] [level] message
formatter = logging.Formatter("%(message)s")  # Keep it clean for now
_stream_handler.setFormatter(formatter)

logger.addHandler(_stream_handler)

from .cache import POCProcessorCacheMixin
from .database import POCProcessorDatabaseMixin
from .metadata import POCProcessorMetadataMixin
from .modes import ExecutionMode  # noqa: F401  (re-exported; public API)
from .process import POCProcessorProcessMixin
from .setup import POCProcessorSetupMixin


class POCProcessor(
    POCProcessorSetupMixin,
    POCProcessorCacheMixin,
    POCProcessorProcessMixin,
    POCProcessorMetadataMixin,
    POCProcessorDatabaseMixin,
):
    """Processes database rows using the core NF pipeline with caching."""
