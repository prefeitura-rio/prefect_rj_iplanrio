"""Every constant of the toolkit."""

from collections.abc import Mapping
from typing import Final

# Telemetry
PREFECT_LOGGER: Final = "prefect"
"""The logger whose tree carries every Prefect log and every toolkit log."""

OTEL_HANDLER_NAME: Final = "toolkit-otel"
"""The name of the OpenTelemetry handler, so it is added once."""

SCOPE: Final = "toolkit"
"""The name of the tracer and of the meter of the toolkit."""

QUERY_SPAN: Final = "toolkit.query"
EXECUTE_SPAN: Final = "toolkit.execute"
UPLOAD_SPAN: Final = "toolkit.upload"
DELETE_SPAN: Final = "toolkit.delete"

TEMPLATE_ATTRIBUTE: Final = "toolkit.template"
"""The span attribute with the template that ran."""

# dbt
COMMAND_WORDS: Final[Mapping[str, tuple[str, ...]]] = {
    "source freshness": ("source", "freshness")
}
"""The commands whose name is more than one word."""

SELECTABLE: Final = frozenset(
    {"build", "run", "test", "source freshness", "seed", "snapshot"}
)
"""The commands that take a target and a selection."""
