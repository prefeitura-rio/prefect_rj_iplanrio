"""Every constant of the toolkit."""

from collections.abc import Mapping
from typing import Final

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

COMMAND_WORDS: Final[Mapping[str, tuple[str, ...]]] = {
    "source freshness": ("source", "freshness")
}
"""The commands whose name is more than one word."""

SELECTABLE: Final = frozenset(
    {"build", "run", "test", "source freshness", "seed", "snapshot"}
)
"""The commands that take a target and a selection."""

TIMEZONE: Final = "America/Sao_Paulo"
"""The time zone of the functions `now_sao_paulo` and `to_sao_paulo`."""

TIMESTAMP_FORMATS: Final = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y - %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M",
    "%Y%m%d%H%M",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%Y%m%d",
)
"""The timestamp formats of the pipelines, tried in this order by `parse_timestamp`."""

AREA_CODES: Final = frozenset(
    [
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "21",
        "22",
        "24",
        "27",
        "28",
        "31",
        "32",
        "33",
        "34",
        "35",
        "37",
        "38",
        "41",
        "42",
        "43",
        "44",
        "45",
        "46",
        "47",
        "48",
        "49",
        "51",
        "53",
        "54",
        "55",
        "61",
        "62",
        "63",
        "64",
        "65",
        "66",
        "67",
        "68",
        "69",
        "71",
        "73",
        "74",
        "75",
        "77",
        "79",
        "81",
        "82",
        "83",
        "84",
        "85",
        "86",
        "87",
        "88",
        "89",
        "91",
        "92",
        "93",
        "94",
        "95",
        "96",
        "97",
        "98",
        "99",
    ]
)
"""The area codes (DDD) that ANATEL assigns, which a phone must start with."""
