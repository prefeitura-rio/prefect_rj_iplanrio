"""Every constant of the toolkit."""

from collections.abc import Mapping
from re import compile as compile_pattern
from typing import Final, LiteralString

from pyarrow import binary as arrow_binary
from pyarrow import float32
from pyarrow import float64 as arrow_float64
from pyarrow import string as arrow_string
from pyarrow import timestamp as arrow_timestamp

PREFECT_LOGGER: Final = "prefect"
"""The logger whose tree carries every Prefect log and every toolkit log."""

OTEL_HANDLER_NAME: Final = "toolkit-otel"
"""The name of the OpenTelemetry handler, so it is added once."""

SCOPE: Final = "toolkit"
"""The name of the tracer and of the meter of the toolkit."""

RUN_TAGS: Final = ("environment", "severity", "code_owner")
"""The tags of a deployment that every log, span and metric of its runs carries."""

GCS_SCHEME: Final = "gcs://"
GS_SCHEME: Final = "gs://"

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

GITHUB_TOKEN_PLACEHOLDER: Final = "GITHUB_TOKEN"  # noqa: S105  # the text of a placeholder, not a token
"""The text of a repository URL that the token of GitHub replaces."""

SELECTABLE: Final = frozenset(
    {"build", "run", "test", "source freshness", "seed", "snapshot"}
)
"""The commands that take a target and a selection."""

DELTA_CASTS: Final[Mapping[str, LiteralString]] = {
    "TIME": "VARCHAR",
    "TIME WITH TIME ZONE": "VARCHAR",
    "UUID": "VARCHAR",
    "INTERVAL": "VARCHAR",
    "UTINYINT": "SMALLINT",
    "USMALLINT": "INTEGER",
    "UINTEGER": "BIGINT",
    "UBIGINT": "DECIMAL(20, 0)",
}
"""The DuckDB types that Delta cannot hold, with the type a dump casts each to."""

TEXT_TYPE: Final = compile_pattern(r"VARCHAR|MSSQL_N?(VAR)?CHAR\(\d+\)")
"""The DuckDB text types: `VARCHAR` and the sized ones of the SQL Server extension."""

TIMEZONE: Final = "America/Sao_Paulo"
"""The time zone of the functions `now_sao_paulo` and `to_sao_paulo`."""

TIMESTAMP_FORMATS: Final = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%.f",
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

BATCH_COLUMN: Final = "batch"
"""The column that holds the key of the chunk, which partitions a Delta table."""

EXTRACTED_AT_COLUMN: Final = "_prefect_extracted_at"
"""The column that holds when the chunk of a row was written, if a dump asks for it."""

ALL_KEY: Final = "all"
"""The key of the one chunk of a table that a dump does not cut."""

DATE_KEY: Final = r"\d{4}-\d{2}-\d{2}"
"""The pattern of the key of a chunk that a date column cuts."""

NUMBER_KEY: Final = r"\d+"
"""The pattern of the key of a chunk that a number column cuts."""

SOURCE_ALIAS: Final = "s"
"""The alias of the new rows in the predicates of a merge."""

TARGET_ALIAS: Final = "t"
"""The alias of the rows of the table in the predicates of a merge."""

COUNT_COLUMN: Final = "count_all"
"""The column that a group by of `pyarrow` names for the size of each group."""

COMMIT_ATTEMPTS: Final = 5
"""How many times a Delta write tries again after a commit conflict."""

VACUUM_RETENTION_HOURS: Final = 168
"""How long replaced Delta files stay, for the queries that still read them."""

FILTER_FORBIDDEN: Final = (";", "--", "/*")
"""The marks that a filter may not hold, so that it stays one predicate."""

ORACLE_BATCH_ROWS: Final = 50_000
"""The rows of one fetch from Oracle, so a chunk never sits whole in memory."""

MONGO_BATCH_ROWS: Final = 50_000
"""The documents of one page of MongoDB, so a chunk never sits whole in memory."""

ORACLE_KEEPALIVE_MINUTES: Final = 2
"""The minutes between the probes that stop a firewall from dropping a connection."""

ORACLE_SNAPSHOT_FALLBACK_CODES: Final = frozenset({942, 1031, 1466, 41900})
"""The Oracle errors of a read `AS OF SCN` that has no answer.

The user may not flash the table back, or its definition just changed. The read goes
on without the number.
"""

ORACLE_EXPRESSIONS: Final = {
    "DB_TYPE_BINARY_FLOAT": ("plain", float32()),
    "DB_TYPE_BINARY_DOUBLE": ("plain", arrow_float64()),
    "DB_TYPE_VARCHAR": ("plain", arrow_string()),
    "DB_TYPE_CHAR": ("plain", arrow_string()),
    "DB_TYPE_NVARCHAR": ("plain", arrow_string()),
    "DB_TYPE_NCHAR": ("plain", arrow_string()),
    "DB_TYPE_CLOB": ("plain", arrow_string()),
    "DB_TYPE_NCLOB": ("plain", arrow_string()),
    "DB_TYPE_LONG": ("plain", arrow_string()),
    "DB_TYPE_RAW": ("plain", arrow_binary()),
    "DB_TYPE_BLOB": ("plain", arrow_binary()),
    "DB_TYPE_LONG_RAW": ("plain", arrow_binary()),
    "DB_TYPE_DATE": ("plain", arrow_timestamp("us")),
    "DB_TYPE_TIMESTAMP": ("plain", arrow_timestamp("us")),
    "DB_TYPE_TIMESTAMP_TZ": ("utc", arrow_timestamp("us")),
    "DB_TYPE_TIMESTAMP_LTZ": ("utc", arrow_timestamp("us")),
    "DB_TYPE_INTERVAL_YM": ("text", arrow_string()),
    "DB_TYPE_INTERVAL_DS": ("text", arrow_string()),
    "DB_TYPE_ROWID": ("rowid", arrow_string()),
    "DB_TYPE_UROWID": ("rowid", arrow_string()),
    "DB_TYPE_XMLTYPE": ("xml", arrow_string()),
}
"""The Oracle types of a fixed form, with their expression kind and Arrow type."""
