"""Types of rj_crm__salesforce_datacloud."""

from dataclasses import dataclass, field
from datetime import date
from typing import Literal, NotRequired, TypedDict

from pydantic import JsonValue

type Row = dict[str, JsonValue]
type Source = Literal["data_cloud", "crm_rest"]
type Mode = Literal["hour", "day"]


@dataclass(frozen=True)
class Feed:
    """A table to extract from Salesforce, with how to read and load it.

    `name` is the BigQuery table, and its staging is `<name>_staging`. `query` is a file
    of `queries/`. `date_columns` (in snake case) become UTC timestamps. `primary_key`
    is the key of the MERGE, and `order_by` orders the pages of a Data Cloud read. A
    `critical` feed that fails stops the run, and any other one warns Discord and lets
    the run go on. With `utc_window` the source writes real UTC and not the wall time of
    São Paulo with a `Z`, so the window is converted. `step_output_only` keeps
    `output_value_text` only for the `ACTION_STEP` rows.
    """

    name: str
    group: str
    query: str
    source: Source = "data_cloud"
    date_columns: tuple[str, ...] = ()
    primary_key: str = "id"
    order_by: str = "ssot__Id__c"
    critical: bool = False
    utc_window: bool = False
    step_output_only: bool = False

    @property
    def staging(self) -> str:
        """Return the name of the staging table."""
        return f"{self.name}_staging"


@dataclass(frozen=True)
class Span:
    """A window to extract, as the text that the queries use, and the day that stamps its rows."""

    start: str
    end: str
    partition_date: date


class Session(TypedDict):
    """A login to Salesforce, that Data Cloud and the CRM REST API both use."""

    access_token: str
    instance_url: str
    dataspace: str


class Window(TypedDict, total=False):
    """What a run extracts.

    `mode` is `hour` (the last hour, every 15 minutes) or `day` (a whole day, once a
    day, for `partition_date` or yesterday). With `reprocess_from` the run redoes each
    day from it to `reprocess_to` (the same day without it) and `mode` does not count.
    """

    mode: Mode
    partition_date: NotRequired[date]
    reprocess_from: NotRequired[date]
    reprocess_to: NotRequired[date]


@dataclass
class Totals:
    """What a run did: the rows that each feed loaded, by group, and the feeds that failed."""

    rows: dict[str, dict[str, int]] = field(default_factory=dict)
    failed: list[str] = field(default_factory=list)
