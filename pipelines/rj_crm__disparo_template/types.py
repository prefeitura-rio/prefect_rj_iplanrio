"""Types of rj_crm__disparo_template."""

from dataclasses import dataclass
from typing import Literal

from pydantic import JsonValue


@dataclass(frozen=True)
class Campaign:
    """The campaign that a run sends, and where it runs.

    `name` is the campaign in the control sheet and in Salesforce. With `test_mode`,
    nothing goes to the log table and the waits are short.
    """

    name: str = "template"
    environment: Literal["staging", "production"] = "staging"
    test_mode: bool = False


@dataclass(frozen=True)
class Audience:
    """Who gets the message: the query, the filters and the retries.

    The query is `query` or the file `query_file` of the pipeline. With `replacements`
    its placeholders are filled, through `processor` if there is one. `dispatched` is
    `cpf` or `telefone`: who got a message today from any campaign is dropped. With
    `interval_days`, who got this campaign in that many days is dropped too. A run with
    `max_retries` tries again, with the next phone of `others`, for the CPFs that
    failed.
    """

    query: str | None = None
    query_file: str | None = None
    processor: str | None = None
    replacements: dict[str, str | int] | None = None
    dispatched: Literal["cpf", "telefone"] | None = "cpf"
    interval_days: int = 0
    drop_duplicate_phones: bool = False
    drop_duplicate_cpfs: bool = True
    drop_failed_phones: bool = False
    enricher: str | None = None
    enricher_params: dict[str, JsonValue] | None = None
    max_retries: int = 0


@dataclass(frozen=True)
class Delivery:
    """How the file goes out and what is watched after it.

    The file is `data_extension` plus the time, with `columns` of the query and
    `separator`, in `remote_path` of the SFTP. `batch_size` cuts the frame in files and
    `batch_sleep_minutes` is the pause between them. `sleep_minutes` is the pause before
    each file, to let a person stop a wrong run. A share of the phones,
    `whitelist_percentage`, goes to the whitelist. After the file, the flow waits
    `materialization_minutes` and then, with `monitor`, it builds the status model and
    checks for the status each `check_interval_minutes`, up to `max_wait_minutes`, and
    alerts if there is none. With `materialize` and no `monitor`, it builds the model
    once.
    """

    data_extension: str | None = None
    columns: list[str] | None = None
    separator: str = ";"
    remote_path: str = "/Import/"
    batch_size: int | None = None
    batch_sleep_minutes: int = 5
    sleep_minutes: int = 5
    whitelist_percentage: int = 0
    whitelist_replace: bool = False
    whitelist_force: bool = False
    monitor: bool = True
    materialize: bool = True
    materialization_minutes: int = 20
    check_interval_minutes: int = 5
    max_wait_minutes: int = 40


@dataclass(frozen=True)
class Csv:
    """A file for the SFTP: its name, its text and the time of the dispatch."""

    name: str
    text: str
    dispatch_date: str


@dataclass(frozen=True)
class Report:
    """What a try sent: the campaign, the dispatch time, the people and the batches."""

    campaign: str
    dispatch_date: str
    total: int
    batches: int
    test_mode: bool


@dataclass(frozen=True)
class Delivered:
    """What a try put on the SFTP: its log rows, last file time and file count."""

    rows: list[dict[str, JsonValue]]
    last_date: str
    batches: int
