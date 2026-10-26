"""The rows of Salesforce, ready for BigQuery."""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from json import dumps
from re import sub

from pydantic import JsonValue

from pipelines.rj_crm__salesforce_datacloud.constants import STEP_TYPE
from pipelines.rj_crm__salesforce_datacloud.types import Feed, Row


def to_snake_case(name: str) -> str:
    """Return a CamelCase or PascalCase name in snake case.

    ```python
    to_snake_case("StartTimestamp")  # "start_timestamp"
    ```
    """
    spaced = sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)

    return sub(r"([a-z\d])([A-Z])", r"\1_\2", spaced).lower()


def clean_data_cloud_name(name: str) -> str:
    """Return a Data Cloud name without the `ssot__` prefix and the `__c`, `__dlm` or `__dll` suffix, in snake case.

    ```python
    clean_data_cloud_name("ssot__StartTime__c")  # "start_time"
    ```
    """
    return to_snake_case(sub(r"(__c|__dlm|__dll)$", "", sub(r"^ssot__", "", name)))


def parse_utc(value: JsonValue) -> str | None:
    """Return a date or time as an ISO text in UTC, or nothing if it is not one.

    A time with no zone counts as UTC.
    """
    if not isinstance(value, str):
        return None

    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        return None

    return (moment if moment.tzinfo else moment.replace(tzinfo=UTC)).astimezone(UTC).isoformat()


def build_rows(names: Sequence[str], values: Sequence[Sequence[JsonValue]], *, data_cloud: bool) -> list[Row]:
    """Return the rows of a Data Cloud answer as objects with clean names.

    If two names clean to the same one, the first stays.
    """
    cleaned = [clean_data_cloud_name(name) if data_cloud else to_snake_case(name) for name in names]
    keep = [index for index, name in enumerate(cleaned) if name not in cleaned[:index]]

    return [{cleaned[index]: row[index] for index in keep} for row in values]


def clean_records(records: Sequence[Mapping[str, JsonValue]]) -> list[Row]:
    """Return the records of the CRM REST API without `attributes`, with names in snake case."""
    rows: list[Row] = []

    for record in records:
        row: Row = {}

        for key, value in record.items():
            if key != "attributes":
                row.setdefault(to_snake_case(key), value)

        rows.append(row)

    return rows


def transform_rows(rows: Sequence[Row], feed: Feed, partition_date: date, loaded_at: datetime) -> list[Row]:
    """Return the rows of a feed with its date columns in UTC, `_loaded_at` and `data_particao`.

    A text with nothing in it becomes nothing, as the bulk API sends `""` for null. With
    `step_output_only`, `output_value_text` is nothing unless the step is an
    `ACTION_STEP`, and an object there is written as JSON.
    """
    result: list[Row] = []

    for row in rows:
        new: Row = {key: None if value == "" else value for key, value in row.items()}

        for column in feed.date_columns:
            if column in new:
                new[column] = parse_utc(new[column])

        new["_loaded_at"] = loaded_at.astimezone(UTC).isoformat()
        new["data_particao"] = partition_date.isoformat()

        if feed.step_output_only and "output_value_text" in new and "ai_agent_interaction_step_type" in new:
            output = new["output_value_text"] if new["ai_agent_interaction_step_type"] == STEP_TYPE else None
            new["output_value_text"] = dumps(output, ensure_ascii=False) if isinstance(output, dict) else output

        result.append(new)

    return result
