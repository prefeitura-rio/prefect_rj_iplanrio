from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from google.cloud import bigquery
from pydantic import JsonValue

from pipelines.rj_crm__salesforce_datacloud.constants import FEEDS, SP_TZ
from pipelines.rj_crm__salesforce_datacloud.helpers.bq import field_names, missing_fields
from pipelines.rj_crm__salesforce_datacloud.helpers.messages import (
    build_feed_failure,
    build_run_failure,
    build_summary,
    cut,
)
from pipelines.rj_crm__salesforce_datacloud.helpers.run import add_up, plan_run
from pipelines.rj_crm__salesforce_datacloud.helpers.salesforce import as_rows
from pipelines.rj_crm__salesforce_datacloud.helpers.schemas import SCHEMAS
from pipelines.rj_crm__salesforce_datacloud.helpers.transform import (
    build_rows,
    clean_data_cloud_name,
    clean_records,
    parse_utc,
    to_snake_case,
    transform_rows,
)
from pipelines.rj_crm__salesforce_datacloud.helpers.window import (
    day_window,
    format_span,
    hour_window,
    plan_spans,
    to_utc_window,
)
from pipelines.rj_crm__salesforce_datacloud.types import Feed, Outcome, Row, Span, Window

NOW = datetime(2026, 9, 29, 15, 20, 30, tzinfo=SP_TZ)


def feed(name: str) -> Feed:
    """Return a feed of the pipeline by name."""
    return next(item for item in FEEDS if item.name == name)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("StartTimestamp", "start_timestamp", id="pascal_case"),
        pytest.param("AiAgentSession", "ai_agent_session", id="a_word_of_capitals"),
        pytest.param("KQIndividualId", "kq_individual_id", id="capitals_before_a_word"),
        pytest.param("already_snake", "already_snake", id="already_snake_case"),
    ],
)
def test_a_name_goes_to_snake_case(name: str, expected: str) -> None:
    """Split at the capitals."""
    assert to_snake_case(name) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("ssot__StartTime__c", "start_time", id="a_field_of_a_dmo"),
        pytest.param("AiAgentSession__dlm", "ai_agent_session", id="a_table_of_a_dmo"),
        pytest.param("EventId__c", "event_id", id="a_field_of_a_dll"),
        pytest.param("MessagingEvents__dll", "messaging_events", id="a_table_of_a_dll"),
    ],
)
def test_a_data_cloud_name_loses_its_prefix_and_suffix(name: str, expected: str) -> None:
    """Strip `ssot__` and the type suffix."""
    assert clean_data_cloud_name(name) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("2026-09-29T10:00:00.000Z", "2026-09-29T10:00:00+00:00", id="a_z_suffix"),
        pytest.param("2026-09-29 10:00:00", "2026-09-29T10:00:00+00:00", id="no_zone_counts_as_utc"),
        pytest.param("2026-09-29T10:00:00-03:00", "2026-09-29T13:00:00+00:00", id="an_offset_goes_to_utc"),
        pytest.param("not a date", None, id="not_a_date"),
        pytest.param(None, None, id="nothing"),
        pytest.param(5, None, id="a_number"),
    ],
)
def test_a_time_goes_to_utc_or_nothing(value: JsonValue, expected: str | None) -> None:
    """Write the time in UTC."""
    assert parse_utc(value) == expected


def test_the_columns_of_data_cloud_get_clean_names_and_a_repeat_keeps_the_first() -> None:
    """Clean each name, and drop a column that cleans to a name already there."""
    rows = build_rows(["ssot__Id__c", "ssot__StartTime__c", "Id__dll"], [["a", "t", "dup"]], data_cloud=True)

    assert rows == [{"id": "a", "start_time": "t"}]


def test_the_records_of_the_crm_lose_attributes_and_get_snake_case_names() -> None:
    """Drop the metadata of Salesforce."""
    assert clean_records([{"attributes": {"type": "X"}, "CreatedDate": "d", "Id": "1"}]) == [
        {"created_date": "d", "id": "1"}
    ]


def test_a_feed_row_gets_utc_dates_and_the_columns_of_the_load() -> None:
    """Convert the date columns, turn empty text to nothing, stamp the load columns.

    They are `_loaded_at` and `data_particao`.
    """
    rows: list[Row] = [{"id": "a", "start_timestamp": "2026-09-29T10:00:00Z", "end_timestamp": "", "other": "x"}]

    [row] = transform_rows(rows, feed("ai_agent_session"), date(2026, 9, 29), datetime(2026, 9, 29, 12, tzinfo=UTC))

    assert row == {
        "id": "a",
        "start_timestamp": "2026-09-29T10:00:00+00:00",
        "end_timestamp": None,
        "other": "x",
        "_loaded_at": "2026-09-29T12:00:00+00:00",
        "data_particao": "2026-09-29",
    }


@pytest.mark.parametrize(
    ("step_type", "output", "expected"),
    [
        pytest.param("ACTION_STEP", {"a": 1}, '{"a": 1}', id="an_action_step_keeps_its_object_as_json"),
        pytest.param("ACTION_STEP", "text", "text", id="an_action_step_keeps_its_text"),
        pytest.param("LLM_STEP", {"a": 1}, None, id="another_step_loses_its_output"),
    ],
)
def test_only_an_action_step_keeps_its_output(step_type: str, output: JsonValue, expected: str | None) -> None:
    """Zero the output of the other steps, and write an object as JSON."""
    rows: list[Row] = [{"ai_agent_interaction_step_type": step_type, "output_value_text": output}]

    [row] = transform_rows(rows, feed("ai_agent_interaction_step"), date(2026, 9, 29), NOW)

    assert row["output_value_text"] == expected


def test_a_table_without_the_step_rule_keeps_its_output() -> None:
    """Leave `output_value_text` alone when the feed has no rule."""
    rows: list[Row] = [{"ai_agent_interaction_step_type": "LLM_STEP", "output_value_text": "x"}]

    [row] = transform_rows(rows, feed("ai_agent_session"), date(2026, 9, 29), NOW)

    assert row["output_value_text"] == "x"


def test_the_hour_window_rolls_back_one_hour() -> None:
    """Start one hour before now, in the wall time of São Paulo."""
    assert hour_window(NOW) == Span("2026-09-29T14:20:30Z", "2026-09-29T15:20:30Z", date(2026, 9, 29))


def test_the_hour_window_does_not_cross_midnight() -> None:
    """Start at midnight when the last hour began the day before."""
    span = hour_window(datetime(2026, 9, 29, 0, 20, tzinfo=SP_TZ))

    assert (span.start, span.end) == ("2026-09-29T00:00:00Z", "2026-09-29T00:20:00Z")


def test_a_past_day_is_a_whole_day_and_today_ends_now() -> None:
    """Cut today at the current time, so the window never asks for the future."""
    assert day_window(date(2026, 9, 28), NOW) == Span("2026-09-28T00:00:00Z", "2026-09-29T00:00:00Z", date(2026, 9, 28))
    assert day_window(date(2026, 9, 29), NOW).end == "2026-09-29T15:20:30Z"


def test_the_utc_window_adds_the_partitions_of_utc() -> None:
    """Convert from São Paulo to UTC, and give the partitions of the window."""
    span, partitions = to_utc_window(Span("2026-09-28T00:00:00Z", "2026-09-29T00:00:00Z", date(2026, 9, 28)))

    assert (span.start, span.end) == ("2026-09-28T03:00:00Z", "2026-09-29T03:00:00Z")
    assert partitions == {"partition_start": "2026-09-28T00:00:00Z", "partition_end": "2026-09-29T00:00:00Z"}


def test_a_window_that_ends_on_midnight_utc_leaves_that_partition_out() -> None:
    """Take the end as exclusive."""
    _, partitions = to_utc_window(Span("2026-09-28T21:00:00Z", "2026-09-29T21:00:00Z", date(2026, 9, 28)))

    assert partitions["partition_end"] == "2026-09-29T00:00:00Z"


def test_a_window_is_formatted_for_a_log() -> None:
    """Drop the seconds and the Z."""
    assert (
        format_span(Span("2026-09-29T14:00:00Z", "2026-09-29T15:00:00Z", date(2026, 9, 29)))
        == "2026-09-29 14:00 to 2026-09-29 15:00 (SP)"
    )


@pytest.mark.parametrize(
    ("window", "starts", "count"),
    [
        pytest.param({}, ["2026-09-29T14:20:30Z"], 1, id="the_hour_by_default"),
        pytest.param({"mode": "day"}, ["2026-09-28T00:00:00Z"], 1, id="a_day_means_yesterday"),
        pytest.param(
            {"mode": "day", "partition_date": date(2026, 9, 1)}, ["2026-09-01T00:00:00Z"], 1, id="a_day_by_date"
        ),
        pytest.param({"reprocess_from": date(2026, 9, 26)}, ["2026-09-26T00:00:00Z"], 1, id="a_reprocess_of_one_day"),
        pytest.param(
            {"reprocess_from": date(2026, 9, 26), "reprocess_to": date(2026, 9, 28), "mode": "hour"},
            ["2026-09-26T00:00:00Z", "2026-09-27T00:00:00Z", "2026-09-28T00:00:00Z"],
            3,
            id="a_reprocess_ignores_the_mode",
        ),
    ],
)
def test_the_windows_of_a_run_follow_its_mode(window: dict[str, object], starts: list[str], count: int) -> None:
    """Plan one window, or one for each day of a reprocess."""
    spans = plan_spans(window, NOW)  # pyright: ignore[reportArgumentType]  # the test passes a plain dict as the window

    assert [span.start for span in spans] == starts
    assert len(spans) == count


@pytest.mark.parametrize(
    ("window", "message"),
    [
        pytest.param(
            {"reprocess_from": date(2026, 9, 28), "reprocess_to": date(2026, 9, 27)},
            "before",
            id="the_end_before_the_start",
        ),
        pytest.param(
            {"reprocess_from": date(2026, 9, 28), "reprocess_to": date(2026, 9, 30)},
            "future",
            id="the_end_in_the_future",
        ),
    ],
)
def test_a_reprocess_that_does_not_make_sense_is_refused(window: dict[str, object], message: str) -> None:
    """Raise before anything runs."""
    with pytest.raises(ValueError, match=message):
        plan_spans(window, NOW)  # pyright: ignore[reportArgumentType]  # the test passes a plain dict as the window


def test_the_feeds_have_unique_names_and_a_query_file() -> None:
    """Check the configuration at test time, as the old loader did at import."""
    names = [item.name for item in FEEDS]
    queries = Path(__file__).parent.parent / "queries"

    assert len(names) == len(set(names))
    assert all((queries / f"{item.query}.sql").is_file() for item in FEEDS)
    assert {item.source for item in FEEDS} == {"data_cloud", "crm_rest"}


def test_an_answer_of_data_cloud_gives_its_rows_as_lists() -> None:
    """Keep the lists and drop anything else."""
    assert as_rows([[1, 2], "x", [3]]) == [[1, 2], [3]]
    assert as_rows("nothing") == []


def test_a_message_is_cut_at_the_limit_of_discord() -> None:
    """Add a note when it is cut."""
    assert cut("short") == "short"
    assert len(cut("x" * 3000)) <= 2000
    assert cut("x" * 3000).endswith("(cortado)")


def test_the_messages_of_a_failure_have_the_window_and_the_error() -> None:
    """Show what failed, where and why."""
    feed_message = build_feed_failure("messaging_session", "Messaging", "w", "boom")

    assert "`messaging_session`" in feed_message
    assert "**Janela:** w" in feed_message
    assert "boom" in build_run_failure("flow", "run-1", "boom")
    assert "sem mensagem" in build_run_failure("flow", "run-1", None)


def test_the_summary_lists_the_rows_by_group_and_the_failed_tables() -> None:
    """Format the counts with a dot as the separator of thousands."""
    message = build_summary("modo='dia'", {"Agentforce": {"ai_agent_session": 12345}}, ["messaging_session"], 1.25)

    assert "• `ai_agent_session`: 12.345 linhas" in message
    assert "❌ **Falharam:** `messaging_session`" in message
    assert message.startswith("⚠️")
    assert build_summary("x", {}, [], 0).startswith("✅")


def test_a_run_plan_picks_the_listed_feeds_and_labels_a_reprocess() -> None:
    """Pick the feeds that the tables name, a window for each day, and a label."""
    names = [FEEDS[0].name]
    window = Window(reprocess_from=date(2026, 7, 15), reprocess_to=date(2026, 7, 16))

    plan = plan_run(window, names, datetime(2026, 7, 17, 12, tzinfo=UTC), 1.0)

    assert ([feed.name for feed in plan.feeds], len(plan.spans), plan.summarize) == (names, 2, True)
    assert plan.label == "reprocessamento 2026-07-15 a 2026-07-16 (2 dia(s))"


@pytest.mark.parametrize(
    ("window", "tables", "feeds", "summarize"),
    [
        pytest.param(Window(), None, len(FEEDS), False, id="the_hour_mode_runs_every_feed_without_a_summary"),
        pytest.param(Window(mode="day"), [], len(FEEDS), True, id="the_day_mode_posts_a_summary"),
    ],
)
def test_a_run_plan_follows_the_mode(window: Window, tables: list[str] | None, feeds: int, summarize: bool) -> None:
    """Run every feed when no table is named, and post the summary only for a day."""
    plan = plan_run(window, tables, datetime(2026, 7, 17, 12, tzinfo=UTC), 1.0)

    assert (len(plan.feeds), plan.summarize) == (feeds, summarize)


def test_a_table_that_is_not_a_feed_is_refused() -> None:
    """Raise with the unknown name before any extraction."""
    with pytest.raises(ValueError, match="Unknown tables: \\['nope'\\]"):
        plan_run({}, ["nope"], datetime(2026, 7, 17, tzinfo=UTC), 1.0)


def test_the_outcomes_add_up_by_group_and_the_failed_feeds_count_once() -> None:
    """Sum the rows of a feed over the windows and list a failed feed one time."""
    totals = add_up(
        [
            Outcome("a", "g1", 3),
            Outcome("a", "g1", 4),
            Outcome("b", "g2", 0, failed=True),
            Outcome("b", "g2", 0, failed=True),
        ]
    )

    assert (totals.rows, totals.failed) == ({"g1": {"a": 7}, "g2": {"b": 0}}, ["b"])


def test_only_the_final_tables_have_the_first_load_time() -> None:
    """Add `_first_loaded_at` to the final tables and not to the staging ones."""
    with_field = {name for name, fields in SCHEMAS.items() if "_first_loaded_at" in set(field_names(fields))}

    assert with_field == {name for name in SCHEMAS if not name.endswith("_staging")}


@pytest.mark.parametrize(
    ("current", "expected"),
    [
        pytest.param(["id", "_loaded_at"], ["_first_loaded_at"], id="a_new_field_is_missing"),
        pytest.param(["id", "_loaded_at", "_first_loaded_at"], [], id="nothing_is_missing"),
    ],
)
def test_the_missing_fields_of_a_table(current: list[str], expected: list[str]) -> None:
    """Find the fields to add to a table that exists."""
    wanted = [bigquery.SchemaField(name, "STRING") for name in ("id", "_loaded_at", "_first_loaded_at")]

    existing = [bigquery.SchemaField(name, "STRING") for name in current]

    assert field_names(missing_fields(existing, wanted)) == expected


def test_the_merge_keeps_the_first_load_time() -> None:
    """Fill `_first_loaded_at` on INSERT from the load time, never on UPDATE."""
    sql = (Path(__file__).parent.parent / "queries" / "merge_staging.sql").read_text()

    assert "INSERT ({{ insert_cols }}, _first_loaded_at)" in sql
    assert "VALUES ({{ insert_vals }}, s._loaded_at)" in sql
