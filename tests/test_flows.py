from dataclasses import replace
from pathlib import Path

import pytest
from toolkit.telemetry import logger

from utils import flows
from utils.flows import dispatch_hsm, load_records, send_alerts
from utils.hsm import Campaign, HsmRun, Recipient, Waits
from utils.types import Cloud, Table

TABLE = Table(dataset_id="ds", table_id="tb")
CLOUD = Cloud(bucket="b", project="p", connection="p.us.biglake")
RECORDS = [{"id": "1"}]


@pytest.fixture
def calls(monkeypatch):
    """Replace the tasks that `load_records` runs with fakes that record each call, in order."""
    seen = []

    async def write_records(records, folder):
        seen.append(("write_records", records, folder.exists()))
        return folder

    async def write_parquet_records(records, folder):
        seen.append(("write_parquet_records", records, folder.exists()))
        return folder

    async def upload_files_to_gcs(folder, table, **settings):
        seen.append(("upload", isinstance(folder, Path), table, settings))

    async def publish_table_in_bq(table, cloud):
        seen.append(("publish", table, cloud))

    monkeypatch.setattr(flows, "write_records", write_records)
    monkeypatch.setattr(flows, "write_parquet_records", write_parquet_records)
    monkeypatch.setattr(flows, "upload_files_to_gcs", upload_files_to_gcs)
    monkeypatch.setattr(flows, "publish_table_in_bq", publish_table_in_bq)
    monkeypatch.setattr(flows, "capture_options", lambda dump_mode: {"capture": dump_mode})

    return seen


def settings(source_format, dump_mode, options):
    """Return what the upload receives besides the folder and the table."""
    return {"bucket": "b", "dump_mode": dump_mode, "source_format": source_format, "options": options}


@pytest.mark.parametrize(
    ("arguments", "writer", "uploaded"),
    [
        pytest.param(
            {},
            "write_records",
            settings("json", "append", {"capture": "append"}),
            id="json_lines_that_append_by_default",
        ),
        pytest.param(
            {"parquet": True},
            "write_parquet_records",
            settings("parquet", "append", {"capture": "append"}),
            id="parquet_for_text_dates",
        ),
        pytest.param(
            {"dump_mode": "overwrite"},
            "write_records",
            settings("json", "overwrite", {"capture": "overwrite"}),
            id="an_overwrite",
        ),
        pytest.param(
            {"options": {"select": ["id"]}},
            "write_records",
            settings("json", "append", {"select": ["id"]}),
            id="the_options_of_the_caller_win",
        ),
    ],
)
async def test_records_are_written_loaded_and_published_in_that_order(calls, arguments, writer, uploaded):
    """Write the records in a folder that exists, load that folder, and publish the table last."""
    await load_records(RECORDS, TABLE, CLOUD, **arguments)

    assert calls == [(writer, RECORDS, True), ("upload", True, TABLE, uploaded), ("publish", TABLE, CLOUD)]


@pytest.fixture
def alerts(monkeypatch):
    """Return the messages that a run of `send_alerts` sent, with a fake query that gives three rows."""
    sent = []
    logged = []

    class Frame:
        def __init__(self, rows):
            self.rows = rows

        def to_dicts(self):
            return self.rows

    async def read_bq(query, *, project):
        assert project == "rj-iplanrio"
        return Frame([{"n": 1}, {"n": 2}, {"n": 3}] if query == "three" else [])

    async def notify_discord(message, *, webhook):
        sent.append((message, webhook))

    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://hook")
    monkeypatch.setattr(flows, "read_bq", read_bq)
    monkeypatch.setattr(flows, "notify_discord", notify_discord)
    monkeypatch.setattr(logger, "info", lambda message, *args: logged.append(message % args))

    return sent, logged


@pytest.mark.parametrize(
    ("query", "test_mode", "sent_messages", "logged_messages"),
    [
        pytest.param(
            "three",
            False,
            [("row 1", "https://hook"), ("row 3", "https://hook")],
            [],
            id="a_row_that_builds_nothing_is_skipped",
        ),
        pytest.param("three", True, [], ["Test run: row 1", "Test run: row 3"], id="test_mode_logs_and_sends_nothing"),
        pytest.param("none", False, [], ["No alert found, so the run ends"], id="no_row_ends_the_run"),
    ],
)
async def test_an_alert_goes_to_discord_for_each_row_that_builds_a_message(
    alerts, query, test_mode, sent_messages, logged_messages
):
    """Send a message for each row, skip a row with no message, and log in test mode."""
    sent, logged = alerts

    def build_message(row):
        return None if row["n"] == 2 else f"row {row['n']}"

    await send_alerts(query, build_message, test_mode=test_mode)

    assert (sent, logged) == (sent_messages, logged_messages)


@pytest.fixture
def dispatch(monkeypatch):
    """Return what a run of `dispatch_hsm` did, with fakes for the sender, the notices and the load."""
    done = []

    async def post_notice(text):
        done.append(("notice", text.splitlines()[0]))

    async def run_hsm(hsm, recipients, selection, wait, *, test_mode):
        done.append(("run_hsm", hsm.campaign_name, len(recipients), selection.percentage, wait, test_mode))
        return HsmRun(rows=[{"id": "1"}], dispatch_date="2026-10-22 10:00", error=state["error"])

    async def post_hsm_results(_hsm, dispatch_date, count, *, test_mode):
        done.append(("results", dispatch_date, count, test_mode))

    async def load_records(rows, table, cloud):
        done.append(("load", rows, table, cloud))

    async def no_sleep(seconds):
        done.append(("sleep", seconds))

    state = {"error": None}
    monkeypatch.setattr(flows, "post_notice", post_notice)
    monkeypatch.setattr(flows, "run_hsm", run_hsm)
    monkeypatch.setattr(flows, "post_hsm_results", post_hsm_results)
    monkeypatch.setattr(flows, "load_records", load_records)
    monkeypatch.setattr(flows, "sleep", no_sleep)

    return done, state


CAMPAIGN = Campaign(name="demo", id_hsm=1, cost_center_id=2, chunk_size=3, test_mode=False)
PEOPLE = [Recipient(to="5521999999999", external_id="1")]
WAITS = Waits(before_dispatch=5, before_results=15)


async def run_dispatch(campaign, recipients):
    """Run `dispatch_hsm` with the sample waits, table and cloud."""
    await dispatch_hsm(campaign, recipients, whitelist_percentage=30, waits=WAITS, table=TABLE, cloud=CLOUD)


async def test_nobody_to_message_posts_a_notice_and_ends(dispatch):
    """Post the notice and send nothing, record nothing and wait for nothing."""
    done, _ = dispatch

    await run_dispatch(CAMPAIGN, [])

    assert [step[0] for step in done] == ["notice"]


async def test_a_run_sends_records_waits_and_reports(dispatch):
    """Send to the people, record what went out, then wait and post the results."""
    done, _ = dispatch

    await run_dispatch(CAMPAIGN, PEOPLE)

    assert done == [
        ("run_hsm", "demo", 1, 30, 5, False),
        ("load", [{"id": "1"}], TABLE, CLOUD),
        ("sleep", 900),
        ("results", "2026-10-22 10:00", 1, False),
    ]


async def test_a_test_run_records_nothing(dispatch):
    """Send and report, but never load the rows of a test."""
    done, _ = dispatch

    await run_dispatch(replace(CAMPAIGN, test_mode=True), PEOPLE)

    assert [step[0] for step in done] == ["run_hsm", "sleep", "results"]
    assert done[0][1] == "teste-demo"


async def test_a_failed_batch_is_recorded_and_then_the_run_raises(dispatch):
    """Record the batches that went out before the error, and skip the wait and the results."""
    done, state = dispatch
    state["error"] = "batch 2 failed"

    with pytest.raises(RuntimeError, match="batch 2 failed"):
        await run_dispatch(CAMPAIGN, PEOPLE)

    assert [step[0] for step in done] == ["run_hsm", "load"]
