from pathlib import Path

import pytest

from utils import flows
from utils.flows import load_records
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
