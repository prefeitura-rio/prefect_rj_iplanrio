from collections.abc import Awaitable, Callable, Iterator, Mapping
from datetime import UTC, datetime
from typing import cast

import pytest
from deltalake import DeltaTable
from fixtures import FakeBigQuery, FakeGcs
from prefect.testing.utilities import prefect_test_harness
from pyarrow import Table as ArrowTable
from pyarrow.parquet import write_table
from toolkit.dbt import DbtRun

from utils import flows
from utils.flows import (
    compact_tables,
    dump_arcgis,
    dump_database,
    dump_gsheet,
    ingest_api,
    load_files,
)
from utils.types import Cloud, Database, Table

CLOUD: Cloud = {"bucket": "simulated-bucket", "project": "p", "biglake_connection": "p.us.connection"}
FIRST_RUN = datetime(2024, 3, 10, 9, 0, tzinfo=UTC)
SECOND_RUN = datetime(2024, 3, 10, 9, 5, tzinfo=UTC)


@pytest.fixture(scope="module", autouse=True)
def prefect_harness() -> Iterator[None]:
    """Run the flows against a temporary Prefect API, so a test never touches a real one."""
    with prefect_test_harness():
        yield


@pytest.fixture
def cloud(fake_gcs: FakeGcs, fake_bigquery: type[FakeBigQuery]) -> type[FakeBigQuery]:
    """Return the fake BigQuery, with the fake bucket behind it."""
    assert fake_gcs
    return fake_bigquery


@pytest.fixture
def run_at(monkeypatch: pytest.MonkeyPatch) -> Callable[[datetime], None]:
    """Return a function that sets the time that the next run takes as its capture."""

    def set_time(moment: datetime) -> None:
        """Make `capture_time` return the moment."""
        monkeypatch.setattr(flows, "capture_time", lambda: moment)

    return set_time


def table_of(name: str) -> Table:
    """Return the table of a test, in a dataset of its own."""
    return {"dataset_id": "api", "table_id": name}


def read(cloud: type[FakeBigQuery], name: str) -> list[int]:
    """Return the ids that BigQuery sees in the table, sorted."""
    detected = cloud.tables[f"p.api_staging.{name}"]
    return sorted(cast("list[int]", DeltaTable(detected.uri).to_pyarrow_table().column("id").to_pylist()))


async def test_each_run_of_an_api_adds_its_records_and_a_rerun_of_the_same_run_adds_nothing(cloud, fake_api, run_at):
    """Keep the history of the API, and make a retry of a run harmless."""
    table = table_of("history")
    url = f"{fake_api.url}/list"

    run_at(FIRST_RUN)
    fake_api.replies.append((200, {"items": [{"id": 1}, {"id": 2}]}))
    await ingest_api(cloud=CLOUD, table=table, url=url, records_key="items")

    run_at(SECOND_RUN)
    fake_api.replies.append((200, {"items": [{"id": 3}]}))
    await ingest_api(cloud=CLOUD, table=table, url=url, records_key="items")

    fake_api.replies.append((200, {"items": [{"id": 4}]}))
    await ingest_api(cloud=CLOUD, table=table, url=url, records_key="items")

    assert read(cloud, "history") == [1, 2, 3]


async def test_a_response_with_no_record_loads_nothing(cloud, fake_api, run_at):
    """Skip the load, so no empty table and no commit appear."""
    run_at(FIRST_RUN)
    fake_api.replies.append((200, []))

    await ingest_api(cloud=CLOUD, table=table_of("empty"), url=f"{fake_api.url}/list")

    assert "p.api_staging.empty" not in cloud.tables


@pytest.mark.usefixtures("cloud")
async def test_the_names_in_the_headers_and_the_params_come_from_the_secret(fake_api, run_at, monkeypatch):
    """Send the token that the environment holds, and never one from the `prefect.yaml`."""
    monkeypatch.setenv("API_X__TOKEN", "abc")
    run_at(FIRST_RUN)
    fake_api.replies.append((200, [{"id": 1}]))

    await ingest_api(
        cloud=CLOUD,
        table=table_of("secret"),
        url=f"{fake_api.url}/list",
        secret_path="/api-x",
        headers={"Authorization": "Bearer {TOKEN}"},
        params={"key": "{TOKEN}"},
    )

    [seen] = fake_api.requests
    assert (seen.headers["authorization"], seen.query) == ("Bearer abc", {"key": ["abc"]})


async def test_overwrite_replaces_the_table_with_the_records_of_the_run(cloud, fake_api, run_at):
    """Keep only the last response, for an API that serves a list and not a stream."""
    table = table_of("snapshot")
    run_at(FIRST_RUN)
    fake_api.replies.append((200, [{"id": 1}, {"id": 2}]))
    await ingest_api(cloud=CLOUD, table=table, url=f"{fake_api.url}/list", dump_mode="overwrite")

    run_at(SECOND_RUN)
    fake_api.replies.append((200, [{"id": 3}]))
    await ingest_api(cloud=CLOUD, table=table, url=f"{fake_api.url}/list", dump_mode="overwrite")

    assert read(cloud, "snapshot") == [3]


async def test_compacting_merges_the_files_of_the_runs_and_loses_no_row(cloud, fake_api, run_at):
    """Leave one file for the day, and BigQuery still reads every record."""
    table = table_of("busy")
    for minute in range(3):
        run_at(datetime(2024, 3, 10, 9, minute, tzinfo=UTC))
        fake_api.replies.append((200, [{"id": minute}]))
        await ingest_api(cloud=CLOUD, table=table, url=f"{fake_api.url}/list")
    uri = cloud.tables["p.api_staging.busy"].uri
    assert len(DeltaTable(uri).file_uris()) == 3

    await compact_tables(cloud=CLOUD, tables=[table])

    assert len(DeltaTable(uri).file_uris()) == 1
    assert read(cloud, "busy") == [0, 1, 2]


async def test_files_that_a_flow_made_are_loaded_and_published(cloud, tmp_path):
    """Load the Parquet files of a folder, and create the BigQuery table over them."""
    write_table(ArrowTable.from_pylist([{"id": 7}, {"id": 8}]), tmp_path / "rows.parquet")

    await load_files(cloud=CLOUD, table=table_of("files"), data_path=tmp_path)

    assert read(cloud, "files") == [7, 8]


DATABASE: Database = {"db_type": "postgres", "host": "h", "port": 5432, "database": "d", "secret_path": "/db-x"}


@pytest.fixture
def steps(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Mapping[str, object]]]:
    """Replace the tasks that a flow calls with recorders, and return the calls in order."""
    calls: list[tuple[str, Mapping[str, object]]] = []

    def recorder(name: str) -> Callable[..., Awaitable[None]]:
        """Return a task that records its name and its keyword arguments."""

        async def step(*args: object, **kwargs: object) -> None:
            """Record the call."""
            calls.append((name, {"args": args, **kwargs}))

        return step

    for name in [
        "dump_db_to_gcs",
        "dump_arcgis_to_gcs",
        "dump_gsheet_to_gcs",
        "publish_table_in_bq",
        "run_dbt",
    ]:
        monkeypatch.setattr(flows, name, recorder(name))

    return calls


@pytest.mark.parametrize(
    ("dbt", "expected"),
    [
        pytest.param(None, ["dump_db_to_gcs", "publish_table_in_bq"], id="without_dbt"),
        pytest.param(
            DbtRun(repository="r"), ["dump_db_to_gcs", "publish_table_in_bq", "run_dbt"], id="dbt_after_publish"
        ),
    ],
)
async def test_a_database_dump_runs_dbt_only_after_the_publish(steps, dbt, expected):
    """Run the dump, publish the table, and then the dbt command if there is one."""
    await dump_database(database=DATABASE, cloud=CLOUD, table=table_of("t"), read={"table": "public.t"}, dbt=dbt)

    assert [name for name, _ in steps] == expected


async def test_an_arcgis_layer_is_dumped_with_its_filter_and_system_then_published(steps):
    """Pass the layer, the filter and the bucket to the dump, and publish."""
    await dump_arcgis(cloud=CLOUD, table=table_of("t"), url="https://x/layer", where="id > 1", crs="EPSG:3857")

    [(dump, arguments), (publish, _)] = steps
    assert (dump, arguments["where"], arguments["crs"], arguments["bucket"], publish) == (
        "dump_arcgis_to_gcs",
        "id > 1",
        "EPSG:3857",
        "simulated-bucket",
        "publish_table_in_bq",
    )


async def test_a_sheet_is_dumped_by_name_then_published(steps):
    """Pass the sheet name to the dump, and publish."""
    await dump_gsheet(cloud=CLOUD, table=table_of("t"), spreadsheet_id="abc", sheet="Dados")

    [(dump, arguments), (publish, _)] = steps
    assert (dump, arguments["sheet"], arguments["bucket"], publish) == (
        "dump_gsheet_to_gcs",
        "Dados",
        "simulated-bucket",
        "publish_table_in_bq",
    )
