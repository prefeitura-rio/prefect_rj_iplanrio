"""The dump against the cloud, simulated in the process or real.

Every scenario runs against two backends. `simulated` is the default: a fake GCS with the
XML API and its generation preconditions, BigQuery over a Delta table with the rules that the
real runs showed, and a database in memory. `gcp` is real GCS, BigQuery and BigLake, and runs
with `pytest -m gcp`. It needs `gcloud`, `bq`, and ADC with rights to create a bucket, a
BigLake connection, a dataset and a service account in `TOOLKIT_GCP_PROJECT`. It creates
scratch resources with a random suffix and deletes all of them at the end.
"""

import asyncio
import io
import json
import os
import shutil
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from functools import partial
from secrets import token_hex
from typing import Protocol, Unpack, cast

import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable
from fixtures import FakeBigQuery, FakeDatabase, FakeGcs
from google.cloud.bigquery import Client
from obstore.store import ObjectStore
from pydantic import SecretStr
from toolkit.constants import BATCH_COLUMN
from toolkit.executor.sources.backends.bigquery import BigQuerySource
from toolkit.gcs import create_store, list_keys
from toolkit.settings import Settings, settings
from toolkit.types import Batches

from prefect_rj_iplanrio import utils
from prefect_rj_iplanrio.types import DumpMode, DumpOptions
from prefect_rj_iplanrio.utils import (
    Destination,
    dump_in_batches,
    dump_to_delta,
    load_to_delta,
    publish,
    read_values,
    source_values,
)

LOCATION = "us"
INSTANT = datetime(2024, 3, 1, 10, tzinfo=UTC)
MONTHS: Batches = {"column": "dia", "width": "month", "begin": "2024-01-01", "end": "2024-03-31"}
SIMULATED_PROJECT = "simulated-project"


class Cloud(Protocol):
    """What a scenario needs of a cloud, so it runs against a real one or a simulated one."""

    bucket: str
    project: str

    def destination(self, name: str) -> Destination:
        """Return the destination of a table of this run."""
        ...

    async def publish(self, destination: Destination) -> None:
        """Create or refresh the BigQuery table of a destination."""
        ...

    def read(self, destination: Destination) -> pl.DataFrame:
        """Return what BigQuery returns for the table of a destination."""
        ...

    def types(self, destination: Destination) -> dict[str, str]:
        """Return the BigQuery type of each column of the table of a destination."""
        ...

    def store(self) -> ObjectStore:
        """Return the object store of the bucket."""
        ...


@dataclass(frozen=True)
class SimulatedCloud:
    """A fake GCS and a fake BigQuery."""

    gcs: FakeGcs
    config: Settings
    bigquery: type[FakeBigQuery]
    bucket: str = "simulated-bucket"
    project: str = SIMULATED_PROJECT

    def destination(self, name: str) -> Destination:
        """Return the destination of a table of this run."""
        return Destination(self.bucket, "simulated_dataset", name)

    async def publish(self, destination: Destination) -> None:
        """Create or refresh the fake BigQuery table of a destination."""
        await publish(destination, self.project, f"{self.project}.us.connection")

    def read(self, destination: Destination) -> pl.DataFrame:
        """Return the rows that the fake BigQuery returns, with the columns it detected."""
        detected = self.bigquery.tables[destination.table(self.project)]
        return cast(
            "pl.DataFrame", pl.from_arrow(DeltaTable(detected.uri).to_pyarrow_table(columns=list(detected.schema)))
        )

    def types(self, destination: Destination) -> dict[str, str]:
        """Return the types that the fake BigQuery detected."""
        return self.bigquery.tables[destination.table(self.project)].schema

    def store(self) -> ObjectStore:
        """Return the store of the fake bucket."""
        return create_store(self.bucket, self.config)


@pytest.fixture(scope="module")
def simulated_cloud(
    fake_gcs: FakeGcs, fake_gcs_settings: Settings, fake_bigquery: type[FakeBigQuery]
) -> Iterator[SimulatedCloud]:
    """Return the simulated cloud: `obstore` and BigQuery reach the fakes, and `deltalake` through the environment."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(utils, "create_store", partial(create_store, config=fake_gcs_settings))

        yield SimulatedCloud(fake_gcs, fake_gcs_settings, fake_bigquery)


@dataclass(frozen=True)
class RealCloud:
    """The scratch resources of one run against the real services."""

    project: str
    bucket: str
    dataset_id: str
    connection: str
    client: Client

    def destination(self, name: str) -> Destination:
        """Return the destination of a table of this run."""
        return Destination(self.bucket, self.dataset_id, name)

    async def publish(self, destination: Destination) -> None:
        """Create or refresh the BigQuery table of a destination."""
        await publish(destination, self.project, self.connection)

    def read(self, destination: Destination) -> pl.DataFrame:
        """Return what BigQuery returns for the table of a destination."""
        rows = self.client.query(f"SELECT * FROM `{destination.table(self.project)}`").to_arrow()
        return cast("pl.DataFrame", pl.from_arrow(rows))

    def types(self, destination: Destination) -> dict[str, str]:
        """Return the BigQuery type of each column of the table of a destination."""
        table = self.client.get_table(destination.table(self.project))
        return {field.name: field.field_type for field in table.schema}

    def store(self) -> ObjectStore:
        """Return the object store of the bucket, with the HMAC key of this run."""
        return create_store(self.bucket)


def run(*command: str, check: bool = True) -> str:
    """Run a command line tool and return its output."""
    done = subprocess.run(command, capture_output=True, text=True, timeout=240, check=check)
    return done.stdout


def retry(command: list[str], attempts: int = 8) -> None:
    """Run a command again while IAM has not caught up with a new resource."""
    for _ in range(attempts):
        if subprocess.run(command, capture_output=True, timeout=120, check=False).returncode == 0:
            return
        time.sleep(6)
    run(*command)


@pytest.fixture(scope="module")
def real_cloud() -> Iterator[RealCloud]:
    """Create a bucket, a BigLake connection, a dataset name and an HMAC key, and delete them after."""
    project = os.environ.get("TOOLKIT_GCP_PROJECT")
    if not project or not shutil.which("gcloud") or not shutil.which("bq"):
        pytest.skip("Set TOOLKIT_GCP_PROJECT and install gcloud and bq")

    token = token_hex(3)
    bucket, dataset_id = f"toolkit-test-{token}", f"toolkit_test_{token}"
    connection, account = f"toolkit_test_{token}", f"tkt{token}"
    email = f"{account}@{project}.iam.gserviceaccount.com"
    key = ""

    try:
        run(
            "gcloud",
            "storage",
            "buckets",
            "create",
            f"gs://{bucket}",
            f"--project={project}",
            "--location=US",
            "--uniform-bucket-level-access",
        )
        run(
            "bq",
            "mk",
            "--connection",
            f"--location={LOCATION}",
            f"--project_id={project}",
            "--connection_type=CLOUD_RESOURCE",
            connection,
        )
        info = json.loads(run("bq", "show", "--connection", "--format=json", f"{project}.{LOCATION}.{connection}"))
        member = f"serviceAccount:{info['cloudResource']['serviceAccountId']}"
        retry(
            [
                "gcloud",
                "storage",
                "buckets",
                "add-iam-policy-binding",
                f"gs://{bucket}",
                f"--member={member}",
                "--role=roles/storage.objectViewer",
            ]
        )
        run(
            "gcloud",
            "iam",
            "service-accounts",
            "create",
            account,
            f"--project={project}",
            "--display-name=toolkit test (temporary)",
        )
        retry(
            [
                "gcloud",
                "storage",
                "buckets",
                "add-iam-policy-binding",
                f"gs://{bucket}",
                f"--member=serviceAccount:{email}",
                "--role=roles/storage.objectAdmin",
            ]
        )
        hmac = json.loads(run("gcloud", "storage", "hmac", "create", email, f"--project={project}", "--format=json"))
        key = hmac["metadata"]["accessId"]

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(settings, "BIGQUERY_PROJECT", project)
            patch.setattr(settings, "GCS_HMAC_ACCESS_ID", key)
            patch.setattr(settings, "GCS_HMAC_SECRET", SecretStr(hmac["secret"]))
            yield RealCloud(project, bucket, dataset_id, f"{project}.{LOCATION}.{connection}", Client(project=project))
    finally:
        run("bq", f"--project_id={project}", "rm", "-r", "-f", "-d", f"{dataset_id}_staging", check=False)
        run("bq", f"--project_id={project}", "rm", "-r", "-f", "-d", dataset_id, check=False)
        run("bq", "rm", "--connection", "-f", f"{project}.{LOCATION}.{connection}", check=False)
        run("gcloud", "storage", "rm", "-r", f"gs://{bucket}", f"--project={project}", check=False)
        if key:
            run("gcloud", "storage", "hmac", "update", key, "--deactivate", f"--project={project}", check=False)
            run("gcloud", "storage", "hmac", "delete", key, f"--project={project}", check=False)
        run("gcloud", "iam", "service-accounts", "delete", email, f"--project={project}", "--quiet", check=False)


@pytest.fixture(
    scope="module",
    params=[pytest.param("simulated", id="simulated"), pytest.param("gcp", id="gcp", marks=pytest.mark.gcp)],
)
def cloud(request: pytest.FixtureRequest) -> Cloud:
    """Return the simulated cloud, or the real one when the run asks for `-m gcp`."""
    fixture = {"simulated": "simulated_cloud", "gcp": "real_cloud"}[request.param]

    return cast("Cloud", request.getfixturevalue(fixture))


def sample(rows: list[dict[str, object]]) -> pa.Table:
    """Return rows as an Arrow table."""
    return pa.Table.from_pylist(rows)


SAMPLE: list[dict[str, object]] = [
    {"id": index, "nome": f" cliente {index} ", "dia": date(2024, 1 + index % 3, 1 + index % 27), "valor": index * 1.5}
    for index in range(1, 301)
]


@pytest.fixture
def database(fake_database: FakeDatabase) -> FakeDatabase:
    """Return the database in memory with the 300 sample orders."""
    fake_database.tables["public.orders"] = sample(SAMPLE)

    return fake_database


async def dump(
    database: FakeDatabase,
    destination: Destination,
    dump_mode: DumpMode = "append",
    table: str = "public.orders",
    **options: Unpack[DumpOptions],
) -> None:
    """Dump a table of the database into the Delta table of a destination."""
    values = read_values("postgres", {"table": table}, "batches" in options)
    await dump_to_delta(database, values, destination.uri, dump_mode, options)


def name(prefix: str) -> str:
    """Return a table name that no other test of the run uses."""
    return f"{prefix}_{token_hex(2)}"


async def test_a_dump_is_read_by_bigquery_with_the_rows_of_the_source(cloud, database):
    """Load monthly windows, publish them, and find every row once in BigQuery."""
    destination = cloud.destination(name("load"))

    await dump(database, destination, batches=MONTHS)
    await cloud.publish(destination)

    frame = cloud.read(destination)
    assert (frame.height, frame["id"].n_unique()) == (300, 300)
    assert sorted(frame[BATCH_COLUMN].unique().to_list()) == ["2024-01-01", "2024-02-01", "2024-03-01"]


async def test_the_same_dump_again_writes_no_commit_and_no_object(cloud, database):
    """Skip every window that did not change, so the bucket and the log stay as they are."""
    destination = cloud.destination(name("rerun"))
    await dump(database, destination, batches=MONTHS)
    before = DeltaTable(destination.uri)
    version, files = before.version(), sorted(before.file_uris())

    await dump(database, destination, batches=MONTHS)

    after = DeltaTable(destination.uri)
    assert after.version() == version
    assert sorted(after.file_uris()) == files


async def test_a_changed_window_is_replaced_in_bigquery_without_duplicates(cloud, database):
    """Replace only the windows that changed, and keep one copy of every row."""
    destination = cloud.destination(name("change"))
    await dump(database, destination, batches=MONTHS)
    await cloud.publish(destination)
    database.tables["public.orders"] = sample([{**SAMPLE[0], "valor": 9999.0}, *SAMPLE[1:]])

    await dump(database, destination, batches=MONTHS)

    frame = cloud.read(destination)
    assert frame.filter(pl.col("id") == 1)["valor"].to_list() == [9999.0]
    assert (frame.height, frame["id"].n_unique()) == (300, 300)


async def test_a_window_that_became_empty_loses_its_rows_in_bigquery(cloud, database):
    """Expire the rows of a month that has none in the source now."""
    destination = cloud.destination(name("empty"))
    await dump(database, destination, batches=MONTHS)
    await cloud.publish(destination)
    database.tables["public.orders"] = sample([row for row in SAMPLE if cast("date", row["dia"]).month != 3])

    await dump(database, destination, batches=MONTHS)

    assert sorted(cloud.read(destination)[BATCH_COLUMN].unique().to_list()) == ["2024-01-01", "2024-02-01"]


async def test_overwrite_with_other_columns_reaches_bigquery_after_publish(cloud, database):
    """Replace the schema in overwrite, refresh the table, and see only the new columns."""
    destination = cloud.destination(name("schema"))
    await dump(database, destination, batches=MONTHS)
    await cloud.publish(destination)

    await dump(database, destination, "overwrite", batches=MONTHS, select=["id", "dia"])
    await cloud.publish(destination)

    assert list(cloud.read(destination).columns) == ["id", "dia", BATCH_COLUMN]


async def test_bigquery_sees_the_new_schema_as_soon_as_the_files_change(cloud, database):
    """Read the schema from the Delta log on every query, so a publish is not needed to see it."""
    destination = cloud.destination(name("detect"))
    await dump(database, destination, batches=MONTHS)
    await cloud.publish(destination)

    await dump(database, destination, "overwrite", batches=MONTHS, select=["id", "dia"])

    assert list(cloud.types(destination)) == ["id", "dia", BATCH_COLUMN]


async def test_publishing_twice_refreshes_the_table_instead_of_failing(cloud, database):
    """Create the table the first time and refresh its schema the next times."""
    destination = cloud.destination(name("twice"))
    await dump(database, destination)

    await cloud.publish(destination)
    await cloud.publish(destination)

    assert cloud.read(destination).height == 300


async def test_the_types_of_the_pipelines_survive_delta_and_bigquery(cloud):
    """Read integers, decimals, dates and both kinds of timestamp with the types that dbt expects."""
    destination = cloud.destination(name("types"))
    frame = pa.table(
        {
            "id": [1],
            "texto": ["a"],
            "numero": [1.5],
            "decimal": pa.array([Decimal("1.25")], pa.decimal128(10, 2)),
            "flag": [True],
            "dia": pa.array([date(2024, 3, 1)]),
            "instante": pa.array([INSTANT.replace(tzinfo=None)], pa.timestamp("us")),
            "instante_utc": pa.array([INSTANT], pa.timestamp("us", "UTC")),
        }
    )
    await dump(FakeDatabase({"public.types": frame}), destination, table="public.types")

    await cloud.publish(destination)

    assert cloud.types(destination) == {
        "id": "INTEGER",
        "texto": "STRING",
        "numero": "FLOAT",
        "decimal": "NUMERIC",
        "flag": "BOOLEAN",
        "dia": "DATE",
        "instante": "DATETIME",
        "instante_utc": "TIMESTAMP",
        BATCH_COLUMN: "STRING",
    }


async def test_runs_that_write_the_same_table_at_the_same_time_lose_no_window(cloud):
    """Let four runs commit to one table over `gs://`, and find every month in it."""
    destination = cloud.destination(name("race"))
    months = [(f"2024-{month:02d}-01", f"2024-{month:02d}-28") for month in range(1, 5)]

    async def one(begin: str, end: str) -> None:
        """Dump a single month into the shared table."""
        rows = [{**row, "dia": date.fromisoformat(begin)} for row in SAMPLE[:10]]
        batches: Batches = {"column": "dia", "width": "month", "begin": begin, "end": end}
        await dump(FakeDatabase({"public.orders": sample(rows)}), destination, batches=batches)

    await asyncio.gather(*(one(begin, end) for begin, end in months))

    windows = cast("list[str]", DeltaTable(destination.uri).to_pyarrow_table().column(BATCH_COLUMN).to_pylist())
    assert sorted(set(windows)) == [begin for begin, _ in months]


async def test_a_dump_leaves_a_checkpoint_and_bigquery_still_reads(cloud, database):
    """Write a checkpoint after the windows, and query the table that has one."""
    destination = cloud.destination(name("check"))

    await dump(database, destination, batches=MONTHS)
    await cloud.publish(destination)

    keys = await list_keys(cloud.store(), f"{destination.prefix}/_delta_log")
    assert any("checkpoint" in key for key in keys)
    assert cloud.read(destination).height == 300


async def test_the_files_of_an_older_layout_go_before_an_overwrite_and_a_delta_table_stays(cloud, database):
    """Delete old Hive files in the first overwrite, and never delete the Delta table in the next."""
    destination = cloud.destination(name("legacy"))
    store = cloud.store()
    old = f"{destination.prefix}/ano=2023/part-old.parquet"
    await store.put_async(old, b"old")
    values = read_values("postgres", {"table": "public.orders"}, batched=False)

    await load_to_delta(database, values, destination, "overwrite", {})
    first = await list_keys(store, destination.prefix)
    await load_to_delta(database, values, destination, "overwrite", {})
    second = await list_keys(store, destination.prefix)

    assert old not in first
    assert any("_delta_log" in key for key in second)
    assert DeltaTable(destination.uri).to_pyarrow_table().num_rows == 300


@pytest.mark.gcp
async def test_bigquery_is_copied_to_the_bucket_in_batches(real_cloud):
    """Export a BigQuery table as one Parquet object for each batch of rows."""
    destination = real_cloud.destination(name("export"))
    schema = f"{real_cloud.project}.{real_cloud.dataset_id}"
    real_cloud.client.query(f"CREATE SCHEMA IF NOT EXISTS `{schema}`").result()
    real_cloud.client.query(
        f"CREATE OR REPLACE TABLE `{schema}.numbers` AS " + "SELECT x AS id FROM UNNEST(GENERATE_ARRAY(1, 2500)) AS x"
    ).result()
    values = source_values("bigquery", f"SELECT id FROM `{schema}.numbers`")

    await dump_in_batches(BigQuerySource(real_cloud.project), values, destination, "append", 1000)

    store = real_cloud.store()
    keys = await list_keys(store, destination.prefix)
    rows = 0
    for key in keys:
        body = bytes(await (await store.get_async(key)).bytes_async())
        rows += pl.read_parquet(io.BytesIO(body)).height
    assert (len(keys), rows) == (3, 2500)
