from contextlib import asynccontextmanager
from pathlib import Path
from typing import ClassVar

import pytest
from psycopg.sql import SQL, Composable
from pydantic import ValidationError

from toolkit import dump
from toolkit.dump import (
    ConnectionRequest,
    DumpContext,
    DumpRequest,
    StageRequest,
    run_dump,
    with_retries,
)
from toolkit.executor.engines.duckdb import Extension
from toolkit.executor.executor import Step
from toolkit.executor.sources.protocols import DBConn, SourceContext
from toolkit.partitions import Selection
from toolkit.settings import Settings

CONFIG = Settings(STAGING_BUCKET="stg", BIGQUERY_PROJECT="proj")
KEYS = [
    "staging/ds/tb/ano_particao=2024/mes_particao=1/data_particao=2024-01-10/f.parquet",
    "staging/ds/tb/ano_particao=2024/mes_particao=2/data_particao=2024-02-10/f.parquet",
]


CONNECTION = {
    "hostname": "h",
    "port": 5432,
    "database": "d",
    "user": "u",
    "password": "p",
}


def request(**overrides):
    values = {
        "database_type": "postgres",
        "connection": CONNECTION,
        "query": "SELECT 1",
        "dataset_id": "ds",
        "table_id": "tb",
    }
    return DumpRequest.model_validate(values | overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"dataset_id": "a-b"}, id="dataset_with_dash"),
        pytest.param({"table_id": "a/b"}, id="table_with_slash"),
        pytest.param({"dump_mode": "merge"}, id="unknown_mode"),
        pytest.param({"connection": CONNECTION | {"port": 0}}, id="port_zero"),
        pytest.param(
            {"connection": CONNECTION | {"extra": 1}}, id="extra_connection_field"
        ),
        pytest.param({"batch_size": 0}, id="batch_zero"),
        pytest.param({"query": ""}, id="empty_query"),
        pytest.param({"unknown": 1}, id="extra_field"),
    ],
)
def test_request_rejects_invalid_values(overrides):
    with pytest.raises(ValidationError):
        request(**overrides)


def test_request_rejects_an_unregistered_database_type():
    with pytest.raises(ValidationError, match=r"unknown source: db2 .*known sources"):
        request(database_type="db2")


def test_request_hides_the_password():
    assert "p'w" not in repr(request(connection=CONNECTION | {"password": "p'w"}))


def test_request_without_a_connection_is_valid():
    assert request(database_type="bigquery", connection=None).connection is None


def test_connection_request_builds_the_source_connection_data():
    assert ConnectionRequest.model_validate(CONNECTION).to_conn() == DBConn(
        "h", 5432, "d", "u", "p"
    )


def test_context_paths():
    context = DumpContext(request(), CONFIG)
    assert context.prefix == "staging/ds/tb"
    assert context.uri == "gs://stg/staging/ds/tb"
    assert context.table == "proj.ds_staging.tb"


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        pytest.param("Data Início", "data_inicio", id="normalized"),
        pytest.param(None, None, id="none"),
    ],
)
def test_date_column_is_normalized(column, expected):
    assert DumpContext(request(partition_column=column), CONFIG).date_column == expected


async def test_overwrite_clears_prefix_and_drops_table(fake_s3, fake_bigquery):
    client, bigquery = fake_s3([*KEYS, "other/x"]), fake_bigquery()
    await DumpContext(request(dump_mode="overwrite"), CONFIG).prepare(client, bigquery)
    assert client.deleted == KEYS
    assert bigquery.calls == [("drop_table", "proj.ds_staging.tb")]


async def test_append_keeps_existing_data(fake_s3, fake_bigquery):
    client, bigquery = fake_s3(KEYS), fake_bigquery()
    await DumpContext(request(dump_mode="append"), CONFIG).prepare(client, bigquery)
    assert client.deleted == []
    assert bigquery.calls == []


@pytest.mark.parametrize(
    ("mode", "exists", "creates"),
    [
        pytest.param("overwrite", True, True, id="overwrite_recreates"),
        pytest.param("append", False, True, id="append_creates_when_missing"),
        pytest.param("append", True, False, id="append_keeps_existing_table"),
    ],
)
async def test_publish_creates_the_external_table(fake_bigquery, mode, exists, creates):
    bigquery = fake_bigquery(exists=exists)
    await DumpContext(request(dump_mode=mode), CONFIG).publish(bigquery)
    created = [call for call in bigquery.calls if call[0] == "create_external_table"]
    assert bool(created) is creates
    assert bigquery.calls[0] == ("create_dataset", "proj.ds_staging")


async def test_publish_drops_the_prod_dataset_when_asked(fake_bigquery):
    bigquery = fake_bigquery()
    context = DumpContext(request(only_staging_dataset=True), CONFIG)
    await context.publish(bigquery)
    assert ("delete_dataset", "proj.ds") in bigquery.calls


async def test_publish_uses_the_biglake_connection_only_when_enabled(fake_bigquery):
    config = Settings(
        STAGING_BUCKET="stg", BIGQUERY_PROJECT="p", BIGQUERY_BIGLAKE_CONNECTION="conn"
    )
    for biglake, expected in ((True, "conn"), (False, "None")):
        bigquery = fake_bigquery()
        await DumpContext(request(biglake_table=biglake), config).publish(bigquery)
        created = next(
            call for call in bigquery.calls if call[0] == "create_external_table"
        )
        assert created[-1] == expected


async def test_clear_selection_deletes_only_the_rewritten_partitions(fake_s3):
    from datetime import date

    from toolkit.partitions import FromDateSelection

    client = fake_s3(KEYS)
    selection = FromDateSelection(column="d", lower=date(2024, 2, 1))
    await DumpContext(request(), CONFIG).clear_selection(client, selection)
    assert client.deleted == [KEYS[1]]


async def test_with_retries_returns_none_after_a_late_success():
    calls = 0

    async def flaky() -> None:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("transient")

    assert await with_retries(3, 0, "t", flaky) is None
    assert calls == 3


async def test_with_retries_returns_the_last_error_when_all_attempts_fail():
    async def broken() -> None:
        raise RuntimeError("down")

    error = await with_retries(2, 0, "t", broken)
    assert isinstance(error, RuntimeError)
    assert str(error) == "down"


def test_stage_request_is_frozen():
    stage = StageRequest(dataset_id="ds", table_id="tb")
    with pytest.raises(ValidationError):
        stage.dataset_id = "other"


class FakeSource:
    """A custom source: it keeps the context it was built from."""

    name: ClassVar[str] = "fake"
    extensions: ClassVar[tuple[Extension, ...]] = ()

    def __init__(
        self,
        context: SourceContext,
        prepared: list[SourceContext],
        *,
        rows: bool = True,
    ) -> None:
        self.context = context
        self.rows = rows
        self.prepared = prepared

    def window(self, query: str, selection: Selection) -> str:
        return f"{query} /* {selection.type} */"

    def setup(self) -> str | None:
        return "-- attach"

    async def fetch(self, query: str) -> list[Path]:
        self.prepared.append(self.context)
        return [Path("a.parquet")] if self.rows else []

    def scan(self, query: str) -> Composable:
        return SQL("(SELECT 1 AS a)")


@pytest.fixture
def stub_services(monkeypatch, fake_s3, fake_bigquery):
    """Replace GCS and BigQuery with fakes, and return the BigQuery fake."""
    bigquery = fake_bigquery()
    client = fake_s3([])

    @asynccontextmanager
    async def create_client(config):
        yield client

    @asynccontextmanager
    async def connect(project=None):
        yield bigquery

    monkeypatch.setattr(dump.gcs, "create_client", create_client)
    monkeypatch.setattr(dump.BigQuery, "connect", staticmethod(connect))
    return bigquery


@pytest.fixture
def engine_factory(fake_engine):
    """An engine factory that opens a fake engine and records what it was given."""
    engines = []
    extensions_seen = []

    @asynccontextmanager
    async def open_engine(extensions, config):
        extensions_seen.append([extension.name for extension in extensions])
        engine = fake_engine([{"column_name": "a", "column_type": "BIGINT"}])
        engines.append(engine)
        yield engine

    return open_engine, engines, extensions_seen


async def test_run_dump_builds_the_source_from_the_context(
    stub_services, engine_factory
):
    open_engine, engines, extensions_seen = engine_factory
    prepared: list[SourceContext] = []
    config = Settings(STAGING_BUCKET="stg", BIGQUERY_PROJECT="proj")

    await run_dump(
        request(database_type=None, batch_size=7),
        make_source=lambda context: FakeSource(context, prepared),
        open_engine=open_engine,
        config=config,
    )

    [context] = prepared
    assert context.conn == DBConn("h", 5432, "d", "u", "p")
    assert context.batch_size == 7
    assert extensions_seen == [["httpfs"]]
    assert engines[0].statements[0] == "-- attach"
    copy = engines[0].statements[-1]
    assert "COPY (" in copy
    assert "'gs://stg/staging/ds/tb/data_" in copy
    created = [c for c in stub_services.calls if c[0] == "create_external_table"]
    assert created == [
        (
            "create_external_table",
            "proj.ds_staging.tb",
            "gs://stg/staging/ds/tb",
            "PARQUET",
            "None",
        )
    ]


async def test_run_dump_applies_the_steps_before_the_copy(
    stub_services, engine_factory, tmp_path
):
    (tmp_path / "upper.sql").write_text("SELECT upper(a) AS a FROM {{ source }}\n")
    open_engine, engines, _ = engine_factory
    config = Settings(STAGING_BUCKET="stg", BIGQUERY_PROJECT="proj")

    await run_dump(
        request(database_type=None),
        make_source=lambda context: FakeSource(context, []),
        open_engine=open_engine,
        config=config,
        steps=[Step("upper")],
        templates=tmp_path,
    )

    statements = engines[0].statements
    view = next(
        i for i, s in enumerate(statements) if "CREATE OR REPLACE TEMP VIEW" in s
    )
    copy = next(i for i, s in enumerate(statements) if "COPY (" in s)
    assert view < copy
    assert "upper(a) AS a FROM (SELECT 1 AS a)" in statements[view]
    assert '"clean_' in statements[copy]


async def test_run_dump_without_steps_creates_no_view(stub_services, engine_factory):
    open_engine, engines, _ = engine_factory
    config = Settings(STAGING_BUCKET="stg", BIGQUERY_PROJECT="proj")

    await run_dump(
        request(database_type=None),
        make_source=lambda context: FakeSource(context, []),
        open_engine=open_engine,
        config=config,
    )

    assert not any("TEMP VIEW" in s for s in engines[0].statements)


async def test_run_dump_skips_the_copy_when_the_source_has_no_rows(
    stub_services, engine_factory
):
    open_engine, engines, _ = engine_factory
    config = Settings(STAGING_BUCKET="stg", BIGQUERY_PROJECT="proj")

    await run_dump(
        request(database_type=None),
        make_source=lambda context: FakeSource(context, [], rows=False),
        open_engine=open_engine,
        config=config,
    )

    assert not any("COPY (" in statement for statement in engines[0].statements)


async def test_run_dump_needs_a_source_or_a_database_type(stub_services):
    with pytest.raises(ValueError, match="Give make_source or set database_type"):
        await run_dump(request(database_type=None))


async def test_run_dump_reports_every_failed_selection(stub_services, fake_engine):
    @asynccontextmanager
    async def broken_engine(extensions, config):
        raise RuntimeError("engine down")
        yield fake_engine()

    config = Settings(
        STAGING_BUCKET="stg",
        BIGQUERY_PROJECT="proj",
        DUMP_RETRY_ATTEMPTS=2,
        DUMP_RETRY_DELAY_SECONDS=0,
    )
    with pytest.raises(RuntimeError, match=r"1 of 1 selections failed: engine down"):
        await run_dump(
            request(database_type=None),
            make_source=lambda context: FakeSource(context, []),
            open_engine=broken_engine,
            config=config,
        )
