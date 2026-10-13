from dataclasses import dataclass, field
from typing import ClassVar, cast

import pytest
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud.bigquery import Client, Table
from pydantic import BaseModel, ValidationError

from toolkit import bigquery
from toolkit.bigquery import BigQuery


@dataclass
class FakeResult:
    rows: list[dict[str, object]]

    def result(self) -> list[dict[str, object]]:
        return self.rows


@dataclass
class FakeClient:
    """The methods that ``toolkit.bigquery`` calls, recording every call in order."""

    instances: ClassVar[list["FakeClient"]] = []

    project: str | None = None
    exists: bool = True
    error: Exception | None = None
    rows: list[dict[str, object]] = field(default_factory=list)
    closed: bool = False
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = field(
        default_factory=list
    )
    created: list[Table] = field(default_factory=list)

    def __post_init__(self) -> None:
        FakeClient.instances.append(self)

    def record(self, name: str, *args: object, **kwargs: object) -> None:
        self.calls.append((name, args, kwargs))

    def get_table(self, table: str) -> str:
        self.record("get_table", table)
        if self.error:
            raise self.error
        if not self.exists:
            raise NotFound("missing")
        return table

    def create_dataset(self, dataset: str, **kwargs: object) -> None:
        self.record("create_dataset", dataset, **kwargs)

    def delete_dataset(self, dataset: str, **kwargs: object) -> None:
        self.record("delete_dataset", dataset, **kwargs)

    def delete_table(self, table: str, **kwargs: object) -> None:
        self.record("delete_table", table, **kwargs)

    def create_table(self, table: Table) -> None:
        self.record("create_table", table)
        self.created.append(table)

    def query(self, sql: str) -> FakeResult:
        self.record("query", sql)
        return FakeResult(self.rows)

    def close(self) -> None:
        self.closed = True


def wrap(client: FakeClient) -> BigQuery:
    """Wrap the fake in the real class; the fake has only what the toolkit calls."""
    return BigQuery(cast("Client", cast("object", client)))


@pytest.fixture
def fake_client_class(monkeypatch):
    """Replace the Google client class, so ``connect`` builds a fake."""
    FakeClient.instances = []
    monkeypatch.setattr(bigquery, "Client", FakeClient)
    return FakeClient


@pytest.mark.parametrize(
    ("argument", "setting", "expected"),
    [
        pytest.param("arg", "setting", "arg", id="argument_wins"),
        pytest.param(None, "setting", "setting", id="setting_when_no_argument"),
        pytest.param("", "setting", "setting", id="empty_argument_uses_the_setting"),
        pytest.param(None, "", None, id="empty_setting_means_adc_default_project"),
        pytest.param("", "", None, id="both_empty_means_adc_default_project"),
    ],
)
async def test_connect_chooses_the_project(
    argument, setting, expected, fake_client_class, monkeypatch
):
    """Regression: an empty setting used to reach the client as ``""`` (HTTP 404)."""
    monkeypatch.setattr(bigquery.settings, "BIGQUERY_PROJECT", setting)
    async with BigQuery.connect(argument) as client:
        assert isinstance(client.client, fake_client_class)
        assert client.client.project == expected


async def test_connect_closes_the_client_at_the_end(fake_client_class):
    async with BigQuery.connect("p") as client:
        assert isinstance(client.client, FakeClient)
        assert client.client.closed is False
    assert fake_client_class.instances[0].closed is True


async def test_connect_closes_the_client_when_the_body_raises(fake_client_class):
    with pytest.raises(RuntimeError, match="boom"):
        async with BigQuery.connect("p"):
            raise RuntimeError("boom")
    assert fake_client_class.instances[0].closed is True


@pytest.mark.parametrize(
    ("method", "call", "expected_kwargs"),
    [
        pytest.param(
            "create_dataset", "create_dataset", {"exists_ok": True}, id="create_dataset"
        ),
        pytest.param(
            "delete_dataset",
            "delete_dataset",
            {"delete_contents": True, "not_found_ok": True},
            id="delete_dataset",
        ),
        pytest.param(
            "drop_table", "delete_table", {"not_found_ok": True}, id="drop_table"
        ),
    ],
)
async def test_idempotent_operations_pass_the_flags_that_make_them_safe(
    method, call, expected_kwargs
):
    client = FakeClient()
    await getattr(wrap(client), method)("p.d")
    assert client.calls == [(call, ("p.d",), expected_kwargs)]


@pytest.mark.parametrize(
    ("client", "expected"),
    [
        pytest.param(FakeClient(exists=True), True, id="found"),
        pytest.param(FakeClient(exists=False), False, id="not_found"),
    ],
)
async def test_table_exists(client, expected):
    assert await wrap(client).table_exists("p.d.t") is expected


async def test_table_exists_does_not_hide_other_errors():
    with pytest.raises(Forbidden):
        await wrap(FakeClient(error=Forbidden("no access"))).table_exists("p.d.t")


@pytest.mark.parametrize(
    ("source_format", "connection_id"),
    [
        pytest.param("PARQUET", None, id="parquet_plain"),
        pytest.param("CSV", None, id="csv_plain"),
        pytest.param("PARQUET", "projects/p/connections/c", id="parquet_biglake"),
    ],
)
async def test_external_table_configuration(source_format, connection_id):
    client = FakeClient()
    await wrap(client).create_external_table(
        "p.d_staging.t",
        "gs://b/staging/d/t",
        source_format,
        connection_id=connection_id,
    )

    config = client.created[0].external_data_configuration
    assert config is not None
    hive = config.hive_partitioning
    assert hive is not None
    assert config.source_format == source_format
    assert config.source_uris == ["gs://b/staging/d/t/*"]
    assert config.autodetect is True
    assert hive.mode == "AUTO"
    assert hive.source_uri_prefix == "gs://b/staging/d/t/"
    assert config.connection_id == connection_id


async def test_external_table_is_replaced_not_updated():
    client = FakeClient()
    await wrap(client).create_external_table("p.d.t", "gs://b/x", "PARQUET")

    assert [name for name, _, _ in client.calls] == ["delete_table", "create_table"]
    assert client.calls[0] == ("delete_table", ("p.d.t",), {"not_found_ok": True})
    created = client.created[0]
    assert (created.project, created.dataset_id, created.table_id) == ("p", "d", "t")


class CountRow(BaseModel):
    n: int


async def test_query_returns_validated_models():
    client = FakeClient(rows=[{"n": 3}, {"n": 4}])
    rows = await wrap(client).query("SELECT n FROM t", expect=CountRow)
    assert rows == [CountRow(n=3), CountRow(n=4)]
    assert client.calls == [("query", ("SELECT n FROM t",), {})]


async def test_query_rejects_a_row_that_does_not_fit_the_model():
    client = FakeClient(rows=[{"n": "not a number"}])
    with pytest.raises(ValidationError):
        await wrap(client).query("SELECT n FROM t", expect=CountRow)


async def test_query_returns_an_empty_list_for_no_rows():
    assert await wrap(FakeClient()).query("SELECT n FROM t", expect=CountRow) == []
