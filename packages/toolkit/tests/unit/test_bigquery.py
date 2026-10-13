import pytest
from google.api_core.exceptions import NotFound

from toolkit.bigquery import BigQuery


class FakeClient:
    def __init__(self, *, exists: bool) -> None:
        self.exists = exists
        self.created = []
        self.deleted = []

    def get_table(self, table: str):
        if not self.exists:
            raise NotFound("missing")
        return table

    def delete_table(self, table: str, *, not_found_ok: bool) -> None:
        self.deleted.append((table, not_found_ok))

    def create_table(self, table) -> None:
        self.created.append(table)


@pytest.mark.parametrize(
    ("exists", "expected"),
    [pytest.param(True, True, id="found"), pytest.param(False, False, id="not_found")],
)
async def test_table_exists(exists, expected):
    assert await BigQuery(FakeClient(exists=exists)).table_exists("p.d.t") is expected  # pyright: ignore[reportArgumentType]


async def test_external_table_is_recreated_with_hive_partitioning_and_connection():
    client = FakeClient(exists=True)
    await BigQuery(client).create_external_table(  # pyright: ignore[reportArgumentType]
        "p.d_staging.t",
        "gs://b/staging/d/t",
        "PARQUET",
        connection_id="projects/p/connections/c",
    )
    config = client.created[0].external_data_configuration
    assert client.deleted == [("p.d_staging.t", True)]
    assert config.source_uris == ["gs://b/staging/d/t/*"]
    assert config.connection_id == "projects/p/connections/c"
    assert config.hive_partitioning.source_uri_prefix == "gs://b/staging/d/t/"
    assert config.autodetect is True
