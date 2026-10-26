import pytest

from toolkit.bigquery import BigQuery, build_source_uri


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        pytest.param(3, 3, id="a_statement_that_changes_rows"),
        pytest.param(None, 0, id="a_statement_with_no_count"),
    ],
)
async def test_a_statement_waits_for_its_job_and_returns_the_rows_it_changed(
    fake_bigquery_client, rows, expected
) -> None:
    """Wait for the job, and return its count, or 0 when it has none."""
    client = fake_bigquery_client(rows)

    changed = await BigQuery(client=client).execute("DELETE FROM t WHERE TRUE")

    assert changed == expected
    [(sql, job)] = client.jobs
    assert sql == "DELETE FROM t WHERE TRUE"
    assert job.waited


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        pytest.param("gcs://b/staging/ds/t", "gs://b/staging/ds/t", id="a_gcs_uri"),
        pytest.param("gs://b/staging/ds/t", "gs://b/staging/ds/t", id="a_gs_uri_stays"),
        pytest.param(
            "gcs://gcs://odd", "gs://gcs://odd", id="only_the_scheme_at_the_start"
        ),
    ],
)
def test_bigquery_reads_a_gcs_uri_as_gs(uri, expected) -> None:
    """Give BigQuery the `gs://` form of the URI, the only one it accepts."""
    assert build_source_uri(uri) == expected


@pytest.mark.parametrize(
    ("table_type", "source_format", "calls"),
    [
        pytest.param(None, None, ["create"], id="no_table_is_created"),
        pytest.param(
            "EXTERNAL", "DELTA_LAKE", ["update"], id="a_delta_table_is_refreshed"
        ),
        pytest.param(
            "EXTERNAL", "CSV", ["delete", "create"], id="a_csv_table_is_replaced"
        ),
        pytest.param(
            "EXTERNAL",
            "PARQUET",
            ["delete", "create"],
            id="a_parquet_table_is_replaced",
        ),
    ],
)
async def test_a_delta_table_replaces_an_external_table_of_another_format(
    fake_bigquery_tables, table_type, source_format, calls
) -> None:
    """Create a table, refresh a Delta one, and replace any other external one."""
    client = fake_bigquery_tables(table_type, source_format)

    await BigQuery(client=client).sync_delta_table(
        "p.d.t", "gcs://b/staging/d/t", "p.us.conn"
    )

    assert client.calls == calls


async def test_a_delta_table_never_replaces_a_native_table(
    fake_bigquery_tables,
) -> None:
    """Refuse a native table, because its rows live only in BigQuery."""
    client = fake_bigquery_tables("TABLE", None)

    with pytest.raises(ValueError, match="native"):
        await BigQuery(client=client).sync_delta_table(
            "p.d.t", "gcs://b/staging/d/t", "p.us.conn"
        )

    assert client.calls == []
