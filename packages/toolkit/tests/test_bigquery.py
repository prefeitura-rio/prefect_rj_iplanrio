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
):
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
def test_bigquery_reads_a_gcs_uri_as_gs(uri, expected):
    """Give BigQuery the `gs://` form of the URI, the only one it accepts."""
    assert build_source_uri(uri) == expected
