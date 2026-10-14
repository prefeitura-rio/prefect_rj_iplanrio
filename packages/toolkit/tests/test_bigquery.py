import pytest

from toolkit.bigquery import BigQuery


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
