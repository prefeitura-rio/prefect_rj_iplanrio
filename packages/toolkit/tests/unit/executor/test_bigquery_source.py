from datetime import date

import pytest

from toolkit.executor.engines.duckdb import Extension
from toolkit.executor.sources.backends.bigquery import BigQuerySource
from toolkit.partitions import AllSelection, FromDateSelection, TimeRangeSelection

QUERY = "SELECT * FROM `p.d.t`"


@pytest.mark.parametrize(
    ("selection", "expected"),
    [
        pytest.param(
            AllSelection(), "SELECT * FROM (SELECT * FROM `p.d.t`) AS src", id="all"
        ),
        pytest.param(
            FromDateSelection(column="dt", lower=date(2024, 1, 1)),
            "SELECT * FROM (SELECT * FROM `p.d.t`) AS src\nWHERE CAST(dt AS DATE) >= DATE('2024-01-01')",
            id="from_date",
        ),
        pytest.param(
            TimeRangeSelection(
                column="dt", lower=date(2024, 1, 1), upper=date(2024, 2, 1)
            ),
            "SELECT * FROM (SELECT * FROM `p.d.t`) AS src\n"
            "WHERE CAST(dt AS DATE) >= DATE('2024-01-01')\n"
            "    AND CAST(dt AS DATE) < DATE('2024-02-01')",
            id="time_range",
        ),
    ],
)
def test_window_query(selection, expected, make_context):
    assert (
        BigQuerySource.from_context(make_context()).window(QUERY, selection).strip()
        == expected
    )


def test_scan_returns_bigquery_query_for_the_project():
    scan = BigQuerySource(project="my-proj").scan("SELECT 'a'")
    assert scan.as_string(None) == "bigquery_query('my-proj', 'SELECT ''a''')"


def test_it_needs_the_community_extension_and_no_setup():
    source = BigQuerySource()
    assert source.extensions == (Extension("bigquery", community=True),)
    assert source.setup() is None


def test_the_factory_needs_no_connection_data(make_context):
    assert BigQuerySource.from_context(make_context(conn=None)).name == "bigquery"
