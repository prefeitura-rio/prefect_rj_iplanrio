from datetime import datetime, timedelta

import pytest

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import SHEET_COLUMNS, SP_TZ
from pipelines.rj_iplanrio__cor_alerts_aggregator.helpers.rows import (
    build_cluster_row,
    build_sent_rows,
    number_lines,
    plan_sheet_update,
    read_sheet_time,
)
from pipelines.rj_iplanrio__cor_alerts_aggregator.tests.conftest import ClusterFactory
from pipelines.rj_iplanrio__cor_alerts_aggregator.types import Row

NOW = datetime(2026, 7, 17, 12, 0, tzinfo=SP_TZ)
HEADER = list(SHEET_COLUMNS.values())


def sheet_line(group: str, created: datetime) -> list[str]:
    """Build a line of the sheet for a group, made at a time."""
    line = [""] * len(HEADER)
    line[HEADER.index("id_grupo")] = group
    line[HEADER.index("data_criacao")] = created.strftime("%d/%m/%Y %H:%M:%S")

    return line


def cluster_row(make_cluster: ClusterFactory, group: str) -> Row:
    """Build the row of a cluster for a group."""
    return build_cluster_row(make_cluster(2), group_id=group, event_id="e", environment="prod", now=NOW)


def test_blank_values_get_a_placeholder_in_numbered_lines() -> None:
    """Number from one and trim each value."""
    assert number_lines(["Rua A", "  ", "Rua C "], "none") == "1. Rua A\n2. none\n3. Rua C"
    assert number_lines(["", ""], "") == "1.\n2."


def test_the_row_of_a_cluster_has_the_columns_of_the_table(make_cluster: ClusterFactory) -> None:
    """Fill the sheet columns, the priority and the partition."""
    row = cluster_row(make_cluster, "g1")

    assert all(column in row for column in SHEET_COLUMNS)
    assert row["enderecos_ocorrencias"] == "1. street 0\n2. street 1"
    assert (row["priority"], row["alert_count"], row["created_date"], row["data_particao"]) == (
        "02",
        2,
        "17/07/2026 12:00:00",
        "2026-07-17",
    )


def test_each_alert_of_a_group_gets_a_sent_row() -> None:
    """Return one row for each id, with the group and the time."""
    rows = build_sent_rows(["a", "b"], group_id="g1", environment="staging", now=NOW)

    assert [(row["alert_id"], row["aggregation_group_id"], row["sent_at"]) for row in rows] == [
        ("a", "g1", "2026-07-17 12:00:00"),
        ("b", "g1", "2026-07-17 12:00:00"),
    ]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("17/07/2026 12:00:00", NOW, id="a_time"),
        pytest.param("yesterday", None, id="not_a_time"),
        pytest.param("", None, id="empty"),
    ],
)
def test_a_cell_of_the_sheet_is_a_time_or_nothing(text: str, expected: datetime | None) -> None:
    """Read the time in São Paulo."""
    assert read_sheet_time(text) == expected


def test_an_empty_sheet_gets_the_header_and_all_the_rows(make_cluster: ClusterFactory) -> None:
    """Write the header, then append the rows."""
    plan = plan_sheet_update([], [cluster_row(make_cluster, "g1")], NOW)

    assert (plan.clear, plan.header, len(plan.appends), plan.updates, plan.stale_rows) == (False, True, 1, {}, [])


def test_a_sheet_with_another_header_is_cleared_first(make_cluster: ClusterFactory) -> None:
    """Rebuild a sheet whose columns changed."""
    plan = plan_sheet_update([["old", "header"], ["x", "y"]], [cluster_row(make_cluster, "g1")], NOW)

    assert (plan.clear, plan.header, len(plan.appends), plan.stale_rows) == (True, True, 1, [])


def test_the_stale_lines_leave_known_groups_update_and_new_groups_append(make_cluster: ClusterFactory) -> None:
    """Number each update by its row after the stale rows leave."""
    existing = [
        HEADER,
        sheet_line("old", NOW - timedelta(hours=30)),
        sheet_line("g1", NOW - timedelta(hours=1)),
        ["too short"],
        sheet_line("g2", NOW - timedelta(hours=2)),
    ]

    plan = plan_sheet_update(
        existing, [cluster_row(make_cluster, "g2"), cluster_row(make_cluster, "g3")], NOW - timedelta(hours=24)
    )

    assert plan.stale_rows == [2]
    assert (plan.clear, plan.header) == (False, False)
    assert list(plan.updates) == [4]
    assert plan.updates[4][HEADER.index("id_grupo")] == "g2"
    assert [line[HEADER.index("id_grupo")] for line in plan.appends] == ["g3"]
