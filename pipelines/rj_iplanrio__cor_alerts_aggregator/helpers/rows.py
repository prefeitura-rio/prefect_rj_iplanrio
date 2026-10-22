"""Rows of the aggregated table, the sent table and the sheet."""

from collections.abc import Sequence
from datetime import datetime

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import (
    DEFAULT_PRIORITY,
    PRIORITIES,
    SHEET_COLUMNS,
    SHEET_TIME_FORMAT,
    SP_TZ,
)
from pipelines.rj_iplanrio__cor_alerts_aggregator.helpers.clusters import describe_event
from pipelines.rj_iplanrio__cor_alerts_aggregator.types import Cluster, Environment, Row, SheetPlan


def number_lines(values: Sequence[str], placeholder: str) -> str:
    """Return the values as numbered lines, with the placeholder for a blank one.

    ```python
    number_lines(["Rua A", " "], "none")  # "1. Rua A\\n2. none"
    ```
    """
    return "\n".join(
        f"{number}. {value.strip() or placeholder}".rstrip() for number, value in enumerate(values, start=1)
    )


def build_cluster_row(
    cluster: Cluster, *, group_id: str, event_id: str, environment: Environment, now: datetime
) -> Row:
    """Return the row of a cluster for the aggregated table, which also fills the sheet."""
    return {
        "aggregation_group_id": group_id,
        "event_id": event_id,
        "bairro": cluster.neighborhood,
        "location": cluster.addresses[0] if cluster.addresses else "",
        "priority": PRIORITIES.get(cluster.severity, DEFAULT_PRIORITY),
        "agency_event_type_code": describe_event(cluster),
        "enderecos_ocorrencias": number_lines(cluster.addresses, "endereco nao informado"),
        "telefones_ocorrencias": number_lines(cluster.user_ids, "telefone nao informado"),
        "created_date": now.strftime(SHEET_TIME_FORMAT),
        "latitude": cluster.centroid_lat,
        "longitude": cluster.centroid_lng,
        "alert_ids": ",".join(cluster.alert_ids),
        "alert_count": cluster.alert_count,
        "alert_type": cluster.alert_type,
        "severity": cluster.severity,
        "environment": environment,
        "data_particao": now.date().isoformat(),
    }


def build_sent_rows(alert_ids: Sequence[str], *, group_id: str, environment: Environment, now: datetime) -> list[Row]:
    """Return the rows of the sent table: one for each alert that went out in a group."""
    return [
        {
            "alert_id": alert_id,
            "aggregation_group_id": group_id,
            "sent_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "environment": environment,
            "data_particao": now.date().isoformat(),
        }
        for alert_id in alert_ids
    ]


def read_sheet_time(text: str) -> datetime | None:
    """Return the time of a cell of the sheet as a time in São Paulo, or nothing if it is not a time."""
    try:
        return datetime.strptime(text, SHEET_TIME_FORMAT).replace(tzinfo=SP_TZ)
    except ValueError:
        return None


def plan_sheet_update(existing: Sequence[Sequence[str]], rows: Sequence[Row], cutoff: datetime) -> SheetPlan:
    """Return the writes that bring the sheet up to date with the rows of the clusters.

    An empty sheet gets the header. A sheet with another header is cleared first. Otherwise the
    rows made before `cutoff` leave, a row whose group is in the sheet gets updated, and the rest
    go to the end. Each update has the number of its row after the stale rows leave.
    """
    header = list(SHEET_COLUMNS.values())
    cells = [[str(row.get(column, "")) for column in SHEET_COLUMNS] for row in rows]

    if not existing:
        return SheetPlan(clear=False, header=True, stale_rows=[], updates={}, appends=cells)

    if list(existing[0]) != header:
        return SheetPlan(clear=True, header=True, stale_rows=[], updates={}, appends=cells)

    time_index = header.index("data_criacao")
    group_index = header.index("id_grupo")
    stale: list[int] = []
    kept: list[Sequence[str]] = []

    for number, line in enumerate(existing[1:], start=2):
        moment = read_sheet_time(line[time_index]) if len(line) > time_index else None

        if moment is not None and moment < cutoff:
            stale.append(number)
        else:
            kept.append(line)

    positions = {line[group_index]: number for number, line in enumerate(kept, start=2) if len(line) > group_index}
    updates: dict[int, list[str]] = {}
    appends: list[list[str]] = []
    group_column = list(SHEET_COLUMNS).index("aggregation_group_id")

    for line in cells:
        position = positions.get(line[group_column])

        if position is None:
            appends.append(line)
        else:
            updates[position] = line

    return SheetPlan(clear=False, header=False, stale_rows=stale, updates=updates, appends=appends)
