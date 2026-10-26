"""Clusters of alerts, and the events they become."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from math import asin, cos, radians, sin, sqrt

from pydantic import JsonValue, TypeAdapter

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import (
    ALERT_TYPE_CODES,
    DEFAULT_PRIORITY,
    EARTH_RADIUS_METERS,
    EVENT_TYPE_LIMIT,
    PRIORITIES,
    REPORTS_SHOWN,
    SP_TZ,
)
from pipelines.rj_iplanrio__cor_alerts_aggregator.types import Alert, Cluster

ALERTS: TypeAdapter[list[Alert]] = TypeAdapter(list[Alert])


def read_alerts(rows: Sequence[Mapping[str, object]]) -> list[Alert]:
    """Check the rows of the query and return them as alerts.

    Raise `ValidationError` if a row lacks a column or has a value of the wrong type.
    """
    return ALERTS.validate_python(rows)


def distance_meters(first: tuple[float, float], second: tuple[float, float]) -> float:
    """Return the Earth surface distance between two latitude and longitude points."""
    lat1, lng1, lat2, lng2 = map(radians, (*first, *second))
    half = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lng2 - lng1) / 2) ** 2

    return 2 * EARTH_RADIUS_METERS * asin(sqrt(half))


def link_components(alerts: Sequence[Alert], radius_meters: float) -> list[list[Alert]]:
    """Group the alerts so that each is within the radius of one of its group.

    A chain of alerts counts.
    """
    parent = list(range(len(alerts)))

    def root(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]

        return index

    for first in range(len(alerts)):
        for second in range(first + 1, len(alerts)):
            near = distance_meters(
                (alerts[first]["latitude"], alerts[first]["longitude"]),
                (alerts[second]["latitude"], alerts[second]["longitude"]),
            )

            if near <= radius_meters:
                parent[root(second)] = root(first)

    groups: dict[int, list[Alert]] = defaultdict(list)

    for index, alert in enumerate(alerts):
        groups[root(index)].append(alert)

    return list(groups.values())


def group_alerts(alerts: Sequence[Alert], radius_meters: float) -> list[Cluster]:
    """Cluster the alerts of each type, and return them by type and oldest alert.

    The alerts of a cluster are in order of time. Its severity is `critica` if one alert
    has it, and its neighborhood is the first one that an alert has.
    """
    by_type: dict[str, list[Alert]] = defaultdict(list)

    for alert in alerts:
        by_type[alert["alert_type"]].append(alert)

    clusters: list[Cluster] = []

    for alert_type in sorted(by_type):
        groups = [
            sorted(group, key=lambda alert: (alert["created_at"], alert["alert_id"]))
            for group in link_components(by_type[alert_type], radius_meters)
        ]

        for number, group in enumerate(sorted(groups, key=lambda group: group[0]["created_at"])):
            clusters.append(
                Cluster(
                    cluster_id=number,
                    alert_type=alert_type,
                    alert_ids=[alert["alert_id"] for alert in group],
                    oldest_alert=group[0]["created_at"],
                    centroid_lat=sum(alert["latitude"] for alert in group) / len(group),
                    centroid_lng=sum(alert["longitude"] for alert in group) / len(group),
                    addresses=[alert["address"] for alert in group],
                    descriptions=[alert["description"] for alert in group],
                    user_ids=[alert["user_id"] for alert in group],
                    neighborhood=next((alert["neighborhood"] for alert in group if alert["neighborhood"]), ""),
                    severity="critica" if any(alert["severity"] == "critica" for alert in group) else "alta",
                )
            )

    return clusters


def should_send_cluster(
    cluster: Cluster, immediate_threshold: int, window_minutes: int, now: datetime
) -> tuple[bool, str]:
    """Tell whether a cluster goes out now, and why.

    It goes out when it has `immediate_threshold` alerts, or when its oldest alert is
    `window_minutes` old. Until then it waits for more alerts. A time without a zone is
    in São Paulo.
    """
    oldest = cluster.oldest_alert if cluster.oldest_alert.tzinfo else cluster.oldest_alert.replace(tzinfo=SP_TZ)
    age = (now - oldest).total_seconds() / 60

    if cluster.alert_count >= immediate_threshold:
        return True, f"Limit reached ({cluster.alert_count} of {immediate_threshold})"

    if age >= window_minutes:
        return True, f"Window over ({age:.1f} minutes) with {cluster.alert_count} alert(s)"

    return False, f"Waiting: {cluster.alert_count} alert(s), {window_minutes - age:.1f} minutes left in the window"


def describe_event(cluster: Cluster, prefix: str = "") -> str:
    """Return the type of the event of a cluster: its code and report addresses.

    One report reads `CODE: report [address]`. Many read `CODE: (1) report [address] |
    (2) ...` for the first five, with the count of the rest. The text has at most 500
    characters.
    """
    code = prefix + ALERT_TYPE_CODES.get(cluster.alert_type, cluster.alert_type.upper())

    if len(cluster.descriptions) == 1:
        address = cluster.addresses[0][:80] if cluster.addresses else ""

        return f"{code}: {cluster.descriptions[0][:150]} [{address}]"[:EVENT_TYPE_LIMIT]

    reports = " | ".join(
        f"({number}) {text[:60]} [{cluster.addresses[number - 1][:40] if number <= len(cluster.addresses) else ''}]"
        for number, text in enumerate(cluster.descriptions[:REPORTS_SHOWN], start=1)
    )

    if len(cluster.descriptions) > REPORTS_SHOWN:
        reports += f" | (+{len(cluster.descriptions) - REPORTS_SHOWN} relatos)"

    return f"{code}: {reports}"[:EVENT_TYPE_LIMIT]


def build_event(cluster: Cluster, event_id: str, created: datetime, prefix: str = "") -> dict[str, JsonValue]:
    """Return the event of a cluster, in the fields that the COR API accepts."""
    return {
        "EventId": event_id,
        "Location": cluster.addresses[0] if cluster.addresses else "",
        "Priority": PRIORITIES.get(cluster.severity, DEFAULT_PRIORITY),
        "AgencyEventTypeCode": describe_event(cluster, prefix),
        "CreatedDate": created.strftime("%Y-%m-%d %H:%M:%S"),
        "Latitude": cluster.centroid_lat,
        "Longitude": cluster.centroid_lng,
    }
