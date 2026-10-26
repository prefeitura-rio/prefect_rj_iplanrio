from collections.abc import Callable
from datetime import datetime, timedelta

import pytest

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import SP_TZ
from pipelines.rj_iplanrio__cor_alerts_aggregator.types import Alert, Cluster

type AlertFactory = Callable[..., Alert]
type ClusterFactory = Callable[..., Cluster]

NOW = datetime(2026, 7, 17, 12, 0, tzinfo=SP_TZ)


@pytest.fixture
def make_alert() -> AlertFactory:
    """Return a factory of alerts at a place, ten minutes old, changeable by field."""

    def build(alert_id: str, latitude: float = -22.9, longitude: float = -43.2, **changes: object) -> Alert:
        alert = {
            "alert_id": alert_id,
            "user_id": f"user-{alert_id}",
            "alert_type": "alagamento",
            "severity": "alta",
            "description": f"report {alert_id}",
            "address": f"street {alert_id}",
            "latitude": latitude,
            "longitude": longitude,
            "neighborhood": "",
            "created_at": NOW - timedelta(minutes=10),
        } | changes

        return Alert(**alert)  # pyright: ignore[reportArgumentType]

    return build


@pytest.fixture
def make_cluster() -> ClusterFactory:
    """Return a factory of `count` alerts with an oldest alert of the age.

    It accepts changes for any field.
    """

    def build(count: int = 1, age_minutes: float = 1, **changes: object) -> Cluster:
        base = {
            "cluster_id": 0,
            "alert_type": "alagamento",
            "alert_ids": [f"a{number}" for number in range(count)],
            "oldest_alert": NOW - timedelta(minutes=age_minutes),
            "centroid_lat": -22.9,
            "centroid_lng": -43.2,
            "addresses": [f"street {number}" for number in range(count)],
            "descriptions": [f"report {number}" for number in range(count)],
            "user_ids": [f"user-{number}" for number in range(count)],
            "neighborhood": "acari",
            "severity": "alta",
        } | changes

        return Cluster(**base)  # pyright: ignore[reportArgumentType]

    return build
