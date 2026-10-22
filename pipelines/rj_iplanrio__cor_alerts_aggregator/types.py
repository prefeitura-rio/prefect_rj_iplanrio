"""Types of rj_iplanrio__cor_alerts_aggregator."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, NotRequired, TypedDict

from pydantic import JsonValue

type Row = dict[str, JsonValue]
type Destination = Literal["cor_api", "google_sheets", "dry_run"]
type Environment = Literal["staging", "prod"]


class Alert(TypedDict):
    """An alert of a person that waits to be sent."""

    alert_id: str
    user_id: str
    alert_type: str
    severity: str
    description: str
    address: str
    latitude: float
    longitude: float
    neighborhood: str
    created_at: datetime


@dataclass(frozen=True)
class Cluster:
    """The alerts of one type that lie within the radius of each other, from the oldest alert on."""

    cluster_id: int
    alert_type: str
    alert_ids: list[str]
    oldest_alert: datetime
    centroid_lat: float
    centroid_lng: float
    addresses: list[str]
    descriptions: list[str]
    user_ids: list[str]
    neighborhood: str
    severity: str

    @property
    def alert_count(self) -> int:
        """Return how many alerts the cluster has."""
        return len(self.alert_ids)


class Rules(TypedDict, total=False):
    """The rules that group the alerts and decide when a cluster goes out."""

    radius_meters: int
    window_minutes: int
    immediate_threshold: int


class Sheet(TypedDict):
    """The Google Sheet that gets the clusters. The rows older than `retention_hours` leave it."""

    spreadsheet_id: str
    retention_hours: NotRequired[int]


@dataclass(frozen=True)
class SheetPlan:
    """The writes that bring the sheet up to date."""

    clear: bool
    header: bool
    stale_rows: list[int]
    updates: dict[int, list[str]]
    appends: list[list[str]]
