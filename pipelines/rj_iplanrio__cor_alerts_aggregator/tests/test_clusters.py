from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import SP_TZ
from pipelines.rj_iplanrio__cor_alerts_aggregator.helpers.clusters import (
    build_event,
    describe_event,
    distance_meters,
    group_alerts,
    read_alerts,
    should_send_cluster,
)
from pipelines.rj_iplanrio__cor_alerts_aggregator.tests.conftest import AlertFactory, ClusterFactory

NOW = datetime(2026, 7, 17, 12, 0, tzinfo=SP_TZ)


def test_a_degree_of_latitude_is_about_111_kilometers() -> None:
    """Measure on the surface of the Earth."""
    assert distance_meters((0, 0), (1, 0)) == pytest.approx(111_195, rel=0.001)


def test_the_alerts_within_the_radius_form_one_cluster_even_through_a_chain(make_alert: AlertFactory) -> None:
    """Join a to c through b, though a and c are farther apart than the radius."""
    alerts = [make_alert("a"), make_alert("b", latitude=-22.9 - 0.0035), make_alert("c", latitude=-22.9 - 0.007)]

    [cluster] = group_alerts(alerts, 500)

    assert cluster.alert_ids == ["a", "b", "c"]
    assert cluster.centroid_lat == pytest.approx(-22.9035)


def test_the_alerts_of_another_type_or_far_away_form_other_clusters(make_alert: AlertFactory) -> None:
    """Never mix types, and keep far places apart."""
    alerts = [make_alert("a"), make_alert("b", alert_type="enchente"), make_alert("c", latitude=-23.5)]

    clusters = group_alerts(alerts, 500)

    assert [(cluster.alert_type, cluster.alert_ids) for cluster in clusters] == [
        ("alagamento", ["a"]),
        ("alagamento", ["c"]),
        ("enchente", ["b"]),
    ]


def test_a_cluster_keeps_its_alerts_in_time_order_with_the_worst_severity(make_alert: AlertFactory) -> None:
    """Order by time then id, take `critica` if any alert has it, and the first neighborhood."""
    early = NOW - timedelta(minutes=30)
    alerts = [
        make_alert("b", created_at=NOW),
        make_alert("a", created_at=early, severity="critica"),
        make_alert("c", created_at=early, neighborhood="acari"),
    ]

    [cluster] = group_alerts(alerts, 500)

    assert cluster.alert_ids == ["a", "c", "b"]
    assert (cluster.oldest_alert, cluster.severity, cluster.neighborhood) == (early, "critica", "acari")


def test_no_alerts_give_no_clusters() -> None:
    """Return an empty list."""
    assert group_alerts([], 500) == []


@pytest.mark.parametrize(
    ("count", "age", "send", "reason"),
    [
        pytest.param(5, 1, True, "Limit reached", id="the_limit_is_reached"),
        pytest.param(6, 1, True, "Limit reached", id="over_the_limit"),
        pytest.param(2, 7, True, "Window over", id="the_window_is_over"),
        pytest.param(2, 6.9, False, "Waiting", id="the_window_is_open"),
    ],
)
def test_a_cluster_goes_out_at_the_limit_or_when_the_window_ends(
    count: int, age: float, send: bool, reason: str, make_cluster: ClusterFactory
) -> None:
    """Send at 5 alerts or after 7 minutes, whichever comes first."""
    decided, why = should_send_cluster(make_cluster(count, age), 5, 7, NOW)

    assert decided is send
    assert reason in why


def test_a_time_without_a_zone_counts_in_sao_paulo(make_cluster: ClusterFactory) -> None:
    """Read the oldest alert as the time of São Paulo."""
    cluster = make_cluster(2, oldest_alert=NOW.replace(tzinfo=None) - timedelta(minutes=8))

    assert should_send_cluster(cluster, 5, 7, NOW)[0] is True


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        pytest.param({}, "ALAGAMENTO: report 0 [street 0]", id="one_report"),
        pytest.param({"addresses": []}, "ALAGAMENTO: report 0 []", id="one_report_without_address"),
        pytest.param({"alert_type": "other"}, "OTHER: report 0 [street 0]", id="an_unknown_type"),
    ],
)
def test_the_event_of_one_report_has_the_code_the_report_and_the_address(
    changes: dict[str, object], expected: str, make_cluster: ClusterFactory
) -> None:
    """Describe a single report."""
    assert describe_event(make_cluster(**changes)) == expected


def test_the_event_of_many_reports_lists_five_and_counts_the_rest(make_cluster: ClusterFactory) -> None:
    """Number the reports and cut the long ones."""
    cluster = make_cluster(
        7, descriptions=[f"report {number} " + "x" * 100 for number in range(7)], addresses=["a" * 90, "b"]
    )

    text = describe_event(cluster, "[T] ")

    assert text.startswith("[T] ALAGAMENTO: (1) report 0 " + "x" * 51 + " [" + "a" * 40 + "] | (2) report 1")
    assert "(5) " in text
    assert "(6) " not in text
    assert text.endswith(" | (+2 relatos)")
    assert len(text) <= 500


def test_the_event_has_the_fields_that_the_cor_api_accepts(make_cluster: ClusterFactory) -> None:
    """Build the payload with the priority of the severity."""
    event = build_event(make_cluster(severity="critica"), "event-1", NOW)

    assert event == {
        "EventId": "event-1",
        "Location": "street 0",
        "Priority": "01",
        "AgencyEventTypeCode": "ALAGAMENTO: report 0 [street 0]",
        "CreatedDate": "2026-07-17 12:00:00",
        "Latitude": -22.9,
        "Longitude": -43.2,
    }


def test_the_rows_of_the_query_become_alerts() -> None:
    """Validate the type of each column."""
    row = {
        "alert_id": "a",
        "user_id": "u",
        "alert_type": "alagamento",
        "severity": "alta",
        "description": "d",
        "address": "x",
        "latitude": -22.9,
        "longitude": -43.2,
        "neighborhood": "acari",
        "created_at": datetime(2026, 7, 17, 11, 50),
    }

    assert read_alerts([row])[0]["created_at"] == datetime(2026, 7, 17, 11, 50)

    with pytest.raises(ValidationError):
        read_alerts([{**row, "latitude": "far"}])
