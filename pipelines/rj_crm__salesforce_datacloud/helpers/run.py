"""Plan a run and add up what it did."""

from collections.abc import Sequence
from datetime import datetime

from pipelines.rj_crm__salesforce_datacloud.constants import FEEDS
from pipelines.rj_crm__salesforce_datacloud.helpers.window import plan_spans
from pipelines.rj_crm__salesforce_datacloud.types import Outcome, RunPlan, Totals, Window


def plan_run(window: Window, tables: Sequence[str] | None, now: datetime, started: float) -> RunPlan:
    """Pick the feeds and the windows of a run, and the label that names it in the logs.

    All the feeds run if `tables` is empty. Raise `ValueError` for a table that is not a
    feed and for a reprocess that does not make sense.
    """
    unknown = set(tables or []) - {feed.name for feed in FEEDS}

    if unknown:
        raise ValueError(f"Unknown tables: {sorted(unknown)}")

    feeds = tuple(feed for feed in FEEDS if not tables or feed.name in tables)
    spans = tuple(plan_spans(window, now))
    reprocess = "reprocess_from" in window
    label = (
        f"reprocessamento {spans[0].partition_date} a {spans[-1].partition_date} ({len(spans)} dia(s))"
        if reprocess
        else f"modo='{window.get('mode', 'hour')}'"
    )

    return RunPlan(
        feeds=feeds,
        spans=spans,
        label=label,
        summarize=reprocess or window.get("mode") == "day",
        started=started,
    )


def add_up(outcomes: Sequence[Outcome]) -> Totals:
    """Sum the rows of each feed by group, and list the feeds that failed once each."""
    totals = Totals()

    for outcome in outcomes:
        group = totals.rows.setdefault(outcome.group, {})
        group[outcome.name] = group.get(outcome.name, 0) + outcome.count

        if outcome.failed and outcome.name not in totals.failed:
            totals.failed.append(outcome.name)

    return totals
