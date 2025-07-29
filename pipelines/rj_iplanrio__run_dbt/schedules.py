# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the dbt execute pipeline.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefeitura_rio.pipelines_utils.io import untuple_clocks
from prefeitura_rio.pipelines_utils.prefect import generate_dbt_transform_schedules

from pipelines.constants import Constants

common_parameters = {
    "github_repo": Constants.REPOSITORY_URL.value,
    "gcs_buckets": Constants.GCS_BUCKET.value,
    "bigquery_project": Constants.RJ_IPLANRIO_AGENT_LABEL.value,
    "environment": "prod",
    "rename_flow": True,
}

daily_parameters = [
    {
        **common_parameters,
        "command": "build",
        "select": "tag:daily tag:dbt-bigquery-monitoring",
    },
    {
        **common_parameters,
        "command": "source freshness",
    },
]

weekly_parameters = [
    {
        **common_parameters,
        "command": "build",
        "select": "tag:weekly",
    }
]


dbt_daily_clocks = generate_dbt_transform_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 6, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        Constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
    flow_run_parameters=daily_parameters,
    runs_interval_minutes=15,
)

dbt_weekly_clocks = generate_dbt_transform_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2024, 3, 17, 6, 20, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        Constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
    flow_run_parameters=weekly_parameters,
    runs_interval_minutes=30,
)

dbt_clocks = dbt_daily_clocks + dbt_weekly_clocks

dbt_schedules = Schedule(clocks=untuple_clocks(dbt_clocks))