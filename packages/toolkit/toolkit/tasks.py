"""Prefect tasks. Each task is a thin wrapper around one function of the toolkit."""

import tempfile
from pathlib import Path

from asyncer import asyncify
from prefect import task

from .dbt import DbtRun, execute_dbt
from .dump import DumpRequest, StageRequest, run_dump, stage_scan
from .dump_arcgis import dump_arcgis
from .dump_url import UrlType, csv_scan, download
from .prefect import rename_current_flow_run


@task
async def rename_current_flow_run_task(new_name: str) -> None:
    """Rename the current flow run."""
    await rename_current_flow_run(new_name)


@task
async def dump_database_task(request: DumpRequest) -> None:
    """Dump a database query to the staging bucket and publish the table."""
    await run_dump(request)


@task
async def dump_url_task(
    url: str,
    request: StageRequest,
    url_type: UrlType = UrlType.DIRECT,
    sheet_order: int = 0,
    sheet_name: str | None = None,
    sheet_range: str | None = None,
    separator: str = ",",
    encoding: str = "utf-8",
    *,
    ignore_errors: bool = False,
) -> None:
    """Download a CSV source and stage it."""
    with tempfile.TemporaryDirectory() as directory:
        path = await asyncify(download)(
            url,
            Path(directory) / "data.csv",
            url_type,
            sheet_order,
            sheet_name,
            sheet_range,
        )
        scan = csv_scan(path, separator, encoding, ignore_errors=ignore_errors)
        await stage_scan(request, scan)


@task
async def dump_arcgis_task(url: str, crs: str, request: StageRequest) -> None:
    """Download an ArcGIS layer and stage it with EPSG:4326 geometries."""
    await dump_arcgis(url, crs, request)


@task
def execute_dbt_task(request: DbtRun) -> None:
    """Run one dbt command."""
    execute_dbt(request)
