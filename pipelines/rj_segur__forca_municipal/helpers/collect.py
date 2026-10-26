"""The reads of each kind of table, as batches of rows."""

from asyncio import gather
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from json import dumps
from math import ceil

from httpx2 import HTTPStatusError
from pydantic import JsonValue
from toolkit.telemetry import logger

from pipelines.rj_segur__forca_municipal.constants import (
    GONE_STATUSES,
    MAX_ROWS_PER_CYCLE,
    SINGLE_PAGE_ENDPOINTS,
)
from pipelines.rj_segur__forca_municipal.helpers.kml import parse_placemarks
from pipelines.rj_segur__forca_municipal.types import Api, Endpoint, Row, Tally
from utils.records import read_json

type Upload = Callable[[list[Row], int], Awaitable[None]]


async def flush_buffer(batches: AsyncIterator[list[Row]], upload: Upload, limit: int = MAX_ROWS_PER_CYCLE) -> int:
    """Hold the batches until there are `limit` rows, and upload them as a cycle.

    Return how many rows came.

    A batch that goes over the limit uploads as many cycles as it fills. The last rows
    upload as the last cycle. A cycle has its number, from 1.
    """
    buffer: list[Row] = []
    cycle = 0
    total = 0

    async for batch in batches:
        buffer.extend(batch)
        total += len(batch)

        while len(buffer) >= limit:
            cycle += 1
            await upload(buffer[:limit], cycle)
            buffer = buffer[limit:]

    if buffer:
        await upload(buffer, cycle + 1)

    return total


def count_cycles(total_pages: int, page_size: int) -> tuple[int, int]:
    """Return how many pages make a cycle of `MAX_ROWS_PER_CYCLE` rows.

    Also return how many cycles the pages need.
    """
    per_cycle = ceil(MAX_ROWS_PER_CYCLE / page_size)

    return per_cycle, ceil(total_pages / per_cycle)


async def read_pages(
    api: Api, endpoint: str, table_id: str, page_size: int, first_page: tuple[int, list[Row]]
) -> AsyncIterator[list[Row]]:
    """Return the items of a list endpoint as one batch for each cycle.

    The first page is already read.
    """
    total_pages, first_items = first_page

    if total_pages > 1 and table_id in SINGLE_PAGE_ENDPOINTS:
        logger.warning("%s has %d pages, but its pagination is not documented: check it", endpoint, total_pages)

    per_cycle, cycles = count_cycles(total_pages, page_size)
    logger.info("%d pages, %d cycles", total_pages, cycles)
    yield first_items

    for start in range(2, total_pages + 1, per_cycle):
        yield await api.get_pages(endpoint, start, min(start + per_cycle - 1, total_pages), page_size)


async def read_unit_day(endpoint: Endpoint, unit_id: str, day: str) -> list[Row]:
    """Return the positions of a unit on a day, each with the day as `data_coleta`."""
    extra = {
        "unitId": unit_id.lower(),
        "dataInicio": day,
        "dataFim": day,
        "horaInicio": "00:00:00",
        "horaFim": "23:59:59",
    }
    total_pages, items = await endpoint.api.get_first(endpoint.path, endpoint.page_size, extra)

    if total_pages > 1:
        items += await endpoint.api.get_pages(endpoint.path, 2, total_pages, endpoint.page_size, extra)

    return [{**row, "data_coleta": day} for row in items]


async def read_unit_days(
    endpoint: Endpoint, unit_ids: Sequence[str], day: str, concurrency: int, tally: Tally
) -> AsyncIterator[list[Row]]:
    """Return the positions of the units on a day as a batch for each group of units.

    A group has `concurrency` units.

    A unit that fails gives nothing, and the tally counts it.
    """
    for start in range(0, len(unit_ids), concurrency):
        group = unit_ids[start : start + concurrency]
        outcomes = await gather(*(read_unit_day(endpoint, unit, day) for unit in group), return_exceptions=True)
        batch: list[Row] = []

        for unit, outcome in zip(group, outcomes, strict=True):
            tally.attempted += 1

            if isinstance(outcome, BaseException):
                tally.errors += 1
                logger.warning(
                    "A unit failed and is skipped: unit=%s day=%s error=%s", unit, day, type(outcome).__name__
                )
            else:
                batch += outcome

        if batch:
            yield batch


def dump_nested(body: dict[str, JsonValue]) -> Row:
    """Return a detail with each list and object as JSON text."""
    return {
        key: dumps(value, ensure_ascii=False) if isinstance(value, list | dict) else value
        for key, value in body.items()
    }


async def read_detail(api: Api, endpoint: str, qmd_id: str, tally: Tally) -> Row | None:
    """Return the detail of a QMD, or nothing if it is gone (404 or 410) or fails.

    A failure counts in the tally, and a QMD that is gone does not.
    """
    try:
        body = read_json(await api.get(endpoint.format(id=qmd_id)))
    except HTTPStatusError as error:
        status = error.response.status_code

        if status in GONE_STATUSES:
            logger.warning("A QMD is gone and is skipped: qmd=%s status=%d", qmd_id, status)
        else:
            tally.errors += 1
            logger.warning("A QMD failed: qmd=%s status=%d", qmd_id, status)

        return None
    except (OSError, ValueError, RuntimeError) as error:
        tally.errors += 1
        logger.warning("A QMD failed: qmd=%s error=%s", qmd_id, type(error).__name__)

        return None

    return dump_nested(body) if isinstance(body, dict) else None


async def read_details(
    api: Api, endpoint: str, ids: Sequence[str], concurrency: int, tally: Tally
) -> AsyncIterator[list[Row]]:
    """Return the QMD details as a batch for each group of `concurrency` ids."""
    for start in range(0, len(ids), concurrency):
        group = ids[start : start + concurrency]
        tally.attempted += len(group)
        rows = [
            row
            for row in await gather(*(read_detail(api, endpoint, qmd_id, tally) for qmd_id in group))
            if row is not None
        ]
        logger.info("Ids %d to %d of %d: %d details", start + 1, start + len(group), len(ids), len(rows))

        if rows:
            yield rows


async def read_kml(api: Api, endpoint: str, qmd_id: str, tally: Tally) -> list[Row]:
    """Return the placemarks of the KML of a QMD.

    A KML that fails or has no placemark counts in the tally.
    """
    try:
        text = (await api.get(endpoint.format(id=qmd_id))).text
    except (HTTPStatusError, OSError, RuntimeError) as error:
        logger.warning("A KML is not available: qmd=%s error=%s", qmd_id, error)
        tally.errors += 1
        return []

    rows = parse_placemarks(text, qmd_id)
    tally.errors += not rows

    return rows


async def read_kmls(
    api: Api, endpoint: str, ids: Sequence[str], concurrency: int, tally: Tally
) -> AsyncIterator[list[Row]]:
    """Return the KML placemarks as a batch for each group of `concurrency` ids."""
    for start in range(0, len(ids), concurrency):
        group = ids[start : start + concurrency]
        tally.attempted += len(group)
        rows = [
            row
            for found in await gather(*(read_kml(api, endpoint, qmd_id, tally) for qmd_id in group))
            for row in found
        ]
        logger.info("Ids %d to %d of %d: %d placemarks", start + 1, start + len(group), len(ids), len(rows))

        if rows:
            yield rows


def check_error_rate(name: str, tally: Tally, limit: float) -> None:
    """Raise `RuntimeError` if the share of failed ids is over the limit."""
    if tally.errors and tally.errors / tally.attempted > limit:
        rate = tally.errors / tally.attempted
        raise RuntimeError(f"{name}: {tally.errors} of {tally.attempted} ids failed ({rate:.1%} is over {limit:.1%})")
