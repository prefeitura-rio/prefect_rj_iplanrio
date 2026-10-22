"""Tasks of rj_crm__geolocalizacao_residencia."""

from datetime import datetime

from prefect import task
from pydantic import TypeAdapter
from toolkit.telemetry import logger

from pipelines.rj_crm__geolocalizacao_residencia.constants import (
    BILLING_PROJECT,
    DEFAULT_SLEEP_SECONDS,
    FALLBACK_STEPS,
    GEOAPIFY_BATCH_SIZE,
    SP_TZ,
)
from pipelines.rj_crm__geolocalizacao_residencia.helpers.geocode import geocode_geoapify, geocode_many, run_step
from pipelines.rj_crm__geolocalizacao_residencia.helpers.providers import nominatim
from pipelines.rj_crm__geolocalizacao_residencia.helpers.rows import has_point, join_rows
from pipelines.rj_crm__geolocalizacao_residencia.settings import Settings
from pipelines.rj_crm__geolocalizacao_residencia.types import Address, Geocode, Row
from utils.tasks import read_bq
from utils.utils import render_query

ADDRESSES: TypeAdapter[list[Address]] = TypeAdapter(list[Address])


def today() -> str:
    """Return today in São Paulo, as the text of the `updated_date` column."""
    return datetime.now(SP_TZ).date().isoformat()


def texts(addresses: list[Address]) -> list[str]:
    """Return the full address of each address."""
    return [address["endereco_completo"] or "" for address in addresses]


@task(retries=2, retry_delay_seconds=30)
async def read_addresses_task() -> list[Address]:
    """Read up to 10,000 addresses of citizens that have no coordinates yet, in a random order.

    Only an address with a full text is kept.
    """
    frame = await read_bq(render_query(__file__, "addresses"), project=BILLING_PROJECT)
    addresses = [address for address in ADDRESSES.validate_python(frame.to_dicts()) if address["endereco_completo"]]
    logger.info("Addresses to geocode: count=%d", len(addresses))

    return addresses


@task
async def geocode_nominatim_task(addresses: list[Address], max_concurrent: int) -> list[Row]:
    """Geocode the addresses with the Nominatim server of the settings, and return the rows that got a point."""
    settings = Settings()
    provider = nominatim(settings.NOMINATIM_DOMAIN, settings.NOMINATIM_USER_AGENT)
    geocodes = await geocode_many(
        provider, texts(addresses), today=today(), concurrent=max_concurrent, wait=DEFAULT_SLEEP_SECONDS
    )
    rows = join_rows(addresses, geocodes, need_street=False)
    logger.info("Nominatim done: geocoded=%d of %d", len(rows), len(addresses))

    return rows


@task
async def geocode_fallback_task(addresses: list[Address]) -> list[Row]:
    """Geocode the addresses with a chain of services, each taking what the one before could not find.

    The order is Waze, MapTiler, Geocode.xyz, LocationIQ and OpenCage. A service whose answers are all empty
    counts as out of quota and is skipped, and so is one whose key is not set. Only an address with a point
    and a street comes out.
    """
    settings = Settings()
    keys = {
        "MAPTILER_API_KEY": settings.MAPTILER_API_KEY.get_secret_value(),
        "GEOCODE_XYZ_API_KEY": settings.GEOCODE_XYZ_API_KEY.get_secret_value(),
        "LOCATIONIQ_API_KEY": settings.LOCATIONIQ_API_KEY.get_secret_value(),
        "OPENCAGE_API_KEY": settings.OPENCAGE_API_KEY.get_secret_value(),
    }
    day = today()
    found_addresses: list[Address] = []
    found_geocodes: list[Geocode] = []
    remaining = list(addresses)

    for step in FALLBACK_STEPS:
        if not remaining:
            break

        key = keys[step.key_setting] if step.key_setting else ""

        if step.key_setting and not key:
            logger.warning("The key is not set, so the service is skipped: service=%s", step.name)
            continue

        geocodes = await run_step(step, texts(remaining), key, day)

        if not any(has_point(geocode) for geocode in geocodes):
            logger.warning("Every answer is empty, so the service counts as out of quota: service=%s", step.name)
            continue

        still: list[Address] = []

        for address, geocode in zip(remaining, geocodes, strict=True):
            if has_point(geocode):
                found_addresses.append(address)
                found_geocodes.append(geocode)
            else:
                still.append(address)

        logger.info("Service done: service=%s found=%d left=%d", step.name, len(found_addresses), len(still))
        remaining = still

    return join_rows(found_addresses, found_geocodes, need_street=True)


@task
async def geocode_geoapify_task(addresses: list[Address]) -> list[Row]:
    """Geocode the addresses with the batch API of Geoapify, 100 at a time, and return the rows that got a point.

    Without a key in the settings the result is empty.
    """
    key = Settings().GEOAPIFY_API_KEY.get_secret_value()

    if not key:
        logger.warning("No Geoapify key, so nothing is geocoded")
        return []

    geocodes = await geocode_geoapify(texts(addresses), key, today=today(), batch_size=GEOAPIFY_BATCH_SIZE)

    return join_rows(addresses, geocodes, need_street=False)
