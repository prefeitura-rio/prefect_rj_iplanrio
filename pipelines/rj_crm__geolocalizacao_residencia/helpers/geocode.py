"""The geocoding runs: one service over many addresses, Geocode.xyz and Geoapify.

Geocode.xyz has retries, and Geoapify takes batches.
"""

from asyncio import Semaphore, gather, sleep
from collections.abc import Sequence
from random import uniform

from httpx2 import AsyncClient, HTTPError
from pydantic import JsonValue, ValidationError
from toolkit.telemetry import logger

from pipelines.rj_crm__geolocalizacao_residencia.constants import (
    DEFAULT_TIMEOUT,
    GEOAPIFY_MAX_WAIT_SECONDS,
    GEOAPIFY_POLL_SECONDS,
    GEOAPIFY_POLL_TIMEOUT,
    GEOAPIFY_SUBMIT_TIMEOUT,
    JITTER_RANGE,
    MAX_BACKOFF_SECONDS,
    MIN_SLEEP_SECONDS,
    XYZ_ATTEMPTS,
    XYZ_THROTTLE_BASE,
)
from pipelines.rj_crm__geolocalizacao_residencia.helpers.providers import (
    OK,
    PAYMENT_REQUIRED,
    TOO_MANY_REQUESTS,
    WAZE,
    Provider,
    empty_geocode,
    finish_geoapify,
    is_valid_xyz,
    locationiq,
    maptiler,
    opencage,
    parse_xyz,
    xyz_url,
)
from pipelines.rj_crm__geolocalizacao_residencia.types import Geocode, Pause, Run, Step
from utils.records import dig, read_json

ERRORS = (HTTPError, ValidationError, ValueError, KeyError, IndexError, TypeError)


def backoff_delay(attempt: int, base: float, jitter: float) -> float:
    """Return the seconds to wait before a try.

    It is `base` doubled for each attempt up to 60, plus or minus a share.

    `jitter` is the share, from -0.1 to 0.1. The wait is never under 0.1 seconds.
    """
    delay = min(base * (1 << attempt), MAX_BACKOFF_SECONDS)

    return max(MIN_SLEEP_SECONDS, delay + jitter * delay)


async def rest(run: Run) -> None:
    """Wait the time that a service asks between two requests, with a little jitter."""
    await run.pause(backoff_delay(0, run.wait, uniform(-JITTER_RANGE, JITTER_RANGE)))


async def geocode_one(run: Run, provider: Provider, address: str) -> Geocode:
    """Ask a service for one address and return its geocode.

    An empty geocode comes back if it finds nothing or fails.

    The wait comes after the request and outside the limit of requests at one time.
    """
    async with run.slots:
        query = provider.build(address)

        try:
            response = await run.client.get(
                query.url, params=query.params, headers=query.headers, timeout=DEFAULT_TIMEOUT
            )
            found = provider.parse(response, run.today)
        except ERRORS:
            found = None

    await rest(run)

    return found or empty_geocode(provider.name, run.today)


async def geocode_many(
    provider: Provider, addresses: Sequence[str], *, today: str, concurrent: int, wait: float
) -> list[Geocode]:
    """Geocode the addresses with one service, `concurrent` at a time, in order."""
    async with AsyncClient() as client:
        run = Run(client, Semaphore(concurrent), today, wait, sleep)
        results = await gather(*(geocode_one(run, provider, address) for address in addresses))

    found = sum(1 for result in results if result["latitude"] is not None)
    logger.info("Geocoded: provider=%s found=%d of %d", provider.name, found, len(addresses))

    return list(results)


async def geocode_xyz_one(run: Run, address: str, key: str) -> Geocode:
    """Ask Geocode.xyz for one address, with up to three tries.

    A throttled answer waits and tries again. A 402, or an error with no credits left,
    stops the tries. Only an answer with valid coordinates counts.
    """
    result: Geocode | None = None

    async with run.slots:
        for attempt in range(XYZ_ATTEMPTS):
            try:
                response = await run.client.get(xyz_url(address, key), timeout=DEFAULT_TIMEOUT)

                if response.status_code == PAYMENT_REQUIRED:
                    break

                if response.status_code not in (OK, TOO_MANY_REQUESTS):
                    continue

                data = read_json(response)
                throttled = str(dig(data, "latt") or "").startswith("Throttled")

                if response.status_code == TOO_MANY_REQUESTS or throttled:
                    jitter = uniform(-JITTER_RANGE, JITTER_RANGE)
                    await run.pause(backoff_delay(attempt, XYZ_THROTTLE_BASE, jitter))
                    continue

                credits_left = dig(data, "remaining_credits")

                if dig(data, "error") and isinstance(credits_left, str | int | float) and float(credits_left) <= 0:
                    break

                if is_valid_xyz(data):
                    result = parse_xyz(data, run.today)
                    break
            except ERRORS:
                continue

    await rest(run)

    return result or empty_geocode("geocode_xyz", run.today)


async def geocode_xyz_many(
    addresses: Sequence[str], key: str, *, today: str, concurrent: int, wait: float
) -> list[Geocode]:
    """Geocode the addresses with Geocode.xyz, `concurrent` at one time, in order."""
    async with AsyncClient() as client:
        run = Run(client, Semaphore(concurrent), today, wait, sleep)

        return list(await gather(*(geocode_xyz_one(run, address, key) for address in addresses)))


async def read_locationiq_balance(key: str) -> int:
    """Return the requests that LocationIQ still gives today, or 0 if unclear."""
    try:
        async with AsyncClient() as client:
            response = await client.get("https://us1.locationiq.com/v1/balance", params={"key": key, "format": "json"})
            balance = dig(read_json(response), "balance", "day")

        return int(balance) if isinstance(balance, str | int | float) else 0
    except ERRORS as error:
        logger.warning("The balance of LocationIQ could not be read: %s", error)
        return 0


async def submit_geoapify(client: AsyncClient, addresses: Sequence[str], key: str) -> str | None:
    """Send a batch of addresses to Geoapify and return the URL of its job.

    It gives nothing if Geoapify refuses it.
    """
    url = "https://api.geoapify.com/v1/batch/geocode/search"

    try:
        response = await client.post(
            url,
            params={"lang": "pt", "filter": "countrycode:br", "apiKey": key},
            json=list(addresses),
            timeout=GEOAPIFY_SUBMIT_TIMEOUT,
        )
    except HTTPError as error:
        logger.warning("The Geoapify batch was not sent: %s", error)
        return None

    if response.status_code != OK:
        logger.warning("The Geoapify batch was refused: status=%d", response.status_code)
        return None

    job = dig(read_json(response), "url")

    return job if isinstance(job, str) else None


async def poll_geoapify(client: AsyncClient, job_url: str, key: str, pause: Pause = sleep) -> list[JsonValue] | None:
    """Wait for a Geoapify job and return its results, or nothing on a failure.

    It gives up after five minutes.
    """
    waited = 0

    while waited < GEOAPIFY_MAX_WAIT_SECONDS:
        await pause(GEOAPIFY_POLL_SECONDS)
        waited += GEOAPIFY_POLL_SECONDS

        try:
            response = await client.get(job_url, params={"apiKey": key}, timeout=GEOAPIFY_POLL_TIMEOUT)
            body = read_json(response) if response.status_code == OK else None
        except ERRORS:
            continue

        status = dig(body, "status")

        if status == "completed":
            results = dig(body, "results")

            return results if isinstance(results, list) else []

        if status == "failed":
            return None

    return None


async def geocode_geoapify(
    addresses: Sequence[str], key: str, *, today: str, batch_size: int, pause: Pause = sleep
) -> list[Geocode]:
    """Geocode the addresses with Geoapify batches of `batch_size`, in order.

    A batch that is refused, fails or times out gives empty geocodes for its addresses.
    """
    geocodes: list[Geocode] = []

    async with AsyncClient() as client:
        for start in range(0, len(addresses), batch_size):
            batch = addresses[start : start + batch_size]
            job = await submit_geoapify(client, batch, key)
            results = await poll_geoapify(client, job, key, pause) if job else None
            logger.info(
                "Geoapify batch: number=%d size=%d done=%s", start // batch_size + 1, len(batch), results is not None
            )

            for index in range(len(batch)):
                item = results[index] if results is not None and index < len(results) else None
                geocodes.append(finish_geoapify(item, today) or empty_geocode("geoapify", today))

    return geocodes


async def run_step(step: Step, addresses: Sequence[str], key: str, today: str) -> list[Geocode]:
    """Run one service of the fallback over the addresses, in order.

    LocationIQ gives a daily balance of requests, and only that many addresses go to it.
    The rest comes back empty. With no balance, or no clear answer, all of them come
    back empty.
    """
    if step.name == "geocode_xyz":
        return await geocode_xyz_many(addresses, key, today=today, concurrent=step.concurrent, wait=step.wait)

    providers = {"waze": WAZE, "maptiler": maptiler(key), "opencage": opencage(key), "locationiq": locationiq(key)}
    provider = providers[step.name]

    if step.name == "locationiq":
        balance = await read_locationiq_balance(key)
        done = await geocode_many(
            provider, addresses[:balance], today=today, concurrent=step.concurrent, wait=step.wait
        )

        return [*done, *(empty_geocode(provider.name, today) for _ in addresses[balance:])]

    return await geocode_many(provider, addresses, today=today, concurrent=step.concurrent, wait=step.wait)
