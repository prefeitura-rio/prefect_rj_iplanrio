"""Tasks of rj_crm__get_history_data."""

from asyncio import to_thread
from datetime import UTC, datetime

from httpx2 import AsyncClient, HTTPError
from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_crm__get_history_data.constants import (
    DATASET_ID,
    FINAL_TABLE_ID,
    HTTP_TIMEOUT,
    MERGE_KEYS,
    PAGE_SIZE,
    PROJECT_ID,
    SOAP_HEADERS,
    TMP_TABLE_ID,
)
from pipelines.rj_crm__get_history_data.helpers import bq
from pipelines.rj_crm__get_history_data.helpers.history import filter_recent, keep_history
from pipelines.rj_crm__get_history_data.helpers.sfmc import (
    build_fields_envelope,
    build_list_envelope,
    parse_extensions,
    parse_fields,
)
from pipelines.rj_crm__get_history_data.types import Extension, Extraction, Row
from utils.records import dig, read_json
from utils.settings import SfmcSettings
from utils.utils import render_query


@task(retries=2, retry_delay_seconds=30)
async def authenticate_task() -> str:
    """Log in to SFMC with the client credentials and return the access token, which lasts about 20 minutes."""
    settings = SfmcSettings()
    payload = {
        "grant_type": "client_credentials",
        "client_id": settings.SFMC_CLIENT_ID,
        "client_secret": settings.SFMC_CLIENT_SECRET.get_secret_value(),
    }

    async with AsyncClient(timeout=30) as client:
        response = await client.post(f"{settings.SFMC_AUTH_URL.rstrip('/')}/v2/token", json=payload)
        response.raise_for_status()

    token = dig(read_json(response), "access_token")

    if not isinstance(token, str) or not token:
        raise ValueError("The login of SFMC has no access_token")

    return token


@task(retries=2, retry_delay_seconds=30)
async def list_extensions_task(token: str, soap_uri: str) -> list[Extension]:
    """List the Data Extensions whose name ends in `historico`, with the pages of the SOAP API.

    Raise `RuntimeError` if the API answers with a status that is neither `OK` nor `MoreDataAvailable`.
    """
    endpoint = f"{soap_uri.rstrip('/')}/Service.asmx"
    found: list[Extension] = []
    continue_id: str | None = None

    async with AsyncClient(timeout=HTTP_TIMEOUT) as client:
        while True:
            response = await client.post(
                endpoint, content=build_list_envelope(token, continue_id), headers=SOAP_HEADERS
            )
            response.raise_for_status()
            status, request_id, page = parse_extensions(response.text)
            found += page

            if status == "MoreDataAvailable":
                continue_id = request_id
            elif status in ("OK", ""):
                break
            else:
                raise RuntimeError(f"The SOAP API answered with an unexpected status: {status}")

    history = keep_history(found)
    logger.info("Data Extensions: total=%d history=%d", len(found), len(history))

    return history


@task
async def extract_task(token: str, soap_uri: str, rest_uri: str, extension: Extension) -> Extraction:
    """Read all the records and the columns of a Data Extension, or return the error if it fails.

    The records come from the REST API in pages of 2500, and the columns from the SOAP API. A failure of one
    extension does not stop the others, so it comes back as the `error` of the extraction.
    """
    url = f"{rest_uri.rstrip('/')}/data/v1/customobjectdata/key/{extension.external_key}/rowset"
    records = []

    try:
        async with AsyncClient(timeout=HTTP_TIMEOUT) as client:
            page = 1

            while True:
                response = await client.get(
                    url, headers={"Authorization": f"Bearer {token}"}, params={"$page": page, "$pageSize": PAGE_SIZE}
                )
                response.raise_for_status()
                body = read_json(response)
                items = dig(body, "items")
                batch = [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
                records += batch
                total = dig(body, "count")

                if not batch or len(records) >= (total if isinstance(total, int) else len(batch)):
                    break

                page += 1

            fields_response = await client.post(
                f"{soap_uri.rstrip('/')}/Service.asmx",
                content=build_fields_envelope(token, extension.external_key),
                headers=SOAP_HEADERS,
            )
            fields_response.raise_for_status()
            fields = parse_fields(fields_response.text)
    except (HTTPError, ValueError) as error:
        logger.error("A Data Extension failed: name=%s error=%s", extension.name, error)
        return Extraction(extension.name, error=str(error))

    logger.info(
        "Data Extension read: name=%s pages=%d records=%d fields=%d", extension.name, page, len(records), len(fields)
    )

    return Extraction(extension.name, records, fields)


@task(retries=2, retry_delay_seconds=60)
async def load_task(rows: list[Row], window_days: int) -> int:
    """Keep the rows of the window, replace the tmp table with them, and MERGE it into the final table.

    The MERGE updates the rows with the same key and inserts the new ones, and it never deletes from the final table.
    Return the rows that the MERGE touched.
    """
    recent = filter_recent(rows, window_days, datetime.now(UTC))
    tmp = f"{PROJECT_ID}.{DATASET_ID}.{TMP_TABLE_ID}"
    target = f"{PROJECT_ID}.{DATASET_ID}.{FINAL_TABLE_ID}"
    await to_thread(bq.ensure_tables, PROJECT_ID, DATASET_ID, [TMP_TABLE_ID, FINAL_TABLE_ID])
    await to_thread(bq.replace_table, PROJECT_ID, tmp, recent)
    sql = render_query(
        __file__,
        "merge_history",
        target=target,
        source=tmp,
        keys=list(MERGE_KEYS),
        columns=bq.COLUMNS,
        update_columns=[column for column in bq.COLUMNS if column not in MERGE_KEYS],
    )
    touched = await to_thread(bq.run_merge, PROJECT_ID, sql)
    logger.info("MERGE done: target=%s rows=%d", target, touched)

    return touched
