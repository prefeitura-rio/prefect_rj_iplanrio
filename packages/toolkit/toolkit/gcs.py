"""Google Cloud Storage operations through the S3-compatible API with HMAC keys."""

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, cast

from aiobotocore.session import AioSession, ClientCreatorContext, get_session

from .settings import Settings, settings

if TYPE_CHECKING:  # pragma: no cover
    from types_aiobotocore_s3.client import S3Client


def create_client(
    config: Settings = settings, session: AioSession | None = None
) -> "ClientCreatorContext[S3Client]":
    """Create a typed S3 client context for GCS.

    :param config: Settings with the endpoint and HMAC keys.
    :param session: Optional aiobotocore session.
    """
    create = cast(
        "Callable[..., ClientCreatorContext[S3Client]]",
        (session or get_session()).create_client,
    )
    scheme = "https" if config.GCS_USE_SSL else "http"
    return create(
        "s3",
        endpoint_url=f"{scheme}://{config.GCS_ENDPOINT}",
        aws_access_key_id=config.GCS_HMAC_ACCESS_ID,
        aws_secret_access_key=config.GCS_HMAC_SECRET.get_secret_value(),
        region_name=config.GCS_REGION,
    )


async def list_keys(client: "S3Client", bucket: str, prefix: str) -> list[str]:
    """List every object key under one prefix.

    :param client: Client from :func:`create_client`.
    :param bucket: Bucket name.
    :param prefix: Key prefix.
    :returns: The keys in listing order.
    """
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(
            key
            for obj in page.get("Contents", [])
            if (key := obj.get("Key")) is not None
        )
    return keys


async def upload_file(client: "S3Client", bucket: str, key: str, path: Path) -> None:
    """Upload one local file to ``key``."""
    await client.put_object(Bucket=bucket, Key=key, Body=path.read_bytes())


async def delete_keys(
    client: "S3Client", bucket: str, keys: list[str], *, concurrency: int = 16
) -> int:
    """Delete objects one by one, because GCS has no S3 multi-object delete.

    :param client: Client from :func:`create_client`.
    :param bucket: Bucket name.
    :param keys: Keys to delete.
    :param concurrency: Maximum number of parallel deletes.
    :returns: The number of deleted keys.
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def delete(key: str) -> None:
        async with semaphore:
            await client.delete_object(Bucket=bucket, Key=key)

    await asyncio.gather(*(delete(key) for key in keys))
    return len(keys)


async def clear_prefix(client: "S3Client", bucket: str, prefix: str) -> int:
    """Delete every object under one prefix.

    :returns: The number of deleted keys.
    """
    return await delete_keys(client, bucket, await list_keys(client, bucket, prefix))
