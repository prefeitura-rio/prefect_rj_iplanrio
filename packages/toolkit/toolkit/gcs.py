"""Google Cloud Storage operations through the S3-compatible API with HMAC keys."""

import asyncio
from collections.abc import Coroutine, Iterable
from pathlib import Path

import obstore
from obstore.store import ObjectStore, S3Store

from .constants import DELETE_SPAN, UPLOAD_SPAN
from .settings import Settings, settings
from .telemetry import DELETED_FILES, UPLOADED_BYTES, UPLOADED_FILES, logger, tracer


def create_store(bucket: str, config: Settings = settings) -> S3Store:
    """Create the S3 store of one bucket, on the endpoint and with the keys of the settings.

    :param bucket: Bucket name.
    :param config: Settings with the endpoint and HMAC keys.
    """
    scheme = "https" if config.GCS_USE_SSL else "http"
    return S3Store(
        bucket,
        endpoint=f"{scheme}://{config.GCS_ENDPOINT}",
        access_key_id=config.GCS_HMAC_ACCESS_ID,
        secret_access_key=config.GCS_HMAC_SECRET.get_secret_value(),
        region=config.GCS_REGION,
        client_options={"allow_http": not config.GCS_USE_SSL},
    )


async def list_keys(store: ObjectStore, prefix: str) -> list[str]:
    """List every object key under one prefix.

    :param store: Store from :func:`create_store`.
    :param prefix: Key prefix.
    :returns: The keys in listing order.
    """
    return [
        item["path"] async for batch in obstore.list(store, prefix) for item in batch
    ]


def files_of(path: Path) -> dict[str, Path]:
    """Map the relative name of each file to its path.

    :param path: A file, or a directory searched recursively.
    :raises FileNotFoundError: If ``path`` does not exist.
    :raises ValueError: If ``path`` is a directory with no files.
    """
    if not path.exists():
        raise FileNotFoundError(path)

    if path.is_file():
        return {path.name: path}

    files = {
        file.relative_to(path).as_posix(): file
        for file in sorted(path.rglob("*"))
        if file.is_file()
    }

    if not files:
        raise ValueError(f"{path} has no files")

    return files


async def gather_limited(
    calls: Iterable[Coroutine[object, object, object]], concurrency: int
) -> None:
    """Await the calls together, with at most ``concurrency`` running at once."""
    semaphore = asyncio.Semaphore(concurrency)

    async def run(call: Coroutine[object, object, object]) -> None:
        async with semaphore:
            await call

    await asyncio.gather(*(run(call) for call in calls))


async def upload(
    store: ObjectStore, prefix: str, path: Path, *, concurrency: int = 16
) -> list[str]:
    """Upload a file, or every file of a directory, under one prefix.

    A directory keeps its relative paths, so Hive folders such as
    ``ano_particao=2024/`` stay in the keys. A large file goes up in parts.

    :param store: Store from :func:`create_store`.
    :param prefix: Key prefix.
    :param path: A file, or a directory.
    :param concurrency: Maximum number of parallel uploads.
    :returns: The keys of the uploaded files.
    :raises FileNotFoundError: If ``path`` does not exist.
    :raises ValueError: If ``path`` is a directory with no files.
    """
    with tracer.start_as_current_span(
        UPLOAD_SPAN, attributes={"toolkit.prefix": prefix}
    ) as span:
        files = files_of(path)
        keys = {name: f"{prefix.rstrip('/')}/{name}" for name in files}

        await gather_limited(
            (
                obstore.put_async(store, keys[name], file)
                for name, file in files.items()
            ),
            concurrency,
        )

        size = sum(file.stat().st_size for file in files.values())
        span.set_attributes({"toolkit.files": len(files), "toolkit.bytes": size})

    UPLOADED_FILES.add(len(files), {"prefix": prefix})
    UPLOADED_BYTES.add(size, {"prefix": prefix})
    logger.info("Files uploaded: prefix=%s files=%d bytes=%d", prefix, len(files), size)
    return list(keys.values())


async def delete_keys(
    store: ObjectStore, keys: list[str], *, concurrency: int = 16
) -> None:
    """Delete objects one by one, because GCS has no S3 multi-object delete.

    :param store: Store from :func:`create_store`.
    :param keys: Keys to delete.
    :param concurrency: Maximum number of parallel deletes.
    """
    with tracer.start_as_current_span(
        DELETE_SPAN, attributes={"toolkit.objects": len(keys)}
    ):
        await gather_limited(
            (obstore.delete_async(store, key) for key in keys), concurrency
        )

    DELETED_FILES.add(len(keys))
    logger.info("Objects deleted: objects=%d", len(keys))
