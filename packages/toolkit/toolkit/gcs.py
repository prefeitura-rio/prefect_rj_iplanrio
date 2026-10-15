"""Google Cloud Storage operations through the S3-compatible API with HMAC keys."""

from asyncio import Semaphore, gather
from collections.abc import Coroutine, Iterable
from pathlib import Path

from obstore import delete_async, put_async
from obstore import list as list_objects
from obstore.store import ObjectStore, S3Store

from .constants import DELETE_SPAN, UPLOAD_SPAN
from .settings import settings
from .telemetry import DELETED_FILES, UPLOADED_BYTES, UPLOADED_FILES, logger, tracer


def create_store(bucket: str) -> S3Store:
    """Create the S3 store of one bucket, with the endpoint and keys of the settings."""
    scheme = "https" if settings.GCS_USE_SSL else "http"
    return S3Store(
        bucket,
        endpoint=f"{scheme}://{settings.GCS_ENDPOINT}",
        access_key_id=settings.GCS_HMAC_ACCESS_ID,
        secret_access_key=settings.GCS_HMAC_SECRET.get_secret_value(),
        region=settings.GCS_REGION,
        client_options={"allow_http": not settings.GCS_USE_SSL},
    )


async def list_keys(store: ObjectStore, prefix: str) -> list[str]:
    """List every object key under one prefix, in listing order."""
    return [
        item["path"] async for batch in list_objects(store, prefix) for item in batch
    ]


def list_files(path: Path) -> dict[str, Path]:
    """Map the relative name of each file to its path.

    Raise `FileNotFoundError` if `path` does not exist. Raise `ValueError` for a
    directory with no files.

    ```python
    list_files(Path("out"))
    # {"ano=2024/a.parquet": Path("out/ano=2024/a.parquet")}
    list_files(Path("out/a.csv"))
    # {"a.csv": Path("out/a.csv")}
    ```
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
    """Await the calls together, with at most `concurrency` running at once."""
    semaphore = Semaphore(concurrency)

    async def run(call: Coroutine[object, object, object]) -> None:
        """Await one call while it holds a slot."""
        async with semaphore:
            await call

    await gather(*(run(call) for call in calls))


async def upload(
    store: ObjectStore, prefix: str, path: Path, *, concurrency: int = 16
) -> list[str]:
    """Upload a file, or a directory, under one prefix and return the keys.

    A directory keeps its relative paths.

    ```python
    await upload(store, "ds/tb", Path("out"))
    # ["ds/tb/ano_particao=2024/a.parquet"]
    ```
    """
    with tracer.start_as_current_span(
        UPLOAD_SPAN, attributes={"toolkit.prefix": prefix}
    ) as span:
        files = list_files(path)
        keys = {name: f"{prefix.rstrip('/')}/{name}" for name in files}

        await gather_limited(
            (put_async(store, keys[name], file) for name, file in files.items()),
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
    """Delete objects one by one, because GCS has no S3 multi-object delete."""
    with tracer.start_as_current_span(
        DELETE_SPAN, attributes={"toolkit.objects": len(keys)}
    ):
        await gather_limited((delete_async(store, key) for key in keys), concurrency)

    DELETED_FILES.add(len(keys))
    logger.info("Objects deleted: objects=%d", len(keys))
