"""Google Cloud Storage operations through the S3-compatible API with HMAC keys."""

from asyncio import Semaphore, gather
from collections.abc import Buffer, Coroutine, Iterable, Iterator, Mapping
from io import BufferedReader, RawIOBase
from pathlib import Path
from typing import BinaryIO, final, override

from obstore import (
    delete_async,
    get,
    head,
    list_with_delimiter_async,
    open_reader,
    put,
    put_async,
)
from obstore import list as list_objects
from obstore.store import ObjectStore, S3Store

from .constants import BUFFER_SIZE, DELETE_SPAN, READ_BUFFER_BYTES, UPLOAD_SPAN
from .settings import GcsSettings
from .telemetry import DELETED_FILES, UPLOADED_BYTES, UPLOADED_FILES, logger, tracer


def create_store(bucket: str) -> S3Store:
    """Create the S3 store of one bucket, with the endpoint and keys of the settings."""
    settings = GcsSettings()
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


async def list_prefixes(store: ObjectStore, prefix: str) -> list[str]:
    """List the child folders of one prefix, with no trailing slash.

    It reads one level, so it does not list the objects below the folders.

    ```python
    await list_prefixes(store, "staging")  # ["staging/ds_a", "staging/ds_b"]
    ```
    """
    listing = await list_with_delimiter_async(store, prefix)
    return sorted(folder.rstrip("/") for folder in listing["common_prefixes"])


def list_sizes(store: ObjectStore, prefix: str) -> dict[str, int]:
    """Map the key of each object under one prefix to its size in bytes."""
    return {
        item["path"]: item["size"]
        for batch in list_objects(store, prefix)
        for item in batch
    }


def has_key(store: ObjectStore, key: str) -> bool:
    """Return whether the object exists."""
    try:
        head(store, key)
    except FileNotFoundError:
        return False

    return True


def read_bytes(store: ObjectStore, key: str) -> bytes | None:
    """Read one object, or return nothing if the key does not exist."""
    try:
        return bytes(get(store, key).bytes())
    except FileNotFoundError:
        return None


def write_bytes(
    store: ObjectStore,
    key: str,
    data: bytes,
    *,
    content_type: str | None = None,
    metadata: Mapping[str, str] | None = None,
) -> None:
    """Write one object and replace it if it exists.

    ```python
    write_bytes(store, "a/b.png", image, content_type="image/png")
    ```
    """
    attributes: dict[str, str] = dict(metadata or {})

    if content_type:
        attributes["Content-Type"] = content_type

    put(store, key, data, attributes=attributes or None)


def download(store: ObjectStore, key: str, path: Path) -> Path:
    """Write one object to a file, create the folders, and return the path."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("wb") as file:
        for chunk in read_chunks(store, key):
            file.write(chunk)

    return path


@final
class ObjectReader:
    """A binary file over one object that reads in chunks."""

    def __init__(self, store: ObjectStore, key: str) -> None:
        """Open the object."""
        self.reader = open_reader(store, key)

    def read(self, size: int = -1, /) -> bytes:
        """Read up to `size` bytes, or all of them with a negative size."""
        return bytes(self.reader.read(None if size < 0 else size))


@final
class ObjectFile(RawIOBase):
    """A seekable binary file over one object, with ranged reads."""

    def __init__(self, store: ObjectStore, key: str) -> None:
        """Open the object, with a read-ahead buffer."""
        super().__init__()
        self.reader = open_reader(store, key, buffer_size=READ_BUFFER_BYTES)

    @override
    def readable(self) -> bool:
        """Return that the file reads."""
        return True

    @override
    def seekable(self) -> bool:
        """Return that the file seeks."""
        return True

    @override
    def readinto(self, buffer: Buffer, /) -> int:
        """Read up to the size of `buffer` bytes into it, and return how many."""
        view = memoryview(buffer).cast("B")
        data = bytes(self.reader.read(len(view)))
        view[: len(data)] = data
        return len(data)

    @override
    def seek(self, offset: int, whence: int = 0, /) -> int:
        """Move to `offset`, from the start, the current place or the end."""
        return self.reader.seek(offset, whence)

    @override
    def tell(self) -> int:
        """Return the current place."""
        return self.reader.tell()


def open_file(store: ObjectStore, key: str) -> BinaryIO:
    """Open one object as a buffered file, for a reader that takes a file."""
    return BufferedReader(ObjectFile(store, key), buffer_size=BUFFER_SIZE)


def read_chunks(store: ObjectStore, key: str) -> Iterator[bytes]:
    """Read one object in chunks, so a large file never sits in memory."""
    for chunk in get(store, key):
        yield bytes(chunk)


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
