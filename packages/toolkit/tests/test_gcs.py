from asyncio import run as run_async
from asyncio import sleep as async_sleep
from pathlib import Path

import pytest
from httpx2 import put
from hypothesis import given
from hypothesis import strategies as st
from obstore import put as put_object
from obstore.store import MemoryStore, ObjectStore

from toolkit import gcs

TREE = {"ano=2024/mes=1/a.parquet": b"a", "ano=2024/b.parquet": b"b"}


def write_tree(root: Path, files: dict[str, bytes]) -> None:
    """Write the files of the tree under the root."""
    for name, content in files.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_bytes(content)


async def upload_list_and_delete(store: ObjectStore, root: Path) -> None:
    """Upload a tree, list the keys and delete them, checking each step."""
    write_tree(root, TREE)

    uploaded = await gcs.upload(store, "ds/tb", root)
    await gcs.upload(store, "other", root / "ano=2024" / "b.parquet")
    await gcs.delete_keys(store, ["ds/tb/ano=2024/b.parquet"], concurrency=2)

    assert sorted(uploaded) == [
        "ds/tb/ano=2024/b.parquet",
        "ds/tb/ano=2024/mes=1/a.parquet",
    ]
    assert await gcs.list_keys(store, "ds/") == ["ds/tb/ano=2024/mes=1/a.parquet"]
    assert await gcs.list_keys(store, "other/") == ["other/b.parquet"]


@pytest.mark.parametrize(
    ("target", "prefix", "expected"),
    [
        pytest.param(
            "",
            "ds/tb",
            ["ds/tb/ano=2024/b.parquet", "ds/tb/ano=2024/mes=1/a.parquet"],
            id="a_directory_keeps_its_hive_folders",
        ),
        pytest.param(
            "ano=2024/b.parquet",
            "ds/tb/",
            ["ds/tb/b.parquet"],
            id="a_file_lands_by_its_name_and_a_trailing_slash_is_not_doubled",
        ),
    ],
)
async def test_an_upload_puts_the_files_under_the_prefix(
    tmp_path, target, prefix, expected
) -> None:
    """Put the files under the prefix."""
    store = MemoryStore()
    write_tree(tmp_path, TREE)

    keys = await gcs.upload(store, prefix, tmp_path / target)

    assert sorted(keys) == expected
    assert await gcs.list_keys(store, "") == expected


@pytest.mark.parametrize(
    ("make", "error", "message"),
    [
        pytest.param(lambda root: root, ValueError, "no files", id="empty_directory"),
        pytest.param(
            lambda root: root / "nope", FileNotFoundError, "nope", id="missing_path"
        ),
    ],
)
async def test_a_path_with_nothing_to_upload_is_an_error(
    tmp_path, make, error, message
) -> None:
    """Reject a path with nothing to upload."""
    with pytest.raises(error, match=message):
        await gcs.upload(MemoryStore(), "p", make(tmp_path))


async def test_upload_list_and_delete(tmp_path) -> None:
    """Upload, list and delete on an in-memory store."""
    await upload_list_and_delete(MemoryStore(), tmp_path)


@given(calls=st.integers(0, 30), limit=st.integers(1, 8))
def test_no_more_calls_run_at_once_than_the_limit_and_all_of_them_finish(
    calls, limit
) -> None:
    """Never run more calls at once than the limit, and finish all of them."""
    running = peak = finished = 0

    async def call() -> None:
        """Count the calls that run at once."""
        nonlocal running, peak, finished
        running += 1
        peak = max(peak, running)
        await async_sleep(0)
        running -= 1
        finished += 1

    run_async(gcs.gather_limited((call() for _ in range(calls)), limit))

    assert (finished, peak <= limit) == (calls, True)


@pytest.mark.s3
async def test_the_store_built_from_the_settings_works_over_the_s3_protocol(
    s3_endpoint, tmp_path, monkeypatch
) -> None:
    """Work over the S3 protocol with the store built from the settings."""
    monkeypatch.setenv("GCS_ENDPOINT", s3_endpoint)
    monkeypatch.setenv("GCS_USE_SSL", "false")
    monkeypatch.setenv("GCS_HMAC_ACCESS_ID", "id")
    monkeypatch.setenv("GCS_HMAC_SECRET", "secret")
    monkeypatch.setenv("GCS_REGION", "us-east-1")
    put(f"http://{s3_endpoint}/bucket").raise_for_status()

    await upload_list_and_delete(gcs.create_store("bucket"), tmp_path)


async def test_prefixes_list_the_folders_of_one_level_only() -> None:
    """Give the child folders of a prefix, and not the objects below them."""
    store = MemoryStore()
    for key in (
        "staging/a/t1/x.parquet",
        "staging/a/t2/y.parquet",
        "staging/b/t3/z.parquet",
        "staging/loose.txt",
    ):
        await store.put_async(key, b"1")

    assert await gcs.list_prefixes(store, "staging") == ["staging/a", "staging/b"]
    assert await gcs.list_prefixes(store, "staging/a") == [
        "staging/a/t1",
        "staging/a/t2",
    ]


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        pytest.param("ds/a.txt", b"hello", id="an_existing_key_returns_its_bytes"),
        pytest.param("ds/missing.txt", None, id="a_missing_key_returns_nothing"),
    ],
)
def test_read_bytes_returns_the_object_or_nothing(
    key: str, expected: bytes | None
) -> None:
    """Return the bytes of an existing key and nothing for a missing key."""
    store = MemoryStore()
    gcs.write_bytes(store, "ds/a.txt", b"hello")

    assert gcs.read_bytes(store, key) == expected


def test_write_bytes_replaces_the_object_and_has_key_sees_it() -> None:
    """Replace an object with the same key and report that it exists."""
    store = MemoryStore()

    assert gcs.has_key(store, "ds/a.txt") is False

    gcs.write_bytes(
        store, "ds/a.txt", b"one", content_type="text/plain", metadata={"sha1": "x"}
    )
    gcs.write_bytes(store, "ds/a.txt", b"two")

    assert gcs.has_key(store, "ds/a.txt") is True
    assert gcs.read_bytes(store, "ds/a.txt") == b"two"


def test_list_sizes_maps_each_key_under_the_prefix_to_its_size() -> None:
    """Map the keys under one prefix to their sizes and skip other prefixes."""
    store = MemoryStore()
    gcs.write_bytes(store, "ds/a.txt", b"12")
    gcs.write_bytes(store, "ds/sub/b.txt", b"123")
    gcs.write_bytes(store, "other/c.txt", b"1")

    assert gcs.list_sizes(store, "ds/") == {"ds/a.txt": 2, "ds/sub/b.txt": 3}


def test_download_writes_the_object_to_a_file_and_read_chunks_yields_it(
    tmp_path: Path,
) -> None:
    """Write the object to a new folder and yield the same bytes in chunks."""
    store = MemoryStore()
    gcs.write_bytes(store, "ds/a.bin", b"abcdef")

    path = gcs.download(store, "ds/a.bin", tmp_path / "out" / "a.bin")

    assert path.read_bytes() == b"abcdef"
    assert b"".join(gcs.read_chunks(store, "ds/a.bin")) == b"abcdef"


def test_object_reader_reads_the_bytes_in_chunks() -> None:
    """Read the bytes of an object in two calls."""
    store = MemoryStore()
    gcs.write_bytes(store, "ds/a.bin", b"abcdef")

    reader = gcs.ObjectReader(store, "ds/a.bin")

    assert (reader.read(4), reader.read(4)) == (b"abcd", b"ef")


@pytest.mark.parametrize(
    ("offset", "size", "expected"),
    [
        pytest.param(0, 4, b"0123", id="from_the_start"),
        pytest.param(6, 4, b"6789", id="from_the_middle"),
        pytest.param(8, 10, b"89", id="past_the_end"),
    ],
)
def test_an_object_file_reads_a_range_after_a_seek(offset, size, expected) -> None:
    """Seek and read like a file, as a Parquet reader does."""
    store = MemoryStore()
    put_object(store, "k", b"0123456789")

    with gcs.open_file(store, "k") as file:
        file.seek(offset)
        assert file.read(size) == expected
