import asyncio
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import httpx2
import pytest
from hypothesis import given
from hypothesis import strategies as st
from obstore.store import MemoryStore, ObjectStore
from pydantic import SecretStr

from toolkit import gcs
from toolkit.settings import settings

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
):
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
):
    """Reject a path with nothing to upload."""
    with pytest.raises(error, match=message):
        await gcs.upload(MemoryStore(), "p", make(tmp_path))


async def test_upload_list_and_delete(tmp_path):
    """Upload, list and delete on an in-memory store."""
    await upload_list_and_delete(MemoryStore(), tmp_path)


@given(calls=st.integers(0, 30), limit=st.integers(1, 8))
def test_no_more_calls_run_at_once_than_the_limit_and_all_of_them_finish(calls, limit):
    """Never run more calls at once than the limit, and finish all of them."""
    running = peak = finished = 0

    async def call() -> None:
        """Count the calls that run at once."""
        nonlocal running, peak, finished
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0)
        running -= 1
        finished += 1

    asyncio.run(gcs.gather_limited((call() for _ in range(calls)), limit))

    assert (finished, peak <= limit) == (calls, True)


@pytest.fixture(scope="module")
def s3_endpoint() -> Iterator[str]:
    """Serve S3 with moto on a free port, and yield its endpoint."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = subprocess.Popen(  # noqa: S603
        [sys.executable, "-m", "moto.server", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(50):
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                break
        time.sleep(0.1)
    yield f"127.0.0.1:{port}"
    server.terminate()


@pytest.mark.s3
async def test_the_store_built_from_the_settings_works_over_the_s3_protocol(
    s3_endpoint, tmp_path, monkeypatch
):
    """Work over the S3 protocol with the store built from the settings."""
    monkeypatch.setattr(settings, "GCS_ENDPOINT", s3_endpoint)
    monkeypatch.setattr(settings, "GCS_USE_SSL", False)
    monkeypatch.setattr(settings, "GCS_HMAC_ACCESS_ID", "id")
    monkeypatch.setattr(settings, "GCS_HMAC_SECRET", SecretStr("secret"))
    monkeypatch.setattr(settings, "GCS_REGION", "us-east-1")
    httpx2.put(f"http://{s3_endpoint}/bucket").raise_for_status()

    await upload_list_and_delete(gcs.create_store("bucket"), tmp_path)
