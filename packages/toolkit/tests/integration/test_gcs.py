import socket
import subprocess
import sys
import time
from collections.abc import Generator
from pathlib import Path

import pytest
from aiobotocore.session import get_session

from toolkit import gcs
from toolkit.settings import Settings

pytestmark = pytest.mark.s3


@pytest.fixture(scope="module")
def moto_endpoint() -> Generator[str]:
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


@pytest.fixture
async def client(moto_endpoint: str):
    config = Settings(
        GCS_ENDPOINT=moto_endpoint,
        GCS_USE_SSL=False,
        GCS_HMAC_ACCESS_ID="id",
        GCS_HMAC_SECRET="secret",  # pyright: ignore[reportArgumentType]
        GCS_REGION="us-east-1",
    )
    async with gcs.create_client(config, get_session()) as s3:
        await s3.create_bucket(Bucket="stg")
        yield s3


async def test_upload_list_and_clear_prefix(client, tmp_path: Path):
    file = tmp_path / "a.parquet"
    file.write_bytes(b"data")
    for index in range(3):
        await gcs.upload_file(client, "stg", f"staging/ds/tb/d={index}/a.parquet", file)
    await gcs.upload_file(client, "stg", "staging/other/a.parquet", file)

    assert len(await gcs.list_keys(client, "stg", "staging/ds/tb/")) == 3
    assert await gcs.clear_prefix(client, "stg", "staging/ds/tb/") == 3
    assert await gcs.list_keys(client, "stg", "staging/") == ["staging/other/a.parquet"]


async def test_delete_keys_deletes_exactly_the_given_keys(client, tmp_path: Path):
    file = tmp_path / "a.parquet"
    file.write_bytes(b"x")
    keys = [f"k/{index}.parquet" for index in range(40)]
    for key in keys:
        await gcs.upload_file(client, "stg", key, file)

    assert await gcs.delete_keys(client, "stg", keys[:30], concurrency=4) == 30
    assert await gcs.list_keys(client, "stg", "k/") == sorted(keys[30:])
