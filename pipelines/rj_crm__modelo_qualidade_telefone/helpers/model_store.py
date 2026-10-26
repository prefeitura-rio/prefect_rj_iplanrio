"""Keep the versions of the model in GCS, or in a folder for the tests.

The layout under the root (`gs://bucket/prefix` or a path)::

    champion.json             the pointer: {"versao": "2026-10-01", "promovido_em":
    "..."} champion_anterior.json    the pointer before it, for a rollback
    <version>/modelo.txt      LightGBM in its native format, which survives a library
    upgrade <version>/metadata.json   the features in the order of the model, the
    hyperparameters and so on

A published version is never overwritten. To promote one is to change the pointer, and
the pointer is always the last thing written, so a failure never points to a half
version. A rollback is `promote_version` with the version before.
"""

from datetime import datetime
from json import dumps, loads
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from google.cloud.exceptions import NotFound
from google.cloud.storage import Blob, Client
from lightgbm import Booster
from toolkit.telemetry import logger

from pipelines.rj_crm__modelo_qualidade_telefone.constants import (
    CHAMPION_FILE,
    GCS_PREFIX,
    METADATA_FILE,
    MODEL_FILE,
    PREVIOUS_CHAMPION_FILE,
    TIMEZONE,
)
from pipelines.rj_crm__modelo_qualidade_telefone.types import Champion


def split_gcs(root: str) -> tuple[str, str]:
    """Return the bucket and the prefix of `gs://bucket/prefix`, with no end slashes."""
    bucket, _, prefix = root[len(GCS_PREFIX) :].partition("/")

    return bucket, prefix.strip("/")


def object_path(root: str, *parts: str) -> str:
    """Return the key of a file under the root in GCS, or its path in a folder."""
    if root.startswith(GCS_PREFIX):
        _, prefix = split_gcs(root)

        return "/".join(part for part in (prefix, *parts) if part)

    return str(Path(root).joinpath(*parts))


def blob_of(root: str, parts: tuple[str, ...], client: Client | None) -> Blob:
    """Return the blob of a file under a root in GCS.

    Raise `ValueError` without a client.
    """
    if client is None:
        raise ValueError("A root in GCS needs a client")

    bucket, _ = split_gcs(root)

    return client.bucket(bucket).blob(object_path(root, *parts))


def read_text(root: str, *parts: str, client: Client | None = None) -> str | None:
    """Return the text of a file under the root, or nothing if it does not exist."""
    if root.startswith(GCS_PREFIX):
        try:
            return blob_of(root, parts, client).download_as_text()
        except NotFound:
            return None

    path = Path(object_path(root, *parts))

    return path.read_text(encoding="utf-8") if path.exists() else None


def write_text(root: str, *parts: str, content: str, client: Client | None = None) -> None:
    """Write a text file under the root, and replace it if it exists."""
    if root.startswith(GCS_PREFIX):
        blob_of(root, parts, client).upload_from_string(content, content_type="text/plain; charset=utf-8")
        return

    path = Path(object_path(root, *parts))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def publish_version(
    root: str, version: str, booster: Booster, metadata: dict[str, object], client: Client | None = None
) -> None:
    """Write the model and the metadata of a new version, and do not promote it.

    The `metadata` must have `features`, in the order that the model expects. Raise
    `FileExistsError` if the version is published, and `ValueError` for a bad version or
    for features that do not match the model.
    """
    if not version or "/" in version:
        raise ValueError(f"Bad version: {version!r}")

    if "features" not in metadata:
        raise ValueError("The metadata needs the key 'features'")

    if booster.feature_name() != metadata["features"]:
        raise ValueError("The features of the model do not match metadata['features']")

    if read_text(root, version, MODEL_FILE, client=client) is not None:
        raise FileExistsError(f"The version {version} is already in {root}")

    write_text(root, version, MODEL_FILE, content=booster.model_to_string(), client=client)
    write_text(
        root,
        version,
        METADATA_FILE,
        content=dumps(metadata, indent=2, ensure_ascii=False, default=str),
        client=client,
    )
    logger.info("Version published: version=%s root=%s", version, root)


def load_version(root: str, version: str, client: Client | None = None) -> Champion:
    """Load one version of the model.

    Raise `FileNotFoundError` if it is missing or half written, and `ValueError` if the
    features of the model do not match the metadata.
    """
    model = read_text(root, version, MODEL_FILE, client=client)
    metadata_text = read_text(root, version, METADATA_FILE, client=client)

    if model is None or metadata_text is None:
        raise FileNotFoundError(f"The version {version} is half written, or missing, in {root}")

    booster = Booster(model_str=model)
    metadata = cast("dict[str, object]", loads(metadata_text))

    if booster.feature_name() != metadata["features"]:
        raise ValueError(f"Version {version}: the features of the model do not match the metadata")

    return Champion(version, booster, metadata)


def load_champion(root: str, client: Client | None = None) -> Champion | None:
    """Load the model that `champion.json` points to, or nothing if no pointer."""
    pointer = read_text(root, CHAMPION_FILE, client=client)

    if pointer is None:
        return None

    return load_version(root, str(cast("dict[str, object]", loads(pointer))["versao"]), client=client)


def promote_version(root: str, version: str, client: Client | None = None) -> None:
    """Point `champion.json` to a published version, and keep the pointer before it.

    Raise `FileNotFoundError` if the version is not complete.
    """
    load_version(root, version, client=client)
    current = read_text(root, CHAMPION_FILE, client=client)

    if current is not None:
        write_text(root, PREVIOUS_CHAMPION_FILE, content=current, client=client)

    pointer = {"versao": version, "promovido_em": datetime.now(ZoneInfo(TIMEZONE)).isoformat(timespec="seconds")}
    write_text(root, CHAMPION_FILE, content=dumps(pointer, indent=2), client=client)
    logger.info("Champion promoted: version=%s", version)
