from collections.abc import Callable
from json import loads
from pathlib import Path

import pytest
from lightgbm import Booster

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.model_store import (
    load_champion,
    load_version,
    object_path,
    promote_version,
    publish_version,
    split_gcs,
)

METADATA: dict[str, object] = {"features": list(FEATURES), "n_treino": 10}


@pytest.mark.parametrize(
    ("root", "expected"),
    [
        pytest.param("gs://bucket/models/phone", ("bucket", "models/phone"), id="a_prefix"),
        pytest.param("gs://bucket/", ("bucket", ""), id="no_prefix"),
        pytest.param("gs://bucket/a/b/", ("bucket", "a/b"), id="a_slash_at_the_end"),
    ],
)
def test_a_gcs_root_splits_in_a_bucket_and_a_prefix(root: str, expected: tuple[str, str]) -> None:
    """Split the bucket from the prefix, and drop the slashes at the ends."""
    assert split_gcs(root) == expected


def test_the_key_of_a_file_joins_the_prefix_and_the_parts() -> None:
    """Join with slashes in GCS and with the path of the system in a folder."""
    assert object_path("gs://bucket/models", "v1", "modelo.txt") == "models/v1/modelo.txt"
    assert object_path("gs://bucket", "champion.json") == "champion.json"
    assert object_path("/tmp/models", "v1") == "/tmp/models/v1"


def test_a_published_version_loads_with_its_metadata(make_booster: Callable[[int], Booster], tmp_path: Path) -> None:
    """Write the model and the metadata, and read them back."""
    publish_version(str(tmp_path), "v1", make_booster(0), METADATA)

    champion = load_version(str(tmp_path), "v1")

    assert champion.version == "v1"
    assert champion.metadata == METADATA
    assert champion.booster.feature_name() == list(FEATURES)


@pytest.mark.parametrize(
    ("version", "metadata", "message"),
    [
        pytest.param("a/b", METADATA, "Bad version", id="a_slash_in_the_version"),
        pytest.param("", METADATA, "Bad version", id="an_empty_version"),
        pytest.param("v1", {"n_treino": 1}, "features", id="no_features_in_the_metadata"),
        pytest.param("v1", {"features": ["a"]}, "do not match", id="other_features"),
    ],
)
def test_a_bad_version_is_not_published(
    make_booster: Callable[[int], Booster], tmp_path: Path, version: str, metadata: dict[str, object], message: str
) -> None:
    """Refuse a bad name, and metadata that does not match the model."""
    with pytest.raises(ValueError, match=message):
        publish_version(str(tmp_path), version, make_booster(0), metadata)


def test_a_published_version_is_never_overwritten(make_booster: Callable[[int], Booster], tmp_path: Path) -> None:
    """Raise when a version is published twice."""
    publish_version(str(tmp_path), "v1", make_booster(0), METADATA)

    with pytest.raises(FileExistsError, match="v1"):
        publish_version(str(tmp_path), "v1", make_booster(1), METADATA)


def test_a_missing_version_is_reported(tmp_path: Path) -> None:
    """Raise for a version that is not there."""
    with pytest.raises(FileNotFoundError, match="v9"):
        load_version(str(tmp_path), "v9")


def test_there_is_no_champion_before_the_first_promotion(
    make_booster: Callable[[int], Booster], tmp_path: Path
) -> None:
    """Return nothing when the pointer does not exist, even if a version is published."""
    publish_version(str(tmp_path), "v1", make_booster(0), METADATA)

    assert load_champion(str(tmp_path)) is None


def test_a_promotion_keeps_the_pointer_before_it_for_a_rollback(
    make_booster: Callable[[int], Booster], tmp_path: Path
) -> None:
    """Move the pointer, and save the old one as `champion_anterior.json`."""
    root = str(tmp_path)
    publish_version(root, "v1", make_booster(0), METADATA)
    publish_version(root, "v2", make_booster(1), METADATA)

    promote_version(root, "v1")
    assert not (tmp_path / "champion_anterior.json").exists()

    promote_version(root, "v2")
    champion = load_champion(root)

    assert champion is not None
    assert champion.version == "v2"
    assert loads((tmp_path / "champion_anterior.json").read_text())["versao"] == "v1"
    assert "promovido_em" in loads((tmp_path / "champion.json").read_text())


def test_a_version_that_is_not_published_cannot_be_promoted(tmp_path: Path) -> None:
    """Raise and leave the pointer as it was."""
    with pytest.raises(FileNotFoundError):
        promote_version(str(tmp_path), "v9")

    assert not (tmp_path / "champion.json").exists()


def test_a_gcs_root_needs_a_client() -> None:
    """Raise `ValueError` for a bucket path with no client."""
    with pytest.raises(ValueError, match="needs a client"):
        load_champion("gs://bucket/models")
