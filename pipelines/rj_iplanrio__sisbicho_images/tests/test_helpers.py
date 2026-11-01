from base64 import b64encode
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha1

import pytest
from obstore.store import MemoryStore
from pydantic import ValidationError
from toolkit.gcs import list_sizes, read_bytes

from pipelines.rj_iplanrio__sisbicho_images.helpers import (
    PdfError,
    build_rows,
    decode_image,
    extract_qrcode,
    looks_like_base64,
    normalize_payload,
    pick_identifier,
    quote_ids,
    read_animals,
    sanitize_identifier,
    to_text,
    upload_photo,
)
from pipelines.rj_iplanrio__sisbicho_images.tests.conftest import PNG, twice
from pipelines.rj_iplanrio__sisbicho_images.types import Animal, Storage

STORAGE = Storage(bucket="photos", prefix="animal/fotos")


def animal(identifier: str = "7", qrcode: bytes | str | None = None, photo: bytes | str | None = None) -> Animal:
    """Build an animal."""
    return Animal(animal_identifier=identifier, cpf="123", qrcode_dados=qrcode, foto_dados=photo)


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        pytest.param(["foto_dados", "ID_ANIMAL", "id"], "ID_ANIMAL", id="a_known_name_in_any_case"),
        pytest.param(
            ["qrcode_dados", "foto_dados", "microchip", "name"], "microchip", id="the_first_column_of_no_media"
        ),
        pytest.param(["id", "animal_id"], "animal_id", id="the_most_likely_name_first"),
    ],
)
def test_the_id_column_is_a_known_name_or_the_first_without_media(columns: list[str], expected: str) -> None:
    """Pick the column that holds the id."""
    assert pick_identifier(columns) == expected


def test_a_table_with_only_media_columns_has_no_id_column() -> None:
    """Raise, so the run does not guess."""
    with pytest.raises(ValueError, match="no column"):
        pick_identifier(["qrcode_dados", "foto_dados"])


def test_an_id_with_a_quote_is_refused_before_it_reaches_a_query() -> None:
    """Allow letters, digits, dashes, dots and underscores."""
    assert quote_ids(["12", "a-b_c.d"]) == ["12", "a-b_c.d"]

    with pytest.raises(ValueError, match="unsafe"):
        quote_ids(["1' OR '1'='1"])


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, "", id="nothing"),
        pytest.param(" abcd ", "abcd", id="spaces_around"),
        pytest.param(b"abcd", "abcd", id="ascii_bytes"),
        pytest.param("data:image/png;base64,abcd", "abcd", id="a_data_uri"),
        pytest.param("DATA:x", "DATA:x", id="a_prefix_without_a_comma"),
    ],
)
def test_the_base64_value_loses_its_wrapping(value: bytes | str | None, expected: str) -> None:
    """Return the Base64 text only."""
    assert to_text(value) == expected


def test_bytes_that_are_not_ascii_are_refused() -> None:
    """Raise, because they cannot be Base64."""
    with pytest.raises(ValueError, match="ASCII"):
        to_text("é".encode())


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("abcd", True, id="whole_blocks"),
        pytest.param("abc", False, id="a_partial_block"),
        pytest.param("ab!d", False, id="a_bad_character"),
        pytest.param("  ", False, id="blank"),
    ],
)
def test_text_looks_like_base64_by_alphabet_and_length(text: str, expected: bool) -> None:
    """Check the alphabet and the blocks."""
    assert looks_like_base64(text) is expected


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(b64encode(PNG).decode(), id="once"),
        pytest.param(twice(PNG), id="twice"),
        pytest.param(b64encode(PNG).decode().rstrip("="), id="without_padding"),
    ],
)
def test_an_image_decodes_once_or_twice(text: str) -> None:
    """Return the image bytes whatever the layers."""
    assert decode_image(text) == PNG


@pytest.mark.parametrize(
    ("text", "error"),
    [
        pytest.param(b64encode(b"%PDF-1.4").decode(), PdfError, id="a_pdf"),
        pytest.param(b64encode(b"just text").decode(), ValueError, id="an_unknown_file"),
        pytest.param("!!!!", ValueError, id="not_base64"),
    ],
)
def test_a_pdf_or_an_unknown_file_does_not_decode(text: str, error: type[Exception]) -> None:
    """Raise for what is not an image."""
    with pytest.raises(error):
        decode_image(text)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param('{"a": 1}', '{"a": 1}', id="json_object"),
        pytest.param('"Nome: Rex"', '{"Nome": "Rex"}', id="json_text_with_keys"),
        pytest.param("Nome: Rex\nRaça: SRD", '{"Nome": "Rex", "Raça": "SRD"}', id="lines_with_keys"),
        pytest.param("Nome: Rex\ncontinua", '{"Nome": "Rex continua"}', id="a_line_that_continues_a_value"),
        pytest.param("antes\nNome: Rex", '{"observacao": "antes", "Nome": "Rex"}', id="a_line_before_any_key"),
        pytest.param("  ", None, id="blank"),
    ],
)
def test_the_text_of_a_qrcode_becomes_json(text: str, expected: str | None) -> None:
    """Keep JSON and turn lines of `key: value` into an object."""
    assert normalize_payload(text) == expected


def test_the_qrcode_in_an_image_is_read_as_json(qrcode_png: Callable[[str], bytes]) -> None:
    """Read the QR code, whatever the layers of Base64."""
    image = qrcode_png("Nome: Rex")

    assert extract_qrcode(b64encode(image)) == '{"Nome": "Rex"}'
    assert extract_qrcode(twice(image)) == '{"Nome": "Rex"}'


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="nothing"),
        pytest.param("not base64!", id="not_base64"),
        pytest.param(b64encode(b"%PDF-1.4"), id="a_pdf"),
        pytest.param(b64encode(PNG), id="an_image_without_a_code"),
        pytest.param(b64encode(b"Nome: Rex"), id="text_that_is_not_a_known_file"),
    ],
)
def test_a_value_without_a_qrcode_gives_nothing(value: bytes | None) -> None:
    """Return nothing and do not raise."""
    assert extract_qrcode(value) is None


@pytest.mark.parametrize(
    ("identifier", "expected"),
    [
        pytest.param("a/b c", "a_b_c", id="unsafe_characters"),
        pytest.param("ok-1_2", "ok-1_2", id="safe"),
        pytest.param("  ", "sem_identificador", id="blank"),
    ],
)
def test_the_id_in_a_file_name_has_only_safe_characters(identifier: str, expected: str) -> None:
    """Replace what a file name should not hold."""
    assert sanitize_identifier(identifier) == expected


def test_a_photo_goes_to_the_bucket_once_under_a_name_with_its_hash(store: MemoryStore) -> None:
    """Name the file by animal and SHA-1, and skip an upload that exists."""
    text = b64encode(PNG).decode()
    digest = sha1(PNG).hexdigest()
    expected = f"animal/fotos/animal_id=7/{digest}.png"

    first = upload_photo(store, STORAGE, animal(photo=text))
    second = upload_photo(store, STORAGE, animal(photo=text))

    assert first == second == f"https://storage.googleapis.com/photos/{expected}"
    assert list(list_sizes(store, "animal/")) == [expected]
    assert read_bytes(store, expected) == PNG


def test_a_missing_photo_or_a_pdf_gives_no_url(store: MemoryStore) -> None:
    """Skip the photo and do not upload."""
    assert upload_photo(store, STORAGE, animal()) is None
    assert upload_photo(store, STORAGE, animal(photo=b64encode(b"%PDF-1.4").decode())) is None
    assert list_sizes(store, "animal/") == {}


def test_a_photo_that_does_not_decode_stops_the_batch(store: MemoryStore) -> None:
    """Raise with the animal, so no broken photo is taken."""
    with pytest.raises(ValueError, match="animal 7"):
        upload_photo(store, STORAGE, animal(photo="!!!!"))


def test_the_rows_have_the_qrcode_the_photo_and_the_time(
    store: MemoryStore, qrcode_png: Callable[[str], bytes]
) -> None:
    """Build one row for each animal."""
    now = datetime(2026, 7, 17, 12, tzinfo=UTC)

    rows = build_rows(
        [animal(photo=b64encode(PNG).decode()), animal("8", qrcode=b64encode(qrcode_png("Nome: Rex")))],
        store,
        STORAGE,
        now,
    )

    assert [(row["id_animal"], row["qrcode_payload"], row["cpf"], row["ingestao_data"]) for row in rows] == [
        ("7", None, "123", "2026-07-17T12:00:00+00:00"),
        ("8", '{"Nome": "Rex"}', "123", "2026-07-17T12:00:00+00:00"),
    ]
    assert str(rows[0]["foto_url"]).endswith(".png")
    assert rows[1]["foto_url"] is None


def test_the_rows_of_the_query_become_animals() -> None:
    """Accept bytes or text for the media, and refuse a missing id."""
    row = {"animal_identifier": "7", "cpf": None, "qrcode_dados": b"x", "foto_dados": "y"}

    assert read_animals([row])[0]["qrcode_dados"] == b"x"

    with pytest.raises(ValidationError):
        read_animals([{**row, "animal_identifier": None}])
