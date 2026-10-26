"""Helpers of rj_iplanrio__sisbicho_images."""

from base64 import b64decode
from binascii import Error as Base64Error
from collections.abc import Mapping, Sequence
from datetime import datetime
from hashlib import sha1
from json import dumps
from re import fullmatch

from cv2 import IMREAD_GRAYSCALE, QRCodeDetector, imdecode
from numpy import frombuffer, uint8
from pydantic import JsonValue, TypeAdapter, ValidationError
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__sisbicho_images.constants import (
    BASE64_ALPHABET,
    BASE64_BLOCK,
    CONTENT_TYPES,
    DEFAULT_CONTENT_TYPE,
    IDENTIFIER_COLUMNS,
    MAGIC_NUMBERS,
    MEDIA_COLUMNS,
    UNKNOWN_EXTENSION,
)
from pipelines.rj_iplanrio__sisbicho_images.types import Animal, PhotoBucket, Row, Storage

ANIMALS: TypeAdapter[list[Animal]] = TypeAdapter(list[Animal])
JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class PdfError(ValueError):
    """The data is a PDF where an image should be."""


def read_animals(rows: Sequence[Mapping[str, object]]) -> list[Animal]:
    """Check the rows of the query and return them as animals.

    Raise `ValidationError` if a row lacks a column or has a value of the wrong type.
    """
    return ANIMALS.validate_python(rows)


def pick_identifier(columns: Sequence[str]) -> str:
    """Return the column that holds the id of the animal: a known name, else the first column of no media.

    Raise `ValueError` if the table has no such column.
    """
    by_lower = {column.lower(): column for column in columns}

    for candidate in IDENTIFIER_COLUMNS:
        if candidate in by_lower:
            return by_lower[candidate]

    for column in columns:
        if column.lower() not in MEDIA_COLUMNS:
            return column

    raise ValueError("The table has no column for the id of the animal")


def quote_ids(identifiers: Sequence[str]) -> list[str]:
    """Return the ids that are safe to write in a query, and raise `ValueError` for one that is not.

    An id has letters, digits, dashes, dots and underscores.
    """
    for identifier in identifiers:
        if not fullmatch(r"[\w.-]+", identifier):
            raise ValueError(f"The id of an animal has an unsafe character: {identifier!r}")

    return list(identifiers)


def to_text(value: bytes | str | None) -> str:
    """Return a Base64 value as text without the `data:` prefix, or empty text if there is none.

    Raise `ValueError` if bytes are not ASCII.
    """
    if value is None:
        return ""

    try:
        text = value.decode("ascii") if isinstance(value, bytes) else value
    except UnicodeDecodeError as error:
        raise ValueError("The bytes of an image are not ASCII Base64") from error

    text = text.strip()

    return text.split(",", 1)[1] if text.lower().startswith("data:") and "," in text else text


def looks_like_base64(text: str) -> bool:
    """Tell whether the text could be Base64: its alphabet, and a length of whole blocks."""
    text = text.strip()

    return bool(text) and set(text) <= BASE64_ALPHABET and len(text) % BASE64_BLOCK == 0


def starts_like_file(data: bytes) -> bool:
    """Tell whether the bytes start as a known file: an image or a PDF."""
    return any(data.startswith(magic) for magic in MAGIC_NUMBERS)


def decode_image(text: str) -> bytes:
    """Decode the Base64 of an image, once or twice, as the source stores either.

    Raise `PdfError` for a PDF, and `ValueError` if the text is not Base64 or is not a
    known file.
    """
    padded = text.strip()
    padded += "=" * (-len(padded) % BASE64_BLOCK)
    data = padded.encode()

    for _ in range(2):
        try:
            data = b64decode(data)
        except Base64Error as error:
            raise ValueError("Invalid Base64") from error

        if data.startswith(b"%PDF"):
            raise PdfError("A PDF came where an image should be")

        if starts_like_file(data):
            return data

    raise ValueError("The data is not a known file after two decodings")


def read_qrcode(image: bytes) -> str | None:
    """Return the text of the QR code in an image, or nothing if there is none."""
    picture = imdecode(frombuffer(image, dtype=uint8), IMREAD_GRAYSCALE)

    if picture is None:
        logger.warning("The bytes of a QR code are not an image")
        return None

    payload = QRCodeDetector().detectAndDecode(picture)[0].strip()

    return payload or None


def normalize_payload(text: str) -> str | None:
    """Return the text of a QR code as JSON.

    JSON stays as it is. Lines of `key: value` become an object, a line without a key
    continues the value before it, and a line before any key goes in `observacao`. Text
    with no line of that kind stays as text.
    """
    cleaned = text.strip()

    if not cleaned:
        return None

    try:
        parsed = JSON.validate_json(cleaned)
    except ValidationError:
        parsed = None

    if isinstance(parsed, dict | list):
        return dumps(parsed, ensure_ascii=False)

    if isinstance(parsed, str):
        cleaned = parsed.strip()

    fields: dict[str, str] = {}
    current: str | None = None

    for line in (line.strip() for line in cleaned.replace("\r", "\n").split("\n")):
        if not line:
            continue

        key, separator, value = line.partition(":")

        if separator and key.strip():
            current = key.strip()
            fields[current] = value.strip()
        else:
            current = current or "observacao"
            fields[current] = f"{fields.get(current, '')} {line}".strip()

    return dumps(fields, ensure_ascii=False) if fields else cleaned


def extract_qrcode(value: bytes | str | None) -> str | None:
    """Return the JSON payload of the QR code in a Base64 image, or nothing if it has none.

    A PDF, text that is not Base64, a file that is not a known image and an image
    without a QR code give nothing.
    """
    text = to_text(value)

    if not looks_like_base64(text):
        return None

    try:
        payload = read_qrcode(decode_image(text))
    except ValueError as error:
        logger.info("A QR code is not an image that decodes: %s", error)
        return None

    return normalize_payload(payload) if payload else None


def sanitize_identifier(identifier: str) -> str:
    """Return an id that is safe in the name of a file: letters, digits, dashes and underscores."""
    text = identifier.strip()

    return "".join(char if char.isalnum() or char in "-_" else "_" for char in text) if text else "sem_identificador"


def name_photo(image: bytes, identifier: str, prefix: str) -> tuple[str, str, str]:
    """Return the blob name, the content type and the SHA-1 of a photo.

    The name has the id of the animal and the SHA-1, so the same photo has the same
    name.
    """
    extension = next((kind for magic, kind in MAGIC_NUMBERS.items() if image.startswith(magic)), UNKNOWN_EXTENSION)
    digest = sha1(image).hexdigest()

    return (
        f"{prefix}/animal_id={sanitize_identifier(identifier)}/{digest}.{extension}",
        CONTENT_TYPES.get(extension, DEFAULT_CONTENT_TYPE),
        digest,
    )


def upload_photo(bucket: PhotoBucket, storage: Storage, animal: Animal) -> str | None:
    """Upload the photo of an animal to the bucket, unless it is there, and return its public URL.

    An animal with no photo or a PDF gives nothing. Raise `ValueError` if the photo is
    not Base64 or not a known file, so a batch never takes a broken photo.
    """
    text = to_text(animal["foto_dados"])

    if not text:
        return None

    identifier = animal["animal_identifier"]

    try:
        image = decode_image(text)
    except PdfError:
        logger.info("The photo of an animal is a PDF, so it is skipped: animal=%s", identifier)
        return None
    except ValueError as error:
        raise ValueError(f"The photo of the animal {identifier} does not decode: {error}") from error

    name, content_type, digest = name_photo(image, identifier, storage["prefix"])
    blob = bucket.blob(name)

    if not blob.exists():
        blob.upload_from_string(image, content_type=content_type)
        blob.metadata = {"sha1": digest}
        blob.patch()

    return f"https://storage.googleapis.com/{storage['bucket']}/{name}"


def build_rows(animals: Sequence[Animal], bucket: PhotoBucket, storage: Storage, now: datetime) -> list[Row]:
    """Return the rows of the table for the animals, with the QR code payload and the URL of the photo.

    Each photo goes to the bucket on the way.
    """
    return [
        {
            "id_animal": animal["animal_identifier"],
            "cpf": animal["cpf"],
            "qrcode_payload": extract_qrcode(animal["qrcode_dados"]),
            "foto_url": upload_photo(bucket, storage, animal),
            "ingestao_data": now.isoformat(),
        }
        for animal in animals
    ]
