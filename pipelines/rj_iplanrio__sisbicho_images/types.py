"""Types of rj_iplanrio__sisbicho_images."""

from typing import NotRequired, TypedDict

from pydantic import JsonValue

type Row = dict[str, JsonValue]


class Animal(TypedDict):
    """An animal with its QR code and photo, as Base64 in the source table."""

    animal_identifier: str
    cpf: str | None
    qrcode_dados: bytes | str | None
    foto_dados: bytes | str | None


class Storage(TypedDict):
    """The bucket where the photos go."""

    bucket: str
    prefix: str


class Limits(TypedDict, total=False):
    """The size of a batch of animals, and the most animals that a run takes."""

    batch_size: int
    max_records: NotRequired[int]
