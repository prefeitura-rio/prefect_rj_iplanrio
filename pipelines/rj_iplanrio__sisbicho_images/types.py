"""Types of rj_iplanrio__sisbicho_images."""

from typing import NotRequired, Protocol, TypedDict

from pydantic import JsonValue

type Row = dict[str, JsonValue]


class Animal(TypedDict):
    """An animal with its QR code and photo, as the source table holds them in Base64."""

    animal_identifier: str
    cpf: str | None
    qrcode_dados: bytes | str | None
    foto_dados: bytes | str | None


class Storage(TypedDict):
    """The bucket where the photos go, and the project that owns it."""

    bucket: str
    prefix: str
    project: str


class Limits(TypedDict, total=False):
    """The size of a batch of animals, and the most animals that a run takes."""

    batch_size: int
    max_records: NotRequired[int]


class PhotoBlob(Protocol):
    """The part of a Cloud Storage blob that the upload of a photo uses."""

    metadata: dict[str, str] | None

    def exists(self) -> bool:
        """Tell whether the blob is in the bucket."""
        ...

    def upload_from_string(self, data: bytes, content_type: str) -> None:
        """Upload the bytes."""
        ...

    def patch(self) -> None:
        """Save the metadata."""
        ...


class PhotoBucket(Protocol):
    """The part of a Cloud Storage bucket that the upload of a photo uses."""

    def blob(self, blob_name: str) -> PhotoBlob:
        """Return a blob of the name."""
        ...
