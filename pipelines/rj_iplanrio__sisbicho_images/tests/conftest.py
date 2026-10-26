from base64 import b64encode
from collections.abc import Callable
from typing import final

import pytest
from cv2 import INTER_NEAREST, QRCodeEncoder, imencode, resize

PNG = b"\x89PNG\r\n\x1a\n" + b"image"


@final
class FakeBlob:
    """A blob of a fake bucket that records its uploads."""

    def __init__(self, bucket: "FakeBucket", name: str) -> None:
        """Store the arguments."""
        self.bucket = bucket
        self.name = name
        self.metadata: dict[str, str] | None = {}

    def exists(self) -> bool:
        """Tell whether the name has an upload."""
        return self.name in self.bucket.files

    def upload_from_string(self, data: bytes, content_type: str) -> None:
        """Record the upload."""
        self.bucket.files[self.name] = (data, content_type)

    def patch(self) -> None:
        """Record the metadata."""
        self.bucket.patched.append(self.name)


@final
class FakeBucket:
    """A bucket that keeps its uploads in memory."""

    def __init__(self) -> None:
        """Store the arguments."""
        self.files: dict[str, tuple[bytes, str]] = {}
        self.patched: list[str] = []

    def blob(self, blob_name: str) -> FakeBlob:
        """Return a blob of the name."""
        return FakeBlob(self, blob_name)


@pytest.fixture
def bucket() -> FakeBucket:
    """Return an empty fake bucket."""
    return FakeBucket()


@pytest.fixture
def qrcode_png() -> Callable[[str], bytes]:
    """Return a function that draws a text as a QR code and returns it as PNG bytes."""

    def draw(text: str) -> bytes:
        code = QRCodeEncoder.create().encode(text)
        big = resize(code, None, fx=8, fy=8, interpolation=INTER_NEAREST)

        return imencode(".png", big)[1].tobytes()

    return draw


def twice(data: bytes) -> str:
    """Encode bytes in Base64 twice, as the source sometimes stores them."""
    return b64encode(b64encode(data)).decode()
