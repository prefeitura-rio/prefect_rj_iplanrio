from base64 import b64encode
from collections.abc import Callable

import pytest
from cv2 import INTER_NEAREST, QRCodeEncoder, imencode, resize
from obstore.store import MemoryStore

PNG = b"\x89PNG\r\n\x1a\n" + b"image"


@pytest.fixture
def store() -> MemoryStore:
    """Return an empty in-memory bucket."""
    return MemoryStore()


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
