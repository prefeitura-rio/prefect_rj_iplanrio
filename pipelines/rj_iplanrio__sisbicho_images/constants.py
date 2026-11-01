"""Constants of rj_iplanrio__sisbicho_images."""

from typing import Final

DEFAULT_BATCH_SIZE: Final = 1000
MAGIC_NUMBERS: Final = {
    b"\x89PNG\r\n\x1a\n": "png",
    b"\xff\xd8\xff": "jpg",
    b"GIF87a": "gif",
    b"GIF89a": "gif",
    b"%PDF": "pdf",
}
"""The first bytes of each kind of file, with its extension."""
CONTENT_TYPES: Final = {"png": "image/png", "jpg": "image/jpeg", "gif": "image/gif"}
DEFAULT_CONTENT_TYPE: Final = "application/octet-stream"
UNKNOWN_EXTENSION: Final = "bin"
IDENTIFIER_COLUMNS: Final = (
    "animal_id",
    "id_animal",
    "id",
    "id_animal_prontuario",
    "codigo_animal",
    "codigo",
    "identificador",
)
"""The columns that can hold the id of an animal, from the most to the least likely."""
MEDIA_COLUMNS: Final = ("qrcode_dados", "foto_dados")
BASE64_ALPHABET: Final = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")
BASE64_BLOCK: Final = 4
