"""Text helpers."""

from re import sub
from unicodedata import normalize


def slugify_hsm(name: str) -> str:
    """Return the name of an HSM for a file or a folder: letters and digits in lower case, joined by `_`.

    ```python
    slugify_hsm("Campanha 2026 - A!")  # "campanha_2026_a"
    ```
    """
    return sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()


def normalize_label(text: object) -> str:
    """Return a category name to compare: no accent, no capitals, and a space for each `_` or run of spaces."""
    flat = str(text).strip().lower().replace("_", " ")
    plain = normalize("NFKD", flat).encode("ascii", "ignore").decode("ascii")

    return sub(r"\s+", " ", plain)
