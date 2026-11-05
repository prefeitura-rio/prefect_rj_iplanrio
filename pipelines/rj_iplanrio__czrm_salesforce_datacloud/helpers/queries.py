"""SQL template loader for `queries/*.sql`."""

from pathlib import Path
from string import Template

QUERIES_DIR = Path(__file__).parent.parent / "queries"


def load_query(name: str, **params: object) -> str:
    """Load and render `queries/<name>.sql` with `$var` placeholders.

    Raise `FileNotFoundError` if the file does not exist.
    Raise `KeyError` if a required placeholder is missing.
    """
    return Template((QUERIES_DIR / f"{name}.sql").read_text(encoding="utf-8")).substitute(**params)
