"""Constants of the executor."""

import re
from pathlib import Path
from typing import Final

TEMPLATES_DIR: Final = Path(__file__).parent / "templates"
ALIAS: Final = "src"
SECRET: Final = "src_secret"  # noqa: S105
COLUMN_PATTERN: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
