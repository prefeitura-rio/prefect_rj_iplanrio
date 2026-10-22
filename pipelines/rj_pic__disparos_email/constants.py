"""Constants of rj_pic__disparos_email."""

from pathlib import Path
from typing import Final

TEMPLATE_DIR: Final = Path(__file__).parent
TEMPLATE_NAME: Final = "email_template.html"
DEFAULT_SUBJECT: Final = "E-mail enviado automaticamente"
PERMANENT_STATUSES: Final = frozenset({400, 401, 403})
"""The statuses of a refused mail. A retry would be refused again."""
REQUEST_TIMEOUT: Final = 30.0
EMAIL_COLUMN: Final = "recipiente_email"
NAME_COLUMN: Final = "recipiente_nome"
