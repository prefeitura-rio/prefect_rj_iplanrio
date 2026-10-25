"""Constants of rj_iplanrio__eai_history."""

from typing import Final

DBT_REPOSITORY: Final = "https://github.com/prefeitura-rio/queries-rj-iplanrio"
DEFAULT_LAST_UPDATE: Final = "1970-01-01T00:00:00+00:00"
"""The cursor when the table is empty or does not exist: every thread is new."""
DEFAULT_SESSION_TIMEOUT_SECONDS: Final = 3600
DEFAULT_DBT_SELECT: Final = "raw_eai_logs_history"
INVALID_USER_IDS: Final = frozenset({"", " ", "/"})
ENGINE_ARGS: Final = {"pool_pre_ping": True, "pool_recycle": 300}
CODE_PLACEHOLDER: Final = "\u00a4C"
LIST_PLACEHOLDER: Final = "\u00a4L\u00a4"
