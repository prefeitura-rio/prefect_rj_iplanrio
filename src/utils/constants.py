"""Constants of the shared Prefect code."""

from pathlib import Path
from re import compile as compile_pattern
from typing import Final

STAGING_PREFIX: Final = "staging"
"""The bucket folder that holds the files of every external table."""

RETRIES: Final = 2
"""How many times a failed step of a dump runs again."""

RETRY_DELAY_SECONDS: Final = 60
"""How long a dump waits before it runs a failed step again."""

DISCORD_LIMIT: Final = 2000
"""The most characters that Discord accepts in one message."""

DOCUMENT_STORES: Final = frozenset({"couchdb", "mongodb"})
"""The database types that store documents, and read a selector instead of SQL."""

CREDENTIAL_SCOPES: Final = (
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/cloud-platform",
)
"""The scopes of the credentials that a pipeline gives to a Google client, by default."""

DBT_FAILED: Final = ("error", "fail", "runtime error")
"""The statuses of a dbt node that mean the node failed."""

DBT_WARNED: Final = ("warn",)
"""The statuses of a dbt node that mean a warning."""

PIPELINES_FOLDER: Final = Path("pipelines")
"""The folder of the repository that has one folder for each pipeline."""

STAGES_FOLDER: Final = "pipelines"
"""The folder of a pipeline with the JSON files of its MongoDB aggregations."""

STAGES_NAME: Final = compile_pattern(r"[a-z0-9]+(-[a-z0-9]+)*")
"""A name of an aggregation: lowercase words with dashes, so it never leaves its folder."""

PLACEHOLDER_NAME: Final = r"[A-Z][A-Z0-9]*(_[A-Z0-9]+)*"
"""The name in a `${NAME}` placeholder: `UPPER_SNAKE_CASE`, as an environment variable."""

TOKEN_KEYS: Final = ("token", "access_token", "authToken", "jwt")
"""The places of a login response where an API puts its token, in the order to look."""
