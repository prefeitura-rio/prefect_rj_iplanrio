"""Constants of the shared Prefect code."""

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
