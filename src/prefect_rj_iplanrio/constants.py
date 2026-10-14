"""Constants of the shared Prefect code."""

from typing import Final

STAGING_PREFIX: Final = "staging"
"""The bucket folder that holds the files of every external table."""

RETRIES: Final = 2
"""How many times a failed step of a dump runs again."""

RETRY_DELAY_SECONDS: Final = 60
"""How long a dump waits before it runs a failed step again."""
