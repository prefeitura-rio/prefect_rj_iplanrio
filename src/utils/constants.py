"""Constants of the shared Prefect code."""

from pathlib import Path
from re import compile as compile_pattern
from typing import Final

from .types import Cloud

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

AUDIO_EXTENSIONS: Final = ("mp3", "wav", "ogg", "oga", "opus")
"""The extensions of the audio files that a transcription accepts."""

NO_SPEECH: Final = "Áudio sem conteúdo reconhecível"
"""The text of an audio that the transcription found no speech in."""

BIFROST_BASE_URL: Final = "https://bifrost.iplan.dados.rio"
"""The address of the Bifrost AI gateway."""

BIFROST_MODEL: Final = "vertex/gemini-3.6-flash"
"""The model that a pipeline asks the gateway for, unless it picks another."""

BIFROST_ATTEMPTS: Final = 3
"""How many times a question to the gateway runs before it fails."""

BIFROST_FIRST_WAIT_SECONDS: Final = 2.0
"""How long the gateway waits before the second try. Each next wait doubles it."""

BIFROST_CALIBRATED_BATCH: Final = 50
BIFROST_CALIBRATED_TOKENS: Final = 8192
"""The batch size for which 8192 output tokens were enough, so the limit scales with the batch."""

USER_AGENT: Final = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
"""The User-Agent of an API request. INMET resets the connection of a library User-Agent such as `python-httpx`."""

DRIVE_FOLDER_MIME: Final = "application/vnd.google-apps.folder"

TEST_PREFIX: Final = "teste-"
"""The prefix of the name of a campaign that runs in test mode."""

DEFAULT_CLOUD: Final = Cloud(bucket="rj-iplanrio", project="rj-iplanrio", connection="rj-iplanrio.us.biglake")
"""The bucket and the BigQuery project of a flow that does not set its own `cloud`."""
