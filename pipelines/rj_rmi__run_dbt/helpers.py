"""Helpers of rj_rmi__run_dbt."""

from os import environ, fdopen
from tempfile import mkstemp


def write_credentials(key: str) -> None:
    """Write the service account key to a file and point the Google ADC at it."""
    descriptor, path = mkstemp(prefix="rj-rmi-sa-", suffix=".json")

    with fdopen(descriptor, "w", encoding="utf-8") as file:
        file.write(key)

    environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
