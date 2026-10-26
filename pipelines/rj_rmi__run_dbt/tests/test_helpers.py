from json import dumps
from os import environ
from pathlib import Path

import pytest

from pipelines.rj_rmi__run_dbt.helpers import write_credentials


def test_the_key_goes_to_a_file_that_the_google_adc_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Write the key as it is and point `GOOGLE_APPLICATION_CREDENTIALS` at the file."""
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    key = dumps({"type": "service_account", "project_id": "rj-rmi"})

    write_credentials(key)
    path = Path(environ["GOOGLE_APPLICATION_CREDENTIALS"])

    try:
        assert path.read_text(encoding="utf-8") == key
    finally:
        path.unlink()
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
