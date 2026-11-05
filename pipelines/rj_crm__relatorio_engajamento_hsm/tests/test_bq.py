import polars as pl
import pytest
from google.api_core.exceptions import NotFound

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import EXAMPLES_SCHEMA
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers import bq


class MissingTable:
    """A BigQuery client whose table does not exist yet."""

    def query(self, _sql: str) -> object:
        """Fail like BigQuery does for a table that is not there."""
        raise NotFound("Not found: Table")


def test_the_first_run_finds_no_example_table_and_gets_an_empty_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """Treat a missing table as no examples, with the schema of the table."""
    monkeypatch.setattr(bq, "connect", MissingTable)

    frame = bq.read_examples("select 1")

    assert frame.is_empty()
    assert frame.schema == pl.Schema(EXAMPLES_SCHEMA)
