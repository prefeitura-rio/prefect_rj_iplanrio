"""Types of rj_smfp__nota_carioca_bq_to_oracle."""

from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import DBT_DEPLOYMENT
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import RawTextEncoding


@dataclass(frozen=True)
class Source:
    """The BigQuery dataset that feeds Oracle, and the bucket that holds the export.

    With no `table_ids`, every table of the dataset loads.
    """

    project: str = "rj-iplanrio-dia"
    dataset_id: str = "nota_carioca_staging"
    bucket: str = "rj-iplanrio-dia-bq-to-oracle"
    table_ids: list[str] | None = None


@dataclass(frozen=True)
class Load:
    """How the tables load into Oracle.

    `template_schema` is the schema of the original tables, which defaults to the schema
    of the login. `excluded_columns` are columns of the original that the new table does
    not get. `sessions` is the number of SQL*Loader sessions. The waits are in minutes:
    `0` turns the In-Memory wait off. `inmemory_window_months` is how many months before
    the current one stay in the In-Memory, and `inmemory_release_delay_seconds` is the
    wait before the table that left use goes out of it.
    """

    template_schema: str | None = None
    excluded_columns: list[str] | None = None
    sessions: int = 2
    progress_interval_seconds: int = 30
    index_parallel_degree: int = 4
    raw_text_encoding: RawTextEncoding = "base64"
    bigquery_quiet_minutes: int = 5
    inmemory_wait_minutes: int = 30
    inmemory_window_months: int = 3
    inmemory_release_delay_seconds: int = 120


@dataclass(frozen=True)
class Dbt:
    """The optional dbt run before the load.

    With `enabled`, the flow starts the deployment, waits for it, and exports only after
    it finishes. With no `parameters`, the defaults of the pipeline apply, and a given
    `parameters` replaces them whole.
    """

    enabled: bool = False
    deployment: str = DBT_DEPLOYMENT
    parameters: dict[str, object] | None = None
    timeout_minutes: int = 120
