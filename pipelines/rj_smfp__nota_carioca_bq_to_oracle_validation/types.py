"""Types of rj_smfp__nota_carioca_bq_to_oracle_validation."""

from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.types import RawTextEncoding


@dataclass(frozen=True)
class Source:
    """The BigQuery dataset that was loaded into Oracle.

    With no `table_ids`, every table of the dataset counts.
    """

    project: str = "rj-iplanrio-dia"
    dataset_id: str = "nota_carioca_staging"
    table_ids: list[str] | None = None


@dataclass(frozen=True)
class Options:
    """How the validation runs.

    With `compute_column_metrics`, it reads each whole table in both databases, which is
    slow. With `fail_on_divergence`, the run fails if a table differs. `template_schema`
    and `excluded_columns` are the same as in the load, and so is `raw_text_encoding`.
    """

    compute_column_metrics: bool = True
    fail_on_divergence: bool = True
    template_schema: str | None = None
    excluded_columns: list[str] | None = None
    raw_text_encoding: RawTextEncoding = "base64"


@dataclass(frozen=True)
class Outcome:
    """The result of the validation of one table."""

    source: str
    target: str
    divergences: int


@dataclass(frozen=True)
class ValidationRequest:
    """The table to validate and the options of the validation.

    `project`, `dataset_id` and `table_id` name the BigQuery source, and the original
    table in Oracle has the same name. `template_schema` is the schema of the original
    table in Oracle. `excluded_columns` are the columns of the original that the load
    does not create (besides the `ROWID` columns that BigQuery lacks, which are excluded
    on their own). `compute_column_metrics` says whether to compute the metrics for each
    column, which read the whole table in both databases. `raw_text_encoding` is the
    encoding of the text of the `RAW` columns in BigQuery (`base64` or `hex`), the one
    that the load uses.
    """

    project: str
    dataset_id: str
    table_id: str
    template_schema: str
    excluded_columns: tuple[str, ...]
    compute_column_metrics: bool
    raw_text_encoding: RawTextEncoding = "base64"
