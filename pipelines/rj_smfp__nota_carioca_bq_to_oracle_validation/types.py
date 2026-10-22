"""Types of rj_smfp__nota_carioca_bq_to_oracle_validation."""

from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import RawTextEncoding


@dataclass(frozen=True)
class Source:
    """The BigQuery dataset that was loaded into Oracle. With no `table_ids`, every table of the dataset counts."""

    project: str = "rj-iplanrio-dia"
    dataset_id: str = "nota_carioca_staging"
    table_ids: list[str] | None = None


@dataclass(frozen=True)
class Options:
    """How the validation runs.

    With `compute_column_metrics`, it reads each whole table in both databases, which is slow. With
    `fail_on_divergence`, the run fails if a table differs. `template_schema` and `excluded_columns` are the same as
    in the load, and so is `raw_text_encoding`.
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
