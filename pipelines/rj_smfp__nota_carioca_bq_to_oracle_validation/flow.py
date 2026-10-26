"""Flow for rj_smfp__nota_carioca_bq_to_oracle_validation."""

from prefect import flow

from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.tasks import (
    list_tables_task,
    log_session_task,
    summarize_task,
    validate_table_task,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.types import Options, Source

DEFAULT_SOURCE = Source()
DEFAULT_OPTIONS = Options()


@flow(log_prints=True, flow_run_name="validacao-{source.dataset_id}")
def rj_smfp__nota_carioca_bq_to_oracle_validation(
    *, source: Source = DEFAULT_SOURCE, options: Options = DEFAULT_OPTIONS
) -> None:
    """Compare the tables that the load wrote to Oracle with their source in BigQuery.

    It only reads.

    Each table is checked for the row count, the columns, the indexes, the partitions,
    the synonyms, the grants, and the In-Memory state. With
    `options.compute_column_metrics`, it also compares a metric of each column. The run
    fails if a table differs, unless `options.fail_on_divergence` is off.
    """
    log_session_task()
    tables = list_tables_task(source=source)
    results = [validate_table_task(source=source, table_id=table_id, options=options) for table_id in tables]
    summarize_task(results=results, options=options)
