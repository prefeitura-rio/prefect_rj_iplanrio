"""Tasks that validate the tables loaded in Oracle by the Nota Carioca pipeline."""

from prefect import task
from prefect.cache_policies import NO_CACHE
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.bigquery import list_tables
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import read_oracle_config, validate_identifier
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.inspect import ValidationRequest, validate_table
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.inspect_sections import describe_session
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.report import format_text_table
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.types import Options, Outcome, Source


@task
def list_tables_task(source: Source) -> list[str]:
    """Return the tables that were asked for or, with none, all of the dataset."""
    return source.table_ids or list_tables(project=source.project, dataset_id=source.dataset_id)


@task(cache_policy=NO_CACHE)
def log_session_task() -> None:
    """Log the user, database and version that the validation is connected to."""
    logger.info("Sessão no Oracle\n" + describe_session(read_oracle_config()))


@task(cache_policy=NO_CACHE)
def validate_table_task(source: Source, table_id: str, options: Options) -> Outcome:
    """Validate one table, log its report and return its summary."""
    config = read_oracle_config()
    request = ValidationRequest(
        project=source.project,
        dataset_id=source.dataset_id,
        table_id=table_id,
        template_schema=validate_identifier(options.template_schema or config.schema),
        excluded_columns=tuple(options.excluded_columns or ()),
        compute_column_metrics=options.compute_column_metrics,
        raw_text_encoding=options.raw_text_encoding,
    )
    report = validate_table(config=config, request=request)
    status = "OK" if report.divergences == 0 else f"{report.divergences} DIVERGÊNCIA(S)"
    logger.info(f"Validação de {report.target}: {status}")

    for section in report.sections:
        if report.divergences == 0:
            logger.info(section)
        else:
            logger.warning(section)

    return Outcome(source=report.source, target=report.target, divergences=report.divergences)


@task(cache_policy=NO_CACHE)
def summarize_task(results: list[Outcome], options: Options) -> None:
    """Log the summary of each table.

    Fail if a table differs and `fail_on_divergence` is on.

    Raise `ValueError` for a divergence.
    """
    rows = [[result.source, result.target, "OK" if not result.divergences else "DIVERGE"] for result in results]
    total = sum(result.divergences for result in results)
    table = format_text_table(["Origem", "Destino", "Status"], rows)
    logger.info(f"Resumo: {len(results)} tabela(s), {total} divergência(s)\n{table}")

    if total and options.fail_on_divergence:
        raise ValueError(f"Validação encontrou {total} divergência(s). Veja o relatório de cada tabela acima.")
