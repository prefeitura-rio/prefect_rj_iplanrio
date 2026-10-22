"""Tasks da validação das tabelas carregadas no Oracle pela rj_smfp__nota_carioca_bq_to_oracle."""

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.bigquery import list_tables
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import RawTextEncoding
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import read_oracle_config, validate_identifier
from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_logger

from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.inspect import (
    ValidationRequest,
    describe_session,
    validate_table,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.report import format_text_table

logger = get_logger("toolkit")


@task
def list_tables_task(project: str, dataset_id: str, table_ids: list[str] | None) -> list[str]:
    """Retorna as tabelas pedidas ou, se nenhuma for informada, todas as do dataset."""
    return table_ids or list_tables(project=project, dataset_id=dataset_id)


@task(cache_policy=NO_CACHE)
def log_session_task(infisical_secret_path: str) -> None:
    """Registra no log com qual usuário, banco e versão a validação está conectada."""
    logger.info("Sessão no Oracle\n" + describe_session(read_oracle_config(infisical_secret_path)))


@task(cache_policy=NO_CACHE)
def validate_table_task(  # noqa: PLR0913
    infisical_secret_path: str,
    project: str,
    dataset_id: str,
    table_id: str,
    template_schema: str | None,
    excluded_template_columns: list[str] | None,
    compute_column_metrics: bool,
    raw_text_encoding: RawTextEncoding,
) -> dict[str, object]:
    """Valida uma tabela, registra o relatório no log e retorna o resumo."""
    config = read_oracle_config(infisical_secret_path)
    request = ValidationRequest(
        project=project,
        dataset_id=dataset_id,
        table_id=table_id,
        template_schema=validate_identifier(template_schema or config.schema),
        excluded_columns=tuple(excluded_template_columns or ()),
        compute_column_metrics=compute_column_metrics,
        raw_text_encoding=raw_text_encoding,
    )
    report = validate_table(config=config, request=request)
    status = "OK" if report.divergences == 0 else f"{report.divergences} DIVERGÊNCIA(S)"
    logger.info(f"Validação de {report.target}: {status}")
    for section in report.sections:
        getattr(logger, "info" if report.divergences == 0 else "warning")(section)
    return {"source": report.source, "target": report.target, "divergences": report.divergences}


@task(cache_policy=NO_CACHE)
def summarize_task(results: list[dict[str, object]], fail_on_divergence: bool) -> None:
    """Registra o resumo de todas as tabelas e falha o run se houver divergência.

    :raises ValueError: Se ``fail_on_divergence`` for verdadeiro e alguma tabela
        divergir.
    """
    rows = [
        [str(result["source"]), str(result["target"]), "OK" if not result["divergences"] else "DIVERGE"]
        for result in results
    ]
    total = sum(int(result["divergences"]) for result in results)
    logger.info(
        f"Resumo: {len(results)} tabela(s), {total} divergência(s)\n"
        + format_text_table(["Origem", "Destino", "Status"], rows)
    )
    if total and fail_on_divergence:
        raise ValueError(f"Validação encontrou {total} divergência(s). Veja o relatório de cada tabela acima.")
