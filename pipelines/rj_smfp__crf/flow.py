"""Flow for rj_smfp__crf."""

from typing import Literal

from prefect import flow
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from pipelines.rj_smfp__crf.tasks import (
    get_max_date_from_bigquery_task,
    process_all_crf_zip_files_task,
    process_daf607_files_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task

# Subtabelas geradas pelo parsing DAF607
_DAF607_SUBTABLES = ("daf607_header", "daf607_detalhe", "daf607_trailer")


@flow(log_prints=True)
def rj_smfp__crf(
    table_id: Literal[
        "periodos_simples",
        "periodos_mei",
        "eventos_simples",
        "eventos_mei",
        "daf607",
    ],
    project_id: str = "rj-iplanrio",
    dataset_id: str = "brutos_crf",
    data_inicio: str | None = None,
    data_fim: str | None = None,
) -> None:
    """Processa uma tabela CRF ou DAF607 do GCS e faz upload para o BigQuery.

    Suporta dois modos de operação, determinados pelo valor de ``table_id``:

    **Modo CRF (tabelas FWF via ZIP)**
    Para ``table_id`` em ``periodos_simples``, ``periodos_mei``, ``eventos_simples``
    ou ``eventos_mei``:

    1. Determinar o intervalo de datas a processar:
       - Se ``data_inicio`` e ``data_fim`` forem ambos ``None``, consulta
         automaticamente o BigQuery para obter a data máxima de partição,
         que é usada como ``data_inicio`` (marca d'água).
       - Caso contrário, usa os valores fornecidos para filtrar os ZIPs.
    2. Listar no GCS apenas os arquivos ZIP dentro do intervalo de datas.
    3. Para cada ZIP: baixar, extrair, ler FWF e salvar parquet localmente.
    4. Fazer upload dos parquets para o BigQuery em modo append.

    **Modo DAF607 (parsing linha a linha de arquivos .ret)**
    Para ``table_id == 'daf607'``:

    1. Consulta o BigQuery usando ``daf607_header`` como referência de marca
       d'água (quando ``data_inicio`` e ``data_fim`` são ambos ``None``).
    2. Baixa e faz parsing dos arquivos ``.ret`` do GCS, separando os registros
       em três DataFrames (header, detalhe, trailer).
    3. Salva os três conjuntos como parquets particionados localmente.
    4. Faz upload das três tabelas — ``daf607_header``, ``daf607_detalhe`` e
       ``daf607_trailer`` — para o BigQuery em modo append.

    :param table_id: Identificador da tabela a processar. Obrigatório.
        Valores válidos: ``'periodos_simples'``, ``'periodos_mei'``,
        ``'eventos_simples'``, ``'eventos_mei'``, ``'daf607'``.
    :param project_id: Google Cloud project ID (padrão: ``'rj-iplanrio'``).
    :param dataset_id: ID do dataset BigQuery de destino (padrão: ``'brutos_crf'``).
    :param data_inicio: Data de início do intervalo no formato ``YYYY-MM-DD``.
        Arquivos com data igual ou anterior são ignorados. Se ``None`` junto
        com ``data_fim``, consulta automaticamente o BigQuery.
    :param data_fim: Data de fim do intervalo no formato ``YYYY-MM-DD``.
        Arquivos com data posterior são ignorados. Se ``None``, não aplica
        limite superior. Requer que ``data_inicio`` também seja fornecido.
    """
    rename_current_flow_run_task(new_name=f"{project_id}.{dataset_id}.{table_id}")
    # inject_bd_credentials_task(environment="prod")

    # -------------------------------------------------------------------------
    # Modo DAF607
    # -------------------------------------------------------------------------
    if table_id == "daf607":
        if data_inicio is None and data_fim is None:
            data_inicio = get_max_date_from_bigquery_task(
                project_id=project_id,
                dataset_id=f"{dataset_id}_staging",
                table_id="daf607_header",
            )

        data_paths = process_daf607_files_task(
            project_id=project_id,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )

        if all(data_paths.get(t) is None for t in _DAF607_SUBTABLES):
            log("Nenhum arquivo DAF607 encontrado no intervalo especificado — flow encerrado sem upload.")
            return

        for subtable in _DAF607_SUBTABLES:
            data_path = data_paths.get(subtable)
            if data_path is None:
                log(f"DAF607: Sem dados para '{subtable}', pulando upload.", level="warning")
                continue

            create_table_and_upload_to_gcs_task(
                data_path=data_path,
                dataset_id=dataset_id,
                dump_mode="append",
                source_format="parquet",
                table_id=subtable,
            )

        return

    # -------------------------------------------------------------------------
    # Modo CRF (FWF via ZIP)
    # -------------------------------------------------------------------------
    if data_inicio is None and data_fim is None:
        data_inicio = get_max_date_from_bigquery_task(
            project_id=project_id,
            dataset_id=f"{dataset_id}_staging",
            table_id=table_id,
        )

    data_path = process_all_crf_zip_files_task(
        project_id=project_id,
        table_id=table_id,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

    if data_path is None:
        log("Nenhum arquivo ZIP encontrado no intervalo especificado — flow encerrado sem upload.")
        return

    create_table_and_upload_to_gcs_task(
        data_path=data_path,
        dataset_id=dataset_id,
        dump_mode="append",
        source_format="parquet",
        table_id=table_id,
    )

rj_smfp__crf(
    table_id="daf607",
    project_id="rj-iplanrio",
    dataset_id="brutos_crf",
    data_inicio="2020-01-01",
    data_fim="2020-02-29",
)