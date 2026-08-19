"""Flow para coleta de precipitação do AlertaRio via SFTP em landing zone GCS."""

from prefect import flow
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect_rj_iplanrio.logging import get_logger
from iplanrio.pipelines_utils.bd import (
    create_table_and_upload_to_gcs_task,
)
from iplanrio.pipelines_utils.env import getenv_or_action
from tasks import (
    get_max_date_from_bigquery_task,
    get_bucket_files_with_datetime_filter_task,
    process_multiple_xml_files_task,
    download_xml_files_from_list_task

)


logger = get_logger(__name__)


@flow(log_prints=True, name="rj-cor-precipitacao-alertario-sftp")
def rj_cor__precipitacao_alertario_sftp(
    dataset_id_pluviometric: str = 'clima_pluviometro',
    table_id_pluviometric: str = 'taxa_precipitacao_alertario_5min',
    dataset_id_meteorological: str = 'clima_estacao_meteorologica',
    table_id_meteorological: str = 'meteorologia_alertario',
    dump_mode: str = "append",
    project_id: str = "rj-iplanrio",
) -> None:
    """Coleta dados de precipitação do AlertaRio via arquivos XML em GCS.

    Flow que processa arquivos XML depositados na landing zone GCS por
    sistema SFTP. Extrai dados de precipitação e meteorologia, transforma
    para formato padrão e carrega em tabelas BigQuery particionadas.

    **Processo:**
    1. Renomeia execução do flow
    2. Injeta credenciais do banco de dados
    3. Lista arquivos XML no bucket da landing zone
    4. Para cada arquivo:
        - Faz download do XML do GCS
        - Faz parse e extrai dados pluviométricos e meteorológicos
        - Transforma dados para formato padrão
        - Salva dados como csv particionado
        - Faz upload para BigQuery em tabelas separadas

    **Parâmetros:**

    :param bucket_landing_zone: Nome do bucket GCS da landing zone
        (padrão: rj-iplanrio-filemage.nimbus).
    :param dataset_id_pluviometric: ID do dataset BigQuery para dados
        pluviométricos (padrão: clima_pluviometro).
    :param table_id_pluviometric: ID da tabela pluviométrica
        (padrão: taxa_precipitacao_alertario_5min).
    :param dataset_id_meteorological: ID do dataset BigQuery para dados
        meteorológicos (padrão: clima_estacao_meteorologica).
    :param table_id_meteorological: ID da tabela meteorológica
        (padrão: meteorologia_alertario).
    :param dump_mode: Modo de salvamento no BigQuery
        (padrão: "append", alternativa: "overwrite").

    **Returns:**
        None
    """
    # Setup
    rename_current_flow_run_task(new_name="precipitacao-alertario-sftp")
    inject_bd_credentials_task(environment="prod")

    bucket_name = getenv_or_action("bucket-nimbus")
    prefix = getenv_or_action("prefix")
    print("🌧️  Iniciando coleta de dados de precipitação AlertaRio via SFTP")

    # Step 1: Listar arquivos XML novos
    print("📥 Listando arquivos XML na landing zone...")
    bq = get_max_date_from_bigquery_task(
        project_id=project_id
    )

    xml_files = get_bucket_files_with_datetime_filter_task(
        max_datetime_from_bq=bq,
        bucket_name=bucket_name,
        prefix=prefix
    )

    content = download_xml_files_from_list_task(
        bucket_name=bucket_name,
        file_names=xml_files
    )

    pluviometric_path, meteorological_path = process_multiple_xml_files_task(
        xml_contents=content
    )

    if dataset_id_pluviometric is not None and pluviometric_path is not None:
        print("📤 Enviando dados pluviométricos para BigQuery: %s", pluviometric_path)
        create_table_and_upload_to_gcs_task(
            data_path=pluviometric_path,
            dataset_id=dataset_id_pluviometric,
            table_id=table_id_pluviometric,
            dump_mode=dump_mode,
        )

    if dataset_id_meteorological is not None and meteorological_path is not None:
        print("📤 Enviando dados meteorológicos para BigQuery: %s", meteorological_path)
        create_table_and_upload_to_gcs_task(
            data_path=meteorological_path,
            dataset_id=dataset_id_meteorological,
            table_id=table_id_meteorological,
            dump_mode=dump_mode
    )

