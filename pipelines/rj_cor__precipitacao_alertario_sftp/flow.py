"""Flow para coleta de precipitação do AlertaRio via SFTP em landing zone GCS."""

from prefect import flow
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_cor__precipitacao_alertario_sftp.tasks import (
    download_xml_from_gcs_task,
    list_xml_files_from_gcs_task,
    parse_xml_task,
    save_meteorological_data_to_parquet_task,
    save_pluviometric_data_to_parquet_task,
    transform_meteorological_data_task,
    transform_pluviometric_data_task,
)

logger = get_logger(__name__)


@flow(log_prints=True)
def rj_cor__precipitacao_alertario_sftp(
    bucket_landing_zone: str = "rj-iplanrio-filemage.nimbus",
    dataset_id_pluviometric: str = "clima_pluviometro",
    table_id_pluviometric: str = "taxa_precipitacao_alertario_5min",
    dataset_id_meteorological: str = "clima_estacao_meteorologica",
    table_id_meteorological: str = "meteorologia_alertario",
    dump_mode: str = "append",
) -> None:
    """Coleta dados de precipitação do AlertaRio via arquivos XML em GCS.

    Flow que processa arquivos XML depositados na landing zone GCS por
    sistema SFTP. Extrai dados de precipitação e meteorologia, transforma
    para formato padrão e carrega em tabelas BigQuery particionadas.

    O flow lista arquivos XML no bucket especificado, faz parsing de cada
    um, transforma os dados em dois DataFrames distintos (pluviométrico e
    meteorológico), salva como Parquet particionado e faz upload para o
    BigQuery.

    :param bucket_landing_zone: Nome do bucket GCS da landing zone
        (ex: rj-iplanrio-filemage.nimbus).
    :param dataset_id_pluviometric: ID do dataset BigQuery para dados
        pluviométricos (ex: clima_pluviometro).
    :param table_id_pluviometric: ID da tabela pluviométrica
        (ex: taxa_precipitacao_alertario_5min).
    :param dataset_id_meteorological: ID do dataset BigQuery para dados
        meteorológicos (ex: clima_estacao_meteorologica).
    :param table_id_meteorological: ID da tabela meteorológica
        (ex: meteorologia_alertario).
    :param dump_mode: Modo de salvamento no BigQuery ("append" ou "overwrite").

    Returns:
        None
    """
    rename_current_flow_run_task(new_name="precipitacao-alertario-sftp")
    inject_bd_credentials_task(environment="prod")

    logger.info("🌧️  Iniciando coleta de dados de precipitação AlertaRio via SFTP")

    # Step 1: Listar arquivos XML novos
    logger.info("📥 Listando arquivos XML na landing zone...")
    xml_files = list_xml_files_from_gcs_task(
        bucket_name=bucket_landing_zone,
        prefix="Chuvas_",
    )

    if not xml_files:
        logger.warning("⚠️  Nenhum arquivo XML encontrado na landing zone")
        return

    logger.info("✅ Encontrados %d arquivo(s) XML", len(xml_files))

    # Step 2-7: Processar cada arquivo
    for file_name in xml_files:
        logger.info("📄 Processando arquivo: %s", file_name)

        # Download do XML
        xml_content = download_xml_from_gcs_task(
            bucket_name=bucket_landing_zone,
            file_name=file_name,
        )

        # Parse do XML
        dfr_pluv, dfr_met = parse_xml_task(
            xml_content=xml_content,
            source_file=file_name,
        )

        # Transformação de dados
        dfr_pluv_transformed = transform_pluviometric_data_task(dfr_pluv)
        dfr_met_transformed = transform_meteorological_data_task(dfr_met)

        # Salvamento em Parquet
        pluv_path = save_pluviometric_data_to_parquet_task(dfr_pluv_transformed)
        met_path = save_meteorological_data_to_parquet_task(dfr_met_transformed)

        # Upload para BigQuery
        if not dfr_pluv_transformed.empty:
            logger.info(
                "☁️  Fazendo upload dos dados pluviométricos para BigQuery "
                "(%s.%s)",
                dataset_id_pluviometric,
                table_id_pluviometric,
            )
            create_table_and_upload_to_gcs_task(
                data_path=str(pluv_path),
                dataset_id=dataset_id_pluviometric,
                table_id=table_id_pluviometric,
                dump_mode=dump_mode,
            )
            logger.info("✅ Dados pluviométricos salvos")
        else:
            logger.warning(
                "⚠️  Nenhum dado pluviométrico para arquivo %s", file_name
            )

        if not dfr_met_transformed.empty:
            logger.info(
                "☁️  Fazendo upload dos dados meteorológicos para BigQuery "
                "(%s.%s)",
                dataset_id_meteorological,
                table_id_meteorological,
            )
            create_table_and_upload_to_gcs_task(
                data_path=str(met_path),
                dataset_id=dataset_id_meteorological,
                table_id=table_id_meteorological,
                dump_mode=dump_mode,
            )
            logger.info("✅ Dados meteorológicos salvos")
        else:
            logger.warning(
                "⚠️  Nenhum dado meteorológico para arquivo %s", file_name
            )

        logger.info("✅ Arquivo %s processado com sucesso", file_name)

    logger.info("✅ Flow concluído com sucesso!")
