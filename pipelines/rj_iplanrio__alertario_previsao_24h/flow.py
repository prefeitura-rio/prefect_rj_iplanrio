"""
Flow para extrair previsões meteorológicas do AlertaRio e carregar no BigQuery
"""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from prefect import flow
from prefect.logging import get_logger

from pipelines.rj_iplanrio__alertario_previsao_24h.alerting import (
    build_alert_log_rows,
    check_alert_deduplication,
    compute_message_hash,
    extract_precipitation_alerts,
    format_precipitation_alert_message,
    send_discord_webhook_message,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.constants import (
    AlertaRioConstants,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.helpers.tasks import (
    create_date_partitions,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.tasks import (
    create_dim_mares_df,
    create_dim_previsao_periodo_df,
    create_dim_quadro_sinotico_df,
    create_dim_temperatura_zona_df,
    fetch_xml_from_url,
    parse_xml_to_dict,
)
from utils.settings import NoticeSettings
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options

logger = get_logger("toolkit")


@flow(log_prints=True, flow_run_name="alertario_previsao_24h_{dataset_id}")
async def rj_iplanrio__alertario_previsao_24h(
    *,
    cloud: Cloud,
    dataset_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    send_discord_alerts: bool = False,
    max_daily_alerts: int | None = None,
    min_alert_interval_hours: int | None = None,
):
    """
    Flow para extrair previsões meteorológicas do AlertaRio e carregar no BigQuery.

    Busca o XML de previsão do AlertaRio, faz parsing e carrega em 4 tabelas simples:
    - dim_quadro_sinotico: Uma linha por execução (quadro sinótico)
    - dim_previsao_periodo: Uma linha por previsão
    - dim_temperatura_zona: Uma linha por zona
    - dim_mares: Uma linha por tábua de maré

    Todas as tabelas compartilham o mesmo id_execucao (UUID) para permitir joins.

    Args:
        dataset_id: ID do dataset no BigQuery (default: brutos_alertario)
        dump_mode: Modo de dump (default: append)
        materialize_after_dump: Se deve materializar após dump (default: False)
        send_discord_alerts: Ativa/desativa envio automático de alerta no Discord.
        max_daily_alerts: Limite de mensagens por dia (default: 2).
        min_alert_interval_hours: Intervalo mínimo em horas entre alertas.
    """

    dataset_id = dataset_id or AlertaRioConstants.DATASET_ID.value
    dump_mode = dump_mode or AlertaRioConstants.DUMP_MODE.value
    materialize_after_dump = (
        materialize_after_dump
        if materialize_after_dump is not None
        else AlertaRioConstants.MATERIALIZE_AFTER_DUMP.value
    )

    file_format = AlertaRioConstants.FILE_FORMAT.value
    biglake_table = AlertaRioConstants.BIGLAKE_TABLE.value
    partition_column = AlertaRioConstants.PARTITION_COLUMN.value
    max_daily_alerts = (
        max_daily_alerts
        if max_daily_alerts is not None
        else AlertaRioConstants.DEFAULT_MAX_DAILY_ALERTS.value
    )
    alert_log_table_id = AlertaRioConstants.TABLE_ALERT_LOG.value
    billing_project_id = AlertaRioConstants.BILLING_PROJECT_ID.value
    discord_channel_label = "alertario_precipitacao"
    min_alert_interval_hours = (
        min_alert_interval_hours
        if min_alert_interval_hours is not None
        else AlertaRioConstants.MIN_ALERT_INTERVAL_HOURS.value
    )

    # Inicializar lista para acumular alertas (será processada no final com as dim_* tables)
    alert_log_rows_to_save = []

    # Renomear flow run para melhor identificação

    # Injetar credenciais do BD

    # Buscar XML do AlertaRio
    xml_content = fetch_xml_from_url()

    # Fazer parsing do XML
    parsed_data = parse_xml_to_dict(xml_content)

    # Criar DataFrames para cada tabela
    df_dim_sinotico = create_dim_quadro_sinotico_df(parsed_data)
    df_dim_periodo = create_dim_previsao_periodo_df(parsed_data)
    df_dim_temperatura = create_dim_temperatura_zona_df(parsed_data)
    df_dim_mares = create_dim_mares_df(parsed_data)

    precipitation_alerts = extract_precipitation_alerts(df_dim_periodo)
    discord_message: str | None = None
    message_hash: str | None = None

    if precipitation_alerts:
        try:
            discord_message = format_precipitation_alert_message(
                precipitation_alerts,
                synoptic_summary=parsed_data.get("quadro_sinotico"),
                synoptic_reference_date=parsed_data.get("create_date"),
            )
            message_hash = compute_message_hash(discord_message)
            logger.info(
                (
                    f"[Alert Debug] {len(precipitation_alerts)} combos relevantes. "
                    f"hash={message_hash} tamanho={len(discord_message)} caracteres."
                ),
            )
        except Exception as error:  # pylint: disable=broad-except
            logger.error(f"Erro ao montar mensagem de alerta: {error}")
            discord_message = None
            message_hash = None

    if not send_discord_alerts:
        logger.warning(
            "Envio automático de alertas no Discord está desativado para depuração.",
        )

    if send_discord_alerts and precipitation_alerts and discord_message:
        webhook_url = NoticeSettings().DISCORD_WEBHOOK_URL.get_secret_value()
        if not webhook_url:
            logger.warning(
                "DISCORD_WEBHOOK_URL não configurada. Alerta não será enviado.",
            )
        else:
            try:
                now_utc = datetime.now(timezone.utc)
                alert_date = now_utc.date()

                # Verificar regras de deduplicação antes de enviar
                try:
                    should_send, reason = check_alert_deduplication(
                        alert_hash=message_hash,
                        billing_project_id=billing_project_id,
                        max_daily_alerts=max_daily_alerts,
                        min_alert_interval_hours=min_alert_interval_hours,
                    )
                    logger.info(f"[Deduplication] {reason}")

                    if not should_send:
                        logger.warning("Alerta pulado devido a regras de deduplicação.")
                        # Não envia, não salva log (pula para próxima seção)
                except Exception as dedup_error:
                    logger.error(f"Erro ao verificar deduplicação: {dedup_error}")
                    logger.error("Pulando envio de alerta por segurança (query falhou).")
                    # Não envia se query falhar (comportamento conservador)
                else:
                    # Só envia se should_send == True
                    if should_send:
                        discord_response = send_discord_webhook_message(
                            webhook_url=webhook_url,
                            message=discord_message,
                        )
                        discord_message_id = discord_response.get("id")
                        sent_at = now_utc
                        log_rows = build_alert_log_rows(
                            alert_date=alert_date,
                            id_execucao=parsed_data["id_execucao"],
                            alert_hash=message_hash,
                            alerts=precipitation_alerts,
                            sent_at=sent_at,
                            discord_message_id=discord_message_id,
                            webhook_channel=discord_channel_label,
                            message_excerpt=discord_message,
                            severity_level="info",
                        )
                        alert_log_rows_to_save.extend(log_rows)
                        logger.info(
                            "Alerta de precipitação enviado ao Discord. Log será salvo junto com as outras tabelas."
                        )
            except Exception as error:  # pylint: disable=broad-except
                logger.error(f"Erro ao processar alerta do Discord: {error}")
    elif send_discord_alerts:
        logger.info("Sem alertas de precipitação para enviar ao Discord.")
    elif precipitation_alerts and discord_message and message_hash:
        preview = discord_message.splitlines()[0:5]
        logger.info(
            (
                "Envio de alerta desativado. Mensagem não enviada "
                f"(hash={message_hash}). Prévia:\n" + "\n".join(preview)
            ),
        )

    # Upload tabela 1: dim_quadro_sinotico
    root_folder_1 = AlertaRioConstants.ROOT_FOLDER.value + "dim_quadro_sinotico/"
    partitions_path_1 = create_date_partitions(
        dataframe=df_dim_sinotico,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_1,
    )
    table = Table(dataset_id=dataset_id, table_id=AlertaRioConstants.TABLE_DIM_QUADRO_SINOTICO.value)
    await upload_files_to_gcs(
        Path(partitions_path_1),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)

    # Upload tabela 2: dim_previsao_periodo
    root_folder_2 = AlertaRioConstants.ROOT_FOLDER.value + "dim_previsao_periodo/"
    partitions_path_2 = create_date_partitions(
        dataframe=df_dim_periodo,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_2,
    )
    table = Table(dataset_id=dataset_id, table_id=AlertaRioConstants.TABLE_DIM_PREVISAO_PERIODO.value)
    await upload_files_to_gcs(
        Path(partitions_path_2),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)

    # Upload tabela 3: dim_temperatura_zona
    root_folder_3 = AlertaRioConstants.ROOT_FOLDER.value + "dim_temperatura_zona/"
    partitions_path_3 = create_date_partitions(
        dataframe=df_dim_temperatura,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_3,
    )
    table = Table(dataset_id=dataset_id, table_id=AlertaRioConstants.TABLE_DIM_TEMPERATURA_ZONA.value)
    await upload_files_to_gcs(
        Path(partitions_path_3),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)

    root_folder_4 = AlertaRioConstants.ROOT_FOLDER.value + "dim_mares/"
    partitions_path_4 = create_date_partitions(
        dataframe=df_dim_mares,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_4,
    )
    table = Table(dataset_id=dataset_id, table_id=AlertaRioConstants.TABLE_DIM_MARES.value)
    await upload_files_to_gcs(
        Path(partitions_path_4),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)

    # Upload tabela 5: alertario_precipitacao_alerts_log (se houver alertas enviados)
    if alert_log_rows_to_save:
        df_alerts = pd.DataFrame(alert_log_rows_to_save)
        root_folder_5 = (
            AlertaRioConstants.ROOT_FOLDER.value + "alertario_precipitacao_alerts_log/"
        )
        partitions_path_5 = create_date_partitions(
            dataframe=df_alerts,
            partition_column="alert_date",  # Diferente das dim_* que usam data_particao
            file_format=file_format,
            root_folder=root_folder_5,
        )
        table = Table(dataset_id=dataset_id, table_id=alert_log_table_id)
        await upload_files_to_gcs(
            Path(partitions_path_5),
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="csv",
            options=capture_options(dump_mode),
        )
        await publish_table_in_bq(table, cloud)
    else:
        logger.info(
            "Nenhum alerta foi enviado nesta execução. Tabela de alerts não será atualizada."
        )
