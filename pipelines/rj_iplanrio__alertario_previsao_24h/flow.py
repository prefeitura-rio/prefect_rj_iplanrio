# -*- coding: utf-8 -*-
"""
Flow para extrair previsões meteorológicas do AlertaRio e carregar no BigQuery
"""

import os
from datetime import datetime, timezone

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__alertario_previsao_24h.constants import (
    AlertaRioConstants,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.alerting import (
    build_alert_log_rows,
    compute_message_hash,
    ensure_alert_log_table,
    extract_precipitation_alerts,
    fetch_daily_alert_status,
    format_precipitation_alert_message,
    get_bigquery_client,
    insert_alert_log_rows,
    send_discord_webhook_message,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.tasks import (
    create_dim_mares_df,
    create_dim_previsao_periodo_df,
    create_dim_quadro_sinotico_df,
    create_dim_temperatura_zona_df,
    fetch_xml_from_url,
    parse_xml_to_dict,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.utils.tasks import (
    create_date_partitions,
)


@flow(log_prints=True)
def rj_iplanrio__alertario_previsao_24h(
    dataset_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    send_discord_alerts: bool = False,
    discord_webhook_env_var: str | None = None,
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
        discord_webhook_env_var: Nome da env var com o webhook (default: DISCORD_WEBHOOK_URL_ALERTARIO).
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
    discord_webhook_env_var = (
        discord_webhook_env_var or AlertaRioConstants.DISCORD_WEBHOOK_ENV_VAR.value
    )
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
    # Sempre usar dataset_staging para consistência com dim_* tables
    # que são criadas pela BD+ em modo staging
    alert_log_dataset_id = f"{dataset_id}_staging"
    log(f"[Alert] Using dataset_id for alerts: {alert_log_dataset_id} (base: {dataset_id})")

    # Renomear flow run para melhor identificação
    rename_current_flow_run_task(new_name=f"alertario_previsao_24h_{dataset_id}")

    # Injetar credenciais do BD
    inject_bd_credentials_task(environment="prod")

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
            log(
                (
                    f"[Alert Debug] {len(precipitation_alerts)} combos relevantes. "
                    f"hash={message_hash} tamanho={len(discord_message)} caracteres."
                ),
                level="info",
            )
        except Exception as error:  # pylint: disable=broad-except
            log(f"Erro ao montar mensagem de alerta: {error}", level="error")
            discord_message = None
            message_hash = None

    if not send_discord_alerts:
        log(
            "Envio automático de alertas no Discord está desativado para depuração.",
            level="warning",
        )

    if send_discord_alerts and precipitation_alerts and discord_message:
        webhook_url = os.getenv(discord_webhook_env_var or "")
        if not webhook_url:
            log(
                f"Variável {discord_webhook_env_var} não configurada. Alerta não será enviado.",
                level="warning",
            )
        elif max_daily_alerts <= 0:
            log(
                f"max_daily_alerts={max_daily_alerts} inválido. Ignorando envio de alerta.",
                level="warning",
            )
        else:
            try:
                now_utc = datetime.now(timezone.utc)
                alert_date = now_utc.date()
                bq_client = get_bigquery_client(project_id=billing_project_id)
                if bq_client is None:
                    log(
                        "Credenciais do BigQuery indisponíveis. Pulando envio de alerta.",
                        level="warning",
                    )
                else:
                    ensure_alert_log_table(
                        client=bq_client,
                        dataset_id=alert_log_dataset_id,
                        table_id=alert_log_table_id,
                    )
                    sent_count, sent_hashes, last_sent_at = fetch_daily_alert_status(
                        client=bq_client,
                        dataset_id=alert_log_dataset_id,
                        table_id=alert_log_table_id,
                        alert_date=alert_date,
                    )
                    debug_last_sent = (
                        last_sent_at.isoformat() if last_sent_at else "nunca"
                    )
                    log(
                        (
                            "[Alert Debug] status diário: enviados="
                            f"{sent_count}, último={debug_last_sent}, hashes={sorted(sent_hashes)}, "
                            f"intervalo_mínimo={min_alert_interval_hours}h"
                        ),
                        level="info",
                    )

                    if last_sent_at and last_sent_at.tzinfo is None:
                        last_sent_at = last_sent_at.replace(tzinfo=timezone.utc)

                    if message_hash in sent_hashes:
                        log(
                            "Alerta de precipitação já enviado com o mesmo conteúdo hoje. Ignorando duplicado.",
                            level="warning",
                        )
                    elif sent_count >= max_daily_alerts:
                        log(
                            f"Limite diário de {max_daily_alerts} alertas atingido. Mensagem será descartada.",
                            level="warning",
                        )
                    elif (
                        last_sent_at
                        and (now_utc - last_sent_at).total_seconds()
                        < min_alert_interval_hours * 3600
                    ):
                        hours_since_last = (
                            now_utc - last_sent_at
                        ).total_seconds() / 3600
                        log(
                            f"Último alerta enviado há {hours_since_last:.2f}h. Aguardando {min_alert_interval_hours}h entre alertas.",
                            level="warning",
                        )
                    else:
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
                        insert_alert_log_rows(
                            client=bq_client,
                            dataset_id=alert_log_dataset_id,
                            table_id=alert_log_table_id,
                            rows=log_rows,
                        )
                        log(
                            "Alerta de precipitação enviado ao Discord e registrado no BigQuery."
                        )
            except Exception as error:  # pylint: disable=broad-except
                log(f"Erro ao processar alerta do Discord: {error}", level="error")
    elif send_discord_alerts:
        log("Sem alertas de precipitação para enviar ao Discord.")
    elif precipitation_alerts and discord_message and message_hash:
        preview = discord_message.splitlines()[0:5]
        log(
            (
                "Envio de alerta desativado. Mensagem não enviada "
                f"(hash={message_hash}). Prévia:\n" + "\n".join(preview)
            ),
            level="info",
        )

    # Upload tabela 1: dim_quadro_sinotico
    root_folder_1 = AlertaRioConstants.ROOT_FOLDER.value + "dim_quadro_sinotico/"
    partitions_path_1 = create_date_partitions(
        dataframe=df_dim_sinotico,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_1,
    )
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path_1,
        dataset_id=dataset_id,
        table_id=AlertaRioConstants.TABLE_DIM_QUADRO_SINOTICO.value,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    # Upload tabela 2: dim_previsao_periodo
    root_folder_2 = AlertaRioConstants.ROOT_FOLDER.value + "dim_previsao_periodo/"
    partitions_path_2 = create_date_partitions(
        dataframe=df_dim_periodo,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_2,
    )
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path_2,
        dataset_id=dataset_id,
        table_id=AlertaRioConstants.TABLE_DIM_PREVISAO_PERIODO.value,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    # Upload tabela 3: dim_temperatura_zona
    root_folder_3 = AlertaRioConstants.ROOT_FOLDER.value + "dim_temperatura_zona/"
    partitions_path_3 = create_date_partitions(
        dataframe=df_dim_temperatura,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_3,
    )
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path_3,
        dataset_id=dataset_id,
        table_id=AlertaRioConstants.TABLE_DIM_TEMPERATURA_ZONA.value,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    root_folder_4 = AlertaRioConstants.ROOT_FOLDER.value + "dim_mares/"
    partitions_path_4 = create_date_partitions(
        dataframe=df_dim_mares,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_4,
    )
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path_4,
        dataset_id=dataset_id,
        table_id=AlertaRioConstants.TABLE_DIM_MARES.value,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )
