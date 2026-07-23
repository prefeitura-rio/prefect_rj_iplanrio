# -*- coding: utf-8 -*-
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.dbt import execute_dbt_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow
from prefect.deployments import run_deployment
# from pipelines.rj_smas__disparo_cadunico.flow import rj_smas__disparo_cadunico

from pipelines.rj_smas__api_datametrica_agendamentos.constants import (
    DatametricaConstants,
)
from pipelines.rj_smas__api_datametrica_agendamentos.tasks import (
    calculate_target_date,
    convert_agendamentos_to_dataframe,
    fetch_agendamentos_from_api,
    get_bigquery_config,
    get_datametrica_credentials,
    transform_agendamentos_data,
)
from pipelines.rj_smas__api_datametrica_agendamentos.utils.tasks import (
    create_date_partitions,
)


@flow(log_prints=True)
def rj_smas__api_datametrica_agendamentos(
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    date: str | None = None,
    infisical_secret_path: str | None = "/api-datametrica",
):
    """
    Flow para extrair agendamentos da API Datametrica e carregar no BigQuery.

    Regra de negócio para datas:
    - Dias normais: busca dados para 2 dias à frente
    - Quinta e sexta-feira: busca dados para 4 dias à frente (cobrindo fim de semana)

    Args:
        dataset_id: ID do dataset no BigQuery (default: None = obtém do Infisical)
        table_id: ID da tabela no BigQuery (default: None = obtém do Infisical)
        dump_mode: Modo de dump (default: append)
        materialize_after_dump: Se deve materializar após dump (default: True)
        date: Data para buscar agendamentos no formato YYYY-MM-DD (default: None = usa regra de negócio)
        infisical_secret_path: Caminho dos secrets no Infisical (default: /api-datametrica)
    """

    # Obter configuração do BigQuery do Infisical se não fornecida via parâmetros
    if dataset_id is None or table_id is None:
        bigquery_config = get_bigquery_config(infisical_secret_path=infisical_secret_path)
        dataset_id = dataset_id or bigquery_config["dataset_id"]
        table_id = table_id or bigquery_config["table_id"]

    # Usar valores dos constants como padrão para outros parâmetros
    dump_mode = dump_mode or DatametricaConstants.DUMP_MODE.value
    materialize_after_dump = (
        materialize_after_dump
        if materialize_after_dump is not None
        else DatametricaConstants.MATERIALIZE_AFTER_DUMP.value
    )

    partition_column = DatametricaConstants.PARTITION_COLUMN.value
    file_format = DatametricaConstants.FILE_FORMAT.value
    root_folder = DatametricaConstants.ROOT_FOLDER.value
    biglake_table = DatametricaConstants.BIGLAKE_TABLE.value

    # Renomear flow run para melhor identificação
    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")

    # Injetar credenciais do BD
    crd = inject_bd_credentials_task(environment="prod")  # noqa

    # Obter credenciais da API Datametrica
    credentials = get_datametrica_credentials(infisical_secret_path=infisical_secret_path)

    # Calcular data target baseada na regra de negócio (a menos que date seja fornecido explicitamente)
    target_date = date if date is not None else calculate_target_date()

    # Buscar dados da API
    raw_data = fetch_agendamentos_from_api(credentials=credentials, date=target_date)

    # Transformar os dados
    processed_data = transform_agendamentos_data(raw_data)

    # Converter para DataFrame
    df = convert_agendamentos_to_dataframe(processed_data)

    # Criar partições por data
    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder,
    )

    create_table_and_upload_to_gcs_task(
        data_path=partitions_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    if materialize_after_dump:
        dbt_select = "raw_cadunico_agendamentos"
        execute_dbt_task(select=dbt_select, target="prod")
    
    print("\n\n******* Triggering cadunico dispatch flow... *******")

    cadunico_params = {
        "campaign_name": "confirma_agendamento_cadunico_prod_v2",
        "query_replacements": {"days_ahead_placeholder": 2, "nome_hsm_placeholder": "confirma_agendamento_cadunico_prod_v2",
            "intervalo_filtro_disparados": 1, "id_hsm_legado_placeholder": 101},
        "data_extension_filename": "whatsapp_cadunico_",
        "de_columns": ["nome_sobrenome", "unidade_cras", "data", "hora", "endereco"],
        "dataset_id": "brutos_salesforce",
        "table_id": "disparos_efetuados",
        "dump_mode": "append",
        "test_mode": False,
        # "chunk_size": 1000,
        "sleep_minutes": 1,
        "flow_environment": "production",
        # "query_processor_name": "skip_weekends",
        # "days_ahead": "2",
        "filter_dispatched_phones_or_cpfs": None,
        "filter_duplicated_phones": False,
        "filter_duplicated_cpfs": True,
        "whitelist_percentage": 0,
        "infisical_secret_path": "/crm_disparo_template",
        "whitelist_environment": "production",
        "query": """WITH
                    segmentacao_original AS (
                        SELECT
                            LPAD(CAST(cpf AS STRING), 11, '0') AS cpf,
                            `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(
                                REGEXP_REPLACE(telefone, r'[^\d]', '')
                            ) AS celular_disparo,
                            INITCAP(
                                IF(
                                    ARRAY_LENGTH(SPLIT(nome_completo, ' ')) > 1,
                                    CONCAT(
                                        SPLIT(nome_completo, ' ')[SAFE_OFFSET(0)],
                                        ' ',
                                        SPLIT(nome_completo, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(nome_completo, ' ')) - 1)]
                                    ),
                                    nome_completo
                                )
                            ) AS nome_sobrenome,
                            unidade_nome AS unidade_cras,
                            CONCAT(unidade_endereco, ' - ', unidade_bairro) AS endereco,
                            FORMAT_DATETIME('%d/%m/%Y', CAST(data_hora AS DATETIME)) AS data,
                            FORMAT_DATETIME('%H:%M', CAST(data_hora AS DATETIME)) AS hora
                        FROM `rj-iplanrio.brutos_data_metrica_staging.cadunico_agendamentos`
                        WHERE
                            DATE(CAST(data_hora AS DATETIME)) = DATE_ADD(
                                CURRENT_DATE('America/Sao_Paulo'),
                                INTERVAL CAST({days_ahead_placeholder} AS int64) DAY
                            )
                    ),

                    filtra_disparados AS (
                        -- verifica se esse cpf já recebeu essa mesma mensagem nos últimos x dias
                        SELECT segmentacao_original.*
                        FROM segmentacao_original
                        LEFT JOIN `rj-crm-registry.brutos_salesforce.status_disparo` sd
                            ON sd.cpf = segmentacao_original.cpf
                            AND sd.nome_hsm = '{nome_hsm_placeholder}'
                            AND sd.envio_datahora >= DATETIME_SUB(
                                CURRENT_DATETIME('America/Sao_Paulo'),
                                INTERVAL {intervalo_filtro_disparados} DAY
                            )
                            AND sd.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL {intervalo_filtro_disparados} DAY)
                            AND sd.indicador_quarentena = FALSE
                        LEFT JOIN `rj-crm-registry.crm_whatsapp.telefone_disparado` td
                            ON td.contato_telefone = segmentacao_original.celular_disparo
                            AND td.id_hsm = CAST({id_hsm_legado_placeholder} AS STRING)
                            AND td.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL {intervalo_filtro_disparados} DAY)
                        WHERE sd.cpf IS NULL AND td.contato_telefone IS NULL
                    ),

                    filtra_celulares_sem_whats AS (
                        -- remove telefones em quarentena ou com qualidade inválida
                        SELECT DISTINCT s.*
                        FROM filtra_disparados AS s
                        LEFT JOIN `rj-crm-registry.intermediario_rmi_telefones.int_telefone` AS tel
                            ON s.celular_disparo = tel.telefone_numero_completo
                        LEFT JOIN UNNEST(tel.consentimento) AS c
                        WHERE
                            (c.indicador_quarentena = FALSE AND tel.telefone_qualidade != 'INVALIDO')
                            OR tel.telefone_numero_completo IS NULL
                    )

                    SELECT
                        celular_disparo AS telefone,
                        cpf AS SubscriberKey,
                        cpf AS externalId,
                        nome_sobrenome,
                        unidade_cras,
                        data,
                        hora,
                        endereco
                    FROM filtra_celulares_sem_whats
                    WHERE celular_disparo IS NOT NULL"""
    }
    run_deployment(
        name="rj-crm--disparo-template-sf/rj-crm--disparo-template-sf--prod",
        parameters=cadunico_params,
        timeout=0,
    )
