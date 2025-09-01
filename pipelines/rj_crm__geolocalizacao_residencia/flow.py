# -*- coding: utf-8 -*-
"""
Flow migrado do Prefect 1.4 para 3.0 - CRM Geolocalização Residência

⚠️ ATENÇÃO: Algumas funções da biblioteca prefeitura_rio NÃO têm equivalente no iplanrio:
- handler_initialize_sentry: SEM EQUIVALENTE
- LocalDaskExecutor: Removido no Prefect 3.0
- KubernetesRun: Removido no Prefect 3.0 (configurado no YAML)
- GCS storage: Removido no Prefect 3.0 (configurado no YAML)
- Parameter: Substituído por parâmetros de função
- case (conditional execution): Substituído por if/else padrão
"""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__geolocalizacao_residencia.constants import GeolocalizacaoConstants
from pipelines.rj_crm__geolocalizacao_residencia.tasks import (
    add_plus_code_column,
    async_geocoding_dataframe,
    async_geocoding_dataframe_with_fallback,
    check_df_emptiness,
    dataframe_to_file,
    download_data_from_bigquery,
    geoapify_batch_geocoding_task,
)


@flow(log_prints=True)
def rj_crm__geolocalizacao_residencia(
    # Strategy parameter to choose geocoding approach
    provider_strategy: str = GeolocalizacaoConstants.DEFAULT_STRATEGY.value,
    # Query override
    query_override: str | None = None,
    # BigQuery parameters
    dataset_id: str | None = None,
    table_id: str | None = None,
    billing_project_id: str | None = None,
    bucket_name: str | None = None,
    biglake_table: bool | None = None,
    # File processing parameters
    file_folder: str | None = None,
    file_format: str | None = None,
    source_format: str | None = None,
    dump_mode: str | None = None,
    # Geocoding parameters
    max_concurrent_nominatim: int | None = None,
    return_original_cols: bool | None = None,
    # Secrets path
    infisical_secret_path: str = "/geocoding",
):
    """
    Flow para geolocalizar endereços residenciais usando múltiplas estratégias.

    Este flow suporta três estratégias de geocoding:
    1. 'nominatim' - Usa apenas Nominatim self-hosted (rápido, menor precisão)
    2. 'fallback' - Usa múltiplos provedores sequencialmente (lento, maior precisão)
    3. 'geoapify_batch' - Usa API em lote do Geoapify (médio, boa precisão)

    Args:
        provider_strategy: Estratégia de geocoding a usar ('nominatim', 'fallback', 'geoapify_batch')
        query_override: Query SQL personalizada (opcional)
        dataset_id: ID do dataset no BigQuery (default: intermediario_dados_mestres)
        table_id: ID da tabela no BigQuery (default: enderecos_geolocalizados)
        billing_project_id: Projeto para billing (default: rj-crm-registry)
        bucket_name: Nome do bucket (default: rj-sms)
        biglake_table: Se é tabela BigLake (default: False)
        file_folder: Pasta para arquivos temporários (default: pipelines/data)
        file_format: Formato do arquivo (default: parquet)
        source_format: Formato fonte para BigQuery (default: parquet)
        dump_mode: Modo de dump (default: append)
        max_concurrent_nominatim: Max requests concorrentes Nominatim (default: 100)
        return_original_cols: Retornar colunas originais (default: True)
    """

    # Usar valores dos constants como padrão para parâmetros
    dataset_id = dataset_id or GeolocalizacaoConstants.DATASET_ID.value
    table_id = table_id or GeolocalizacaoConstants.TABLE_ID.value
    billing_project_id = billing_project_id or GeolocalizacaoConstants.BILLING_PROJECT_ID.value
    bucket_name = bucket_name or GeolocalizacaoConstants.BUCKET_NAME.value
    biglake_table = biglake_table if biglake_table is not None else GeolocalizacaoConstants.BIGLAKE_TABLE.value

    file_folder = file_folder or GeolocalizacaoConstants.FILE_FOLDER.value
    file_format = file_format or GeolocalizacaoConstants.FILE_FORMAT.value
    source_format = source_format or GeolocalizacaoConstants.SOURCE_FORMAT.value
    dump_mode = dump_mode or GeolocalizacaoConstants.DUMP_MODE.value

    max_concurrent_nominatim = max_concurrent_nominatim or GeolocalizacaoConstants.MAX_CONCURRENT_NOMINATIM.value
    return_original_cols = (
        return_original_cols if return_original_cols is not None else GeolocalizacaoConstants.RETURN_ORIGINAL_COLS.value
    )

    # Query to use
    query = query_override or GeolocalizacaoConstants.ADDRESS_QUERY.value
    address_column = GeolocalizacaoConstants.ADDRESS_COLUMN.value

    # Renomear flow run para melhor identificação
    rename_flow_run = rename_current_flow_run_task(new_name=f"geo_{provider_strategy}_{table_id}_{dataset_id}")

    # Injetar credenciais do BD
    crd = inject_bd_credentials_task(environment="prod")

    # Download addresses from BigQuery
    dataframe = download_data_from_bigquery(
        query=query,
        billing_project_id=billing_project_id,
        bucket_name=bucket_name,
    )

    # Check if we have addresses to process
    empty_addresses = check_df_emptiness(dataframe=dataframe)

    if not empty_addresses:
        # Choose geocoding strategy
        if provider_strategy == GeolocalizacaoConstants.STRATEGY_NOMINATIM.value:
            # Strategy 1: Nominatim only (fast, basic accuracy)
            georeferenced_table = async_geocoding_dataframe(
                dataframe=dataframe,
                address_column=address_column,
                max_concurrent_nominatim=max_concurrent_nominatim,
                return_original_cols=return_original_cols,
            )

        elif provider_strategy == GeolocalizacaoConstants.STRATEGY_FALLBACK.value:
            # Strategy 2: Multiple provider fallback (slower, higher accuracy)
            georeferenced_table = async_geocoding_dataframe_with_fallback(
                dataframe=dataframe,
                address_column=address_column,
            )

        elif provider_strategy == GeolocalizacaoConstants.STRATEGY_GEOAPIFY_BATCH.value:
            # Strategy 3: Geoapify batch processing (medium speed, good accuracy)
            georeferenced_table = geoapify_batch_geocoding_task(
                dataframe=dataframe,
                address_column=address_column,
                batch_size=GeolocalizacaoConstants.GEOAPIFY_BATCH_SIZE.value,
                return_original_cols=return_original_cols,
            )
        else:
            raise ValueError(
                f"Invalid provider_strategy: {provider_strategy}. Must be one of: nominatim, fallback, geoapify_batch"
            )

        # Check if geocoding produced results
        empty_geocoded = check_df_emptiness(dataframe=georeferenced_table)

        if not empty_geocoded:
            # Add plus codes
            georeferenced_table = add_plus_code_column(georeferenced_table)

            # Save to file
            base_path = dataframe_to_file(
                dataframe=georeferenced_table, file_folder=file_folder, file_format=file_format
            )

            # Upload to GCS and BigQuery
            create_table_and_upload_to_gcs_task(
                data_path=base_path,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode=dump_mode,
                biglake_table=biglake_table,
                source_format=source_format,
            )
        else:
            print(f"No geocoded results from strategy '{provider_strategy}' - skipping upload")
    else:
        print("No addresses found to geocode - skipping flow execution.")
