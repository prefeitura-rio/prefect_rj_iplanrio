# -*- coding: utf-8 -*-

from iplanrio.pipelines_utils.env import inject_bd_credentials
from prefect import flow, unmapped

from pipelines.rj_smas__cadunico.tasks import (
    append_data_to_storage,
    create_table_if_not_exists,
    get_existing_partitions,
    get_files_to_ingest,
    ingest_file,
    need_to_ingest,
)


@flow(log_prints=True)
def rj_smas__cadunico(
    raw_bucket="rj-smas",
    raw_prefix_area="raw/protecao_social_cadunico/registro_familia",
    staging_bucket="rj-iplanrio",
    table_id="registro_familia",
    dataset_id="brutos_cadunico",
):
    """
    Pipeline simplificada do CadÚnico:
    1. Verifica partições existentes em staging
    2. Compara com arquivos em raw
    3. Ingere apenas arquivos novos
    """
    ingested_files_output = "/tmp/ingested_files/"

    _ = inject_bd_credentials()

    # Verificar o que já existe em staging
    existing_partitions = get_existing_partitions(prefix=f"staging/{dataset_id}/{table_id}", bucket_name=staging_bucket)

    # Identificar arquivos novos para ingerir
    files_to_ingest = get_files_to_ingest(
        prefix=raw_prefix_area, partitions=existing_partitions, bucket_name=raw_bucket
    )

    # Verificar se há arquivos para ingerir
    if need_to_ingest(files_to_ingest=files_to_ingest):
        # Processar arquivos em paralelo
        ingested_files = ingest_file.map(
            blob_name=files_to_ingest,
            bucket_name=unmapped(raw_bucket),
            output_directory=unmapped(ingested_files_output),
        )

        # Criar tabela se não existir
        create_table = create_table_if_not_exists(
            dataset_id=dataset_id,
            table_id=table_id,
            wait_for=[ingested_files],
        )

        # Upload dos dados para storage
        append_data_to_storage(
            data_path=ingested_files_output,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait_for=[create_table],
        )
