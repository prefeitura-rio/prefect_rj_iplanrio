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
    prefix_raw_area="raw/protecao_social_cadunico/registro_familia",
    ingested_files_output="/tmp/ingested_files/",
    dataset_id="brutos_cadunico",
    table_id="registro_familia",
):
    """
    Pipeline simplificada do CadÚnico:
    1. Verifica partições existentes em staging
    2. Compara com arquivos em raw
    3. Ingere apenas arquivos novos
    """

    _ = inject_bd_credentials()

    # Verificar o que já existe em staging
    existing_partitions = get_existing_partitions(prefix=f"staging/{dataset_id}/{table_id}", bucket_name="rj-iplanrio")

    # Identificar arquivos novos para ingerir
    files_to_ingest = get_files_to_ingest(prefix=prefix_raw_area, partitions=existing_partitions, bucket_name="rj-smas")

    # Verificar se há arquivos para ingerir
    if need_to_ingest(files_to_ingest=files_to_ingest):
        # Processar arquivos em paralelo
        ingested_files = ingest_file.map(
            blob_name=files_to_ingest,
            bucket_name=unmapped("rj-smas"),
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
