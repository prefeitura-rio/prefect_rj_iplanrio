# -*- coding: utf-8 -*-
# ruff: noqa: PLR0913
from iplanrio.pipelines_utils.env import inject_bd_credentials
from prefect import flow

from pipelines.rj_smas__cadunico.tasks import (
    get_existing_partitions,
    get_files_to_ingest,
    ingest_files,
    need_to_ingest,
)


@flow(log_prints=True)
def rj_smas__cadunico(
    raw_bucket: str = "rj-smas",
    raw_prefix_area: str = "raw/protecao_social_cadunico/registro_familia",
    staging_bucket: str = "rj-iplanrio",
    table_id: str = "registro_familia",
    dataset_id: str = "brutos_cadunico",
    max_concurrent: int = 3,
):
    """
    Pipeline otimizada do CadÚnico:
    1. Verifica partições existentes em staging
    2. Compara com arquivos em raw
    3. Processa, cria tabela e faz upload de cada arquivo individualmente
    """
    staging_prefix_area = f"staging/{dataset_id}/{table_id}"

    _ = inject_bd_credentials()

    # Verificar o que já existe em staging
    existing_partitions = get_existing_partitions(
        prefix=staging_prefix_area,
        bucket_name=staging_bucket,
        dataset_id=dataset_id,
        table_id=table_id,
    )

    # Identificar arquivos novos para ingerir
    files_to_ingest = get_files_to_ingest(
        prefix=raw_prefix_area, partitions=existing_partitions, bucket_name=raw_bucket
    )

    # Verificar se há arquivos para ingerir
    if need_to_ingest(files_to_ingest=files_to_ingest):
        # Processar arquivos de forma assíncrona com upload imediato
        ingest_files(
            files_to_ingest=files_to_ingest,
            bucket_name=raw_bucket,
            dataset_id=dataset_id,
            table_id=table_id,
            max_concurrent=max_concurrent,
        )
