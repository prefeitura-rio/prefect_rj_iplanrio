# -*- coding: utf-8 -*-
# ruff: noqa: PLR0913
"""
Dump CADUNICO TO BQ
"""

from iplanrio.pipelines_utils.env import inject_bd_credentials
from prefect import flow

from pipelines.rj_smas__cadunico.tasks import (
    download_repository_task,
    execute_dbt_task,
    get_existing_partitions_task,
    get_files_to_ingest_task,
    ingest_files_task,
    need_to_ingest_task,
    push_models_to_branch_task,
    update_layout_from_storage_and_create_versions_dbt_models_task,
)


@flow(log_prints=True)
def rj_smas__cadunico(
    raw_bucket: str = "rj-smas",
    raw_prefix_area: str = "raw/protecao_social_cadunico/registro_familia",
    staging_bucket: str = "rj-iplanrio",
    table_id: str = "registro_familia",
    layout_table_id: str = "layout",
    dataset_id: str = "brutos_cadunico",
    max_concurrent: int = 3,
    force_create_models: bool = False,
    git_repository_path="https://github.com/prefeitura-rio/queries-rj-iplanrio",
    branch="cadunico",
    dbt_target: str = "prod",
    dbt_select: str = "--select  raw.smas.protecao_social_cadunico",
):
    """
    Pipeline otimizada do CadÚnico:
    1. Verifica partições existentes em staging
    2. Compara com arquivos em raw
    3. Processa, cria tabela e faz upload de cada arquivo individualmente
    """
    staging_prefix_area = f"staging/{dataset_id}/{table_id}"

    injected_credential = inject_bd_credentials()

    # Verificar o que já existe em staging
    existing_partitions = get_existing_partitions_task(
        prefix=staging_prefix_area,
        bucket_name=staging_bucket,
        dataset_id=dataset_id,
        table_id=table_id,
        wait_for=[injected_credential],
    )

    # Identificar arquivos novos para ingerir
    files_to_ingest = get_files_to_ingest_task(
        prefix=raw_prefix_area, partitions=existing_partitions, bucket_name=raw_bucket
    )
    need_to_ingest = need_to_ingest_task(files_to_ingest=files_to_ingest)
    # Verificar se há arquivos para ingerir
    if need_to_ingest:
        # Processar arquivos de forma assíncrona com upload imediato
        injected_files = ingest_files_task(
            files_to_ingest=files_to_ingest,
            bucket_name=raw_bucket,
            dataset_id=dataset_id,
            table_id=table_id,
            max_concurrent=max_concurrent,
            wait_for=[need_to_ingest],
        )
    injected_files = False
    if need_to_ingest or force_create_models:
        repository_path = download_repository_task(
            git_repository_path=git_repository_path,
            branch=branch,
            wait=injected_files,
        )

        models_updated = update_layout_from_storage_and_create_versions_dbt_models_task(
            dataset_id=dataset_id,
            layout_table_id=layout_table_id,
            registo_familia_table_id=table_id,
            raw_bucket=raw_bucket,
            raw_prefix_area=raw_prefix_area,
            staging_bucket=staging_bucket,
            force_create_models=force_create_models,
            repository_path=repository_path,
        )

        push_sucess = push_models_to_branch_task(
            repository_path=repository_path,
            commit_message=f"feat: update CadUnico models - {dataset_id}",
            author_name="CadUnico Pipeline",
            author_email="pipeline@prefeitura.rio",
            wait_for=[models_updated],
        )
        execute_dbt_task(target=dbt_target, select=dbt_select, wait_for=[push_sucess])
