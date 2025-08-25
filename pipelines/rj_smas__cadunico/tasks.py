# -*- coding: utf-8 -*-
# ruff: noqa

from typing import List, Optional

from prefect import task

from pipelines.rj_smas__cadunico.utils_dbt import (
    download_repository,
    execute_dbt,
    get_github_token,
    push_models_to_branch,
)
from pipelines.rj_smas__cadunico.utils_dump import (
    get_existing_partitions,
    get_files_to_ingest,
    ingest_files,
    need_to_ingest,
)
from pipelines.rj_smas__cadunico.utils_layout import (
    update_layout_from_storage_and_create_versions_dbt_models,
)


@task
def get_existing_partitions_task(prefix: str, bucket_name: str, dataset_id: str, table_id: str) -> List[str]:
    return get_existing_partitions(prefix=prefix, bucket_name=bucket_name, dataset_id=dataset_id, table_id=table_id)


@task()
def get_files_to_ingest_task(prefix: str, partitions: List[str], bucket_name: str) -> List[str]:
    return get_files_to_ingest(prefix=prefix, partitions=partitions, bucket_name=bucket_name)


@task
def need_to_ingest_task(files_to_ingest: list) -> bool:
    return need_to_ingest(files_to_ingest=files_to_ingest)


@task
def ingest_files_task(
    files_to_ingest: List[str],
    bucket_name: str,
    dataset_id: str,
    table_id: str,
    max_concurrent: int = 3,
) -> None:
    return ingest_files(
        files_to_ingest=files_to_ingest,
        bucket_name=bucket_name,
        dataset_id=dataset_id,
        table_id=table_id,
        max_concurrent=max_concurrent,
    )


@task
def download_repository_task(git_repository_path: str, branch: str = "master", wait=None) -> str:
    wait = wait
    return download_repository(git_repository_path=git_repository_path, branch=branch)


@task
def update_layout_from_storage_and_create_versions_dbt_models_task(
    dataset_id: str = "brutos_cadunico",
    layout_table_id: str = "layout",
    registo_familia_table_id: str = "registro_familia",
    raw_bucket: str = "rj-smas",
    raw_prefix_area: str = "raw/protecao_social_cadunico/layout",
    staging_bucket: str = "rj-iplanrio",
    force_create_models: bool = True,
    repository_path: str = "/tmp/dbt_repository/",
):
    return update_layout_from_storage_and_create_versions_dbt_models(
        dataset_id=dataset_id,
        layout_table_id=layout_table_id,
        registo_familia_table_id=registo_familia_table_id,
        raw_bucket=raw_bucket,
        raw_prefix_area=raw_prefix_area,
        staging_bucket=staging_bucket,
        force_create_models=force_create_models,
        repository_path=repository_path,
    )


@task
def push_models_to_branch_task(
    repository_path: str,
    github_token: Optional[str],
    commit_message: str = "feat: update CadUnico models",
    author_name: str = "pipeline_cadunico",
    author_email: str = "pipeline@prefeitura.rio",
) -> bool:
    if not github_token:
        github_token = get_github_token()

    return push_models_to_branch(
        repository_path=repository_path,
        github_token=github_token,
        commit_message=commit_message,
        author_name=author_name,
        author_email=author_email,
    )


@task
def execute_dbt_task(
    target: str = "prod",
    select: str = "--select  raw.smas.protecao_social_cadunico",
):
    execute_dbt(target=target, select=select)
