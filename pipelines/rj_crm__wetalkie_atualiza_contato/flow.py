"""
Flow migrado do Prefect 1.4 para 3.0 - CRM Wetalkie Atualiza Contato
"""

from pathlib import Path

from prefect import flow
from toolkit.dbt import DbtRun

from pipelines.rj_crm__wetalkie_atualiza_contato.constants import (
    WetalkieAtualizaContatoConstants,
)
from pipelines.rj_crm__wetalkie_atualiza_contato.helpers.tasks import (
    access_api,
    skip_flow_if_empty,
)
from pipelines.rj_crm__wetalkie_atualiza_contato.tasks import (
    download_missing_contacts,
    get_contacts,
    safe_export_df_to_parquet,
)
from utils.tasks import publish_table_in_bq, run_dbt, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table_id}_{dataset_id}")
async def rj_crm__wetalkie_atualiza_contato(
    *,
    cloud: Cloud,
    # Parâmetros opcionais para override manual na UI
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    infisical_secret_path: str = "/wetalkie",
):
    """
    Flow para atualizar dados de contatos faltantes via API Wetalkie.

    Este flow busca contatos no BigQuery que não possuem telefone/WhatsApp,
    consulta a API Wetalkie para obter esses dados e atualiza as informações.

    Args:
        dataset_id: ID do dataset no BigQuery (default: brutos_wetalkie)
        table_id: ID da tabela no BigQuery (default: contato)
        dump_mode: Modo de dump (default: append)
        materialize_after_dump: Se deve materializar após dump (default: False)
        infisical_secret_path: Caminho dos secrets no Infisical (default: /wetalkie)
    """

    # Usar valores dos constants como padrão para parâmetros
    dataset_id = dataset_id or WetalkieAtualizaContatoConstants.DATASET_ID.value
    table_id = table_id or WetalkieAtualizaContatoConstants.TABLE_ID.value
    dump_mode = dump_mode or WetalkieAtualizaContatoConstants.DUMP_MODE.value
    materialize_after_dump = (
        materialize_after_dump
        if materialize_after_dump is not None
        else WetalkieAtualizaContatoConstants.MATERIALIZE_AFTER_DUMP.value
    )

    file_format = WetalkieAtualizaContatoConstants.FILE_FORMAT.value
    root_folder = WetalkieAtualizaContatoConstants.ROOT_FOLDER.value
    query = WetalkieAtualizaContatoConstants.CONTACTS_QUERY.value
    billing_project_id = WetalkieAtualizaContatoConstants.BILLING_PROJECT_ID.value
    bucket_name = WetalkieAtualizaContatoConstants.BUCKET_NAME.value

    # Renomear flow run para melhor identificação

    # Injetar credenciais do BD

    # Buscar contatos com dados faltantes do BigQuery
    df_contacts = download_missing_contacts(query=query, billing_project_id=billing_project_id, bucket_name=bucket_name)

    # Verificar se há contatos para processar
    validated_contacts = skip_flow_if_empty(
        data=df_contacts,
        message="No contacts found with missing phone data. Skipping flow execution.",
    )
    if validated_contacts is None:
        return  # flow termina aqui, nada downstream é agendado

    # Acessar API Wetalkie
    api = access_api(
        infisical_secret_path,
        "wetalkie_url",
        "wetalkie_user",
        "wetalkie_pass",
        login_route=WetalkieAtualizaContatoConstants.API_LOGIN_ROUTE.value,
    )

    # Buscar dados dos contatos na API Wetalkie
    updated_contacts = get_contacts(api, validated_contacts)
    # Verificar se algum contato foi atualizado
    final_contacts = skip_flow_if_empty(
        data=updated_contacts,
        message="No contacts were successfully updated from API. Skipping upload.",
    )

    if final_contacts is None:
        return  # flow termina aqui, nada downstream é agendado

    # Exportar dados para arquivo
    exported_path = safe_export_df_to_parquet(
        dfr=final_contacts,
        output_path=root_folder,
    )

    # Upload para GCS e BigQuery
    table = Table(dataset_id=dataset_id, table_id=table_id)
    await upload_files_to_gcs(
        Path(exported_path),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="parquet",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)

    # Materializar com DBT se necessário
    if materialize_after_dump:
        await run_dbt(DbtRun(repository="https://github.com/prefeitura-rio/queries-rj-iplanrio", command="run", target="prod", select=table_id))
