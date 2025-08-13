# -*- coding: utf-8 -*-
"""
Flow migrado do Prefect 1.4 para 3.0 - SMAS Disparo CADUNICO

⚠️ ATENÇÃO: Algumas funções da biblioteca prefeitura_rio NÃO têm equivalente no iplanrio:
- handler_initialize_sentry: SEM EQUIVALENTE
- task_run_dbt_model_task: SEM EQUIVALENTE
- LocalDaskExecutor: Removido no Prefect 3.0
- KubernetesRun: Removido no Prefect 3.0 (configurado no YAML)
- GCS storage: Removido no Prefect 3.0 (configurado no YAML)
- Parameter: Substituído por parâmetros de função
- case (conditional execution): Substituído por if/else padrão
"""


from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task, getenv_or_action
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_smas__disparo_cadunico.constants import CadunicoConstants

from pipelines.rj_smas__disparo_cadunico.tasks import (
    check_api_status,
    create_dispatch_dfr,
    create_dispatch_payload,
    dispatch,
    get_destinations,
    printar,
    remove_duplicate_phones,
)
from pipelines.rj_smas__disparo_cadunico.utils.tasks import (
    access_api,
    create_date_partitions,
    skip_flow_if_empty,
)


@flow(log_prints=True)
def rj_smas__disparo_cadunico(
    # Parâmetros opcionais para override manual na UI.
    id_hsm: int | None = None,
    campaign_name: str | None = None,
    cost_center_id: int | None = None,
    chunk_size: int | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    infisical_secret_path: str = "/wetalkie",
):

    dataset_id = dataset_id or CadunicoConstants.CADUNICO_DATASET_ID.value
    table_id = table_id or CadunicoConstants.CADUNICO_TABLE_ID.value
    dump_mode = dump_mode or CadunicoConstants.CADUNICO_DUMP_MODE.value
    id_hsm = id_hsm or CadunicoConstants.CADUNICO_ID_HSM.value
    campaign_name = campaign_name or CadunicoConstants.CADUNICO_CAMPAIGN_NAME.value
    cost_center_id = cost_center_id or CadunicoConstants.CADUNICO_COST_CENTER_ID.value
    chunk_size = chunk_size or CadunicoConstants.CADUNICO_CHUNK_SIZE.value

    # Valores fixos das constants (não alteráveis na UI)
    query_processor_name = CadunicoConstants.CADUNICO_QUERY_PROCESSOR_NAME.value
    billing_project_id = CadunicoConstants.CADUNICO_BILLING_PROJECT_ID.value
    query = CadunicoConstants.CADUNICO_QUERY.value

    destinations = getenv_or_action("CADUNICO__DESTINATIONS", action="ignore")

    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")
    crd = inject_bd_credentials_task(environment="prod")  # noqa

    api = access_api(
        infisical_secret_path,
        "wetalkie_url",
        "wetalkie_user",
        "wetalkie_pass",
        login_route="users/login",
    )

    api_status = check_api_status(api)

    destinations_result = get_destinations(
        destinations=destinations,
        query=query,
        billing_project_id=billing_project_id,
        query_processor_name=query_processor_name,
    )

    validated_destinations = skip_flow_if_empty(
        data=destinations_result,
        message="No destinations found from query. Skipping flow execution.",
    )

    unique_destinations = remove_duplicate_phones(validated_destinations)

    if api_status:
        dispatch_payload = create_dispatch_payload(
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            destinations=unique_destinations,
        )

        printar(id_hsm)

        dispatch_date = dispatch(
            api=api,
            id_hsm=id_hsm,
            dispatch_payload=dispatch_payload,
            chunk=chunk_size,
        )

        dfr = create_dispatch_dfr(
            id_hsm=id_hsm,
            dispatch_payload=dispatch_payload,
            dispatch_date=dispatch_date,
        )

        partitions_path = create_date_partitions(
            dataframe=dfr,
            partition_column="dispatch_date",
            file_format="parquet",
            root_folder="./data_dispatch/",
        )

        create_table = create_table_and_upload_to_gcs_task(
            data_path=partitions_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            biglake_table=False,
        )
