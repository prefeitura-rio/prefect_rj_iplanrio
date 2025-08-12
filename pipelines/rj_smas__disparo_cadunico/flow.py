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
)


@flow(log_prints=True)
def rj_smas__disparo_cadunico():
    # Carregar configurações das constants
    dataset_id = CadunicoConstants.CADUNICO_DATASET_ID.value
    table_id = CadunicoConstants.CADUNICO_TABLE_ID.value
    dump_mode = CadunicoConstants.CADUNICO_DUMP_MODE.value
    id_hsm = CadunicoConstants.CADUNICO_ID_HSM.value
    campaign_name = CadunicoConstants.CADUNICO_CAMPAIGN_NAME.value
    cost_center_id = CadunicoConstants.CADUNICO_COST_CENTER_ID.value
    chunk_size = CadunicoConstants.CADUNICO_CHUNK_SIZE.value
    query_processor_name = CadunicoConstants.CADUNICO_QUERY_PROCESSOR_NAME.value
    billing_project_id = CadunicoConstants.CADUNICO_BILLING_PROJECT_ID.value
    infisical_secret_path = CadunicoConstants.CADUNICO_INFISICAL_SECRET_PATH.value
    
    # Query das constants
    query = CadunicoConstants.CADUNICO_QUERY.value
    destinations = getenv_or_action("CADUNICO__DESTINATIONS", action="ignore")
    # Tarefas padrão do Prefect 3.0
    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")
    crd = inject_bd_credentials_task(environment="prod")  # noqa

    # Acesso à API usando segredos do Infisical via variáveis de ambiente
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

    unique_destinations = remove_duplicate_phones(destinations_result)

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
            file_format="csv",
            root_folder="./data_dispatch/",
        )

        create_table = create_table_and_upload_to_gcs_task(
            data_path=partitions_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            biglake_table=False,
        )
