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
    # Carregar configurações via environment variables (padrão Infisical)
    dataset_id = getenv_or_action("CADUNICO__DATASET_ID")
    table_id = getenv_or_action("CADUNICO__TABLE_ID")
    dump_mode = getenv_or_action("CADUNICO__DUMP_MODE")
    # Parâmetros para disparo CADUNICO
    query = getenv_or_action("CADUNICO__QUERY")
    destinations = getenv_or_action("CADUNICO__DESTINATIONS", action="ignore")
    id_hsm = int(getenv_or_action("CADUNICO__ID_HSM"))
    campaign_name = getenv_or_action("CADUNICO__CAMPAIGN_NAME")
    cost_center_id = int(getenv_or_action("CADUNICO__COST_CENTER_ID"))
    chunk_size = int(getenv_or_action("CADUNICO__CHUNK_SIZE"))
    query_processor_name = getenv_or_action("CADUNICO__QUERY_PROCESSOR_NAME")
    billing_project_id = getenv_or_action("CADUNICO__BILLING_PROJECT_ID")
    # Parâmetros de segredos
    infisical_secret_path = getenv_or_action("CADUNICO__INFISICAL_SECRET_PATH")
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

    # Flow para disparo CADUNICO (baseado no template)
    api_status = check_api_status(api)

    # Get destinations usando função do template
    destinations_result = get_destinations(
        destinations=destinations,
        query=query,
        billing_project_id=billing_project_id,
        query_processor_name=query_processor_name,
    )

    # Remove duplicatas (funcionalidade do template)
    unique_destinations = remove_duplicate_phones(destinations_result)

    if api_status:
        dispatch_payload = create_dispatch_payload(
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            destinations=unique_destinations,
        )

        printar(id_hsm)  # Debug do template

        # Disparo com chunks (função do template)
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

        # Upload para BigQuery
        create_table = create_table_and_upload_to_gcs_task(
            data_path=partitions_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            biglake_table=False,
        )
