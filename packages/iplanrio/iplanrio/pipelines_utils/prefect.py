# -*- coding: utf-8 -*-
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Union

import yaml
from prefect import task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import DeploymentFilter, FlowFilter, FlowRunFilter
from prefect.client.schemas.sorting import FlowRunSort
from prefect.context import get_run_context
from prefect.schedules import Interval

from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.io import query_to_line
from iplanrio.pipelines_utils.logging import log


@task
def rename_current_flow_run_task(new_name: str):
    """
    Atualiza o nome da execução do fluxo atual.
    """

    # Pega o contexto da execução atual para obter o ID
    context = get_run_context()
    flow_run_id = context.task_run.flow_run_id
    log(f"Obtido o ID da execução do fluxo: {flow_run_id}")

    # Usa o cliente assíncrono do Prefect para interagir com a API
    # 1. Define uma função async interna para fazer o trabalho com o cliente
    async def _update_run_name():
        async with get_client() as client:
            await client.update_flow_run(flow_run_id=flow_run_id, name=new_name)

    asyncio.run(_update_run_name())

    log(f"Nome da execução do fluxo atualizado para {new_name}!")


def generate_dump_db_schedules(
    interval: timedelta,
    start_date: datetime,
    db_database: str,
    db_host: str,
    db_port: Union[str, int],
    db_type: str,
    dataset_id: str,
    infisical_secret_path: str,
    table_parameters: dict,
    biglake_table: bool = True,
    db_charset: str = NOT_SET,
    batch_size: int = 50000,
    runs_interval_minutes: int = 15,
    timezone: str = "America/Sao_Paulo",
) -> List[Interval]:
    """
    Generates multiple schedules for database dumping.
    """
    other_parameters = {
        "retry_dump_upload_attempts": 1,
        "batch_data_type": "csv",
        "log_number_of_batches": 100,
        "break_query_frequency": None,
        "break_query_start": None,
        "break_query_end": None,
        "partition_columns": None,
        "partition_date_format": None,
        "partition_columns": None,
        "lower_bound_date": None,
        "break_query_frequency": None,
        "break_query_start": None,
        "break_query_end": None,
    }

    db_port = str(db_port)
    clocks = []
    for count, parameters in enumerate(table_parameters):
        parameter_defaults = {
            "batch_size": batch_size,
            "infisical_secret_path": infisical_secret_path,
            "db_database": db_database,
            "db_host": db_host,
            "db_port": db_port,
            "db_type": db_type,
            "dataset_id": dataset_id,
            "table_id": parameters["table_id"],
            "db_charset": db_charset,
            "biglake_table": biglake_table,
            "dump_mode": parameters["dump_mode"],
            "execute_query": query_to_line(parameters["execute_query"]),
        }

        # Add remaining parameters if value is not None
        for key, value in parameters.items():
            if value is not None and key not in ["interval", "start_date"]:
                parameter_defaults[key] = value

        if "dbt_alias" in parameters:
            parameter_defaults["dbt_alias"] = parameters["dbt_alias"]
        if "dataset_id" in parameters:
            parameter_defaults["dataset_id"] = parameters["dataset_id"]
        new_interval = parameters["interval"] if "interval" in parameters else interval
        new_start_date = (
            parameters["start_date"]
            if "start_date" in parameters
            else start_date + timedelta(minutes=runs_interval_minutes * count)
        )

        for key, value in other_parameters.items():
            if key not in parameters:
                parameter_defaults[key] = value

        clocks.append(
            Interval(
                new_interval,
                anchor_date=new_start_date,
                parameters=parameter_defaults,
                slug=parameters["table_id"],
                timezone=timezone,
            )
        )
    return clocks


async def delete_flow_run_batch(
    number_of_runs: int,
    flow_name: str = None,
    deployment_name: str = None,
    states: list[str] | None = None,
    concurrency_limit: int = 20,
) -> int:
    """
    Busca até 'batch_size' execuções de fluxo que correspondam aos estados
    e as deleta. Retorna o número de execuções realmente deletadas neste batch.

    Possible states:
        "Scheduled",
        "Late",
        "AwaitingRetry",
        "Pending",
        "Running",
        "Retrying",
        "Paused",
        "Cancelling",
        "Cancelled",
        "Completed",
        "Cached",
        "RolledBack",
        "Failed",
        "Crashed"
    """
    API_FETCH_LIMIT = 200

    if not states:
        states = []

    states = [state.capitalize() for state in states]
    batches_int = int(number_of_runs / API_FETCH_LIMIT)
    total_estimated_batches = (
        batches_int if number_of_runs % API_FETCH_LIMIT == 0 else batches_int
    )

    total_deleted_count = 0
    batch_number = 0

    print(f"Iniciando processo para deletar até {number_of_runs} execuções de fluxo.")
    print(f"Filtros: flow_name='{flow_name}', states={states}")
    print(
        f"Estimativa: {total_estimated_batches} lotes de no máximo {API_FETCH_LIMIT} execuções cada."
    )
    total_time = 0
    async with get_client() as client:
        while total_deleted_count < number_of_runs:
            batch_number += (
                1  # Incrementa o contador do lote no início de cada iteração
            )
            runs_to_fetch = min(API_FETCH_LIMIT, number_of_runs - total_deleted_count)
            start_time = time.time()
            try:
                flow_runs_in_batch = await client.read_flow_runs(
                    flow_filter=(
                        FlowFilter(name={"any_": [flow_name]}) if flow_name else None
                    ),
                    deployment_filter=(
                        DeploymentFilter(name={"any_": [deployment_name]})
                        if deployment_name
                        else None
                    ),
                    flow_run_filter=FlowRunFilter(state={"name": {"any_": states}}),
                    sort=FlowRunSort.END_TIME_DESC,
                    limit=runs_to_fetch,
                )
            except Exception as e:
                print(f"Erro ao buscar lote da API: {e}. Interrompendo.")
                break

            if not flow_runs_in_batch:
                print(
                    "Nenhuma execução de fluxo adicional foi encontrada. O processo será finalizado."
                )
                break

            total_in_batch = len(flow_runs_in_batch)
            print(
                f"Lote: {batch_number}/{total_estimated_batches} with {total_in_batch} runs."
            )
            semaphore = asyncio.Semaphore(concurrency_limit)

            async def delete_run_with_semaphore(run):
                async with semaphore:
                    try:
                        await client.delete_flow_run(run.id)
                        return True
                    except Exception as e:
                        return e

            delete_tasks = [
                delete_run_with_semaphore(run) for run in flow_runs_in_batch
            ]
            results = await asyncio.gather(*delete_tasks)
            deleted_in_this_batch = sum(1 for r in results if r is True)
            # failures_in_this_batch = total_in_batch - deleted_in_this_batch
            total_deleted_count += deleted_in_this_batch
            batch_time = time.time() - start_time
            total_time += batch_time
            estimated_time_to_finish = str(
                timedelta(
                    seconds=(total_time / batch_number)
                    * (total_estimated_batches - batch_number)
                )
            )[:7]
            print(
                f"  Deleted: {total_deleted_count}/{number_of_runs} - {round(100 * total_deleted_count/number_of_runs, 2)}% | {round(batch_time, 2)}s / {estimated_time_to_finish}"
            )
            if total_in_batch < runs_to_fetch:
                print(
                    "Último lote de execuções disponível foi processado. O processo será finalizado."
                )
                break

    print(f"\nOperação finalizada após processar {batch_number} lote(s).")
    print(f"Total de {total_deleted_count} execuções deletadas.")
    return total_deleted_count


def create_schedules(
    schedules_parameters: list,
    base_interval_seconds: int,
    base_anchor_date_str: str,
    runs_interval_minutes: int,
    timezone: str,
    slug_field: str = None,
):
    """
    Generates a full Prefect deployment YAML.

        Args:
            schedules_parameters (list): A list of dictionaries, each defining a table to dump.
            deployment_name (str): The name for the Prefect deployment.
            entrypoint (str): The entrypoint for the flow (e.g., 'path/to/flow.py:flow_name').
            base_interval_seconds (int): The base interval for schedules.
            base_anchor_date_str (str): The anchor date for the first schedule.
            runs_interval_minutes (int): The number of minutes to wait between starting each schedule.
            timezone (str): The IANA timezone for all schedules.
            work_pool_name (str): The name of the work pool.
            work_queue_name (str): The name of the work queue.
            job_image (str): The Docker image for the job.
            job_command (str): The command to execute the flow run.

        Returns:
            dict: A dictionary representing the complete Prefect deployment YAML.
    """
    base_anchor_date = datetime.fromisoformat(base_anchor_date_str)
    schedules = []

    for i, table_params in enumerate(schedules_parameters):
        # Calculate the staggered anchor date for this schedule
        anchor_date = base_anchor_date + timedelta(minutes=runs_interval_minutes * i)
        flow_run_parameters = {}
        for key, value in table_params.items():
            flow_run_parameters[key] = value
            # Create the final schedule object for the YAML

        schedule_config = {
            "interval": base_interval_seconds,
            "anchor_date": anchor_date.isoformat(),
            "timezone": timezone,
        }

        if slug_field:
            schedule_config["slug"] = flow_run_parameters[slug_field]
        schedule_config["parameters"] = flow_run_parameters

        schedules.append(schedule_config)
    # Assemble the final deployment structure
    return yaml.dump(
        {
            "schedules": schedules,
        },
        sort_keys=False,
        indent=2,
        width=120,
    )


def create_dump_db_schedules(
    table_parameters_list: list,
    base_interval_seconds: int,
    base_anchor_date_str: str,
    runs_interval_minutes: int,
    timezone: str,
):
    """
    Generates a full Prefect deployment YAML for database dump tasks.

    Args:
        deployment_name (str): The name for the Prefect deployment.
        entrypoint (str): The entrypoint for the flow (e.g., 'path/to/flow.py:flow_name').
        table_parameters_list (list): A list of dictionaries, each defining a table to dump.
        base_interval_seconds (int): The base interval for schedules.
        base_anchor_date_str (str): The anchor date for the first schedule.
        runs_interval_minutes (int): The number of minutes to wait between starting each schedule.
        timezone (str): The IANA timezone for all schedules.
    Returns:
        dict: A dictionary representing the complete Prefect deployment YAML.
    """
    base_anchor_date = datetime.fromisoformat(base_anchor_date_str)
    schedules = []

    for i, table_params in enumerate(table_parameters_list):
        # Calculate the staggered anchor date for this schedule
        anchor_date = base_anchor_date + timedelta(minutes=runs_interval_minutes * i)

        # Start with a base set of parameters for the flow run
        flow_run_parameters = {}

        # Merge the specific parameters for this table
        # This includes table_id, execute_query, dump_mode, etc.
        for key, value in table_params.items():
            if key == "execute_query":
                flow_run_parameters[key] = query_to_line(value).strip()
            else:
                flow_run_parameters[key] = value

        # Ensure required parameters from the list are set
        if "table_id" not in flow_run_parameters:
            raise ValueError(f"Missing 'table_id' in table parameters at index {i}")

        # Create the final schedule object for the YAML
        schedule_config = {
            "interval": base_interval_seconds,
            "anchor_date": anchor_date.isoformat(),
            "timezone": timezone,
            "slug": flow_run_parameters["table_id"],
            "parameters": flow_run_parameters,
        }
        schedules.append(schedule_config)

    # Assemble the final deployment structure
    return yaml.dump(
        {
            "schedules": schedules,
        },
        sort_keys=False,
        indent=2,
        width=120,
    )
